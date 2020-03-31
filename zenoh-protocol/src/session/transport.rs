use async_std::prelude::*;
use async_std::sync::{
    Arc,
    channel,
    Mutex,
    MutexGuard,
    Receiver,
    RwLock,
    Sender,
};
use async_std::task;
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::Ordering;

use crate::zerror;
use crate::core::{
    AtomicZInt,
    ZError,
    ZErrorKind,
    ZInt,
    ZResult
};
use crate::proto::{
    Body,
    Message,
    MessageKind,
    SeqNum,
    SeqNumGenerator
};
use crate::session::{
    MsgHandler,
    SessionInner
};
use crate::link::{
    Link,
    Locator
};
use zenoh_util::{
    zasynclock,
    zrwopt
};
use zenoh_util::collections::CreditQueue;


// Constants
pub(crate) const PRIO_CTRL: usize = 0;
const SIZE_CTRL: usize = 16;
const CRED_CTRL: isize = 1;

pub(crate) const _PRIO_RETX: usize = 1;
const SIZE_RETX: usize = 256;
const CRED_RETX: isize = 1;

pub(crate) const _PRIO_FRAG: usize = 2;
const SIZE_FRAG: usize = 256;
const CRED_FRAG: isize = 1;

pub(crate) const PRIO_DATA: usize = 3;
const SIZE_DATA: usize = 256;
const CRED_DATA: isize = 100;

const CONCURRENCY: usize = 16;




async fn consume_loop(transport: Arc<Transport>) {
    async fn consume(transport: &Arc<Transport>) -> Option<bool> {
        // @TODO: Fragement the message if too large
        // @TODO: Implement the reliability queue
        let mut message = transport.queue_tx.pull().await;

        let is_reliable = message.inner.is_reliable();
        match message.inner.body {
            Body::Data{ref mut sn, ..} |
            Body::Unit{ref mut sn, ..} |
            Body::Declare{ref mut sn, ..} |
            Body::Pull{ref mut sn, ..} |
            Body::Query{ref mut sn, ..} => {
                // Update the sequence number
                let mut l_sn = zasynclock!(transport.conduit_tx_sn);
                let sn_gen = match l_sn.get_mut(&message.inner.cid) {
                    Some(sn_gen) => sn_gen,
                    // This CID does not exist, return
                    None => return Some(true)
                };
                *sn = if is_reliable {
                    sn_gen.reliable.get()
                } else {
                    sn_gen.unreliable.get()
                };
            },
            _ => {}
        }

        transport.transmit(&message).await;
        
        Some(true)
    }
    
    // The loop to consume the messages in the queue
    loop {
        // Future to wait for the stop signal
        let stop = transport.signal_recv.recv();
        // Future to wait for a message to send
        let consume = consume(&transport);
        // Race the two futures
        match consume.race(stop).await {
            // Message received, it is Ok, continue
            Some(true) => continue,
            // Stop signal received, it is Err, break
            Some(false) => break,
            // Error on the channel, it is Err, break
            None => break
        }
    }
}


/*************************************/
/*          TRANSPORT                */
/*************************************/

// Struct to add additional fields to the message required for transmission
pub struct MessageTx {
    // The inner message to transmit
    pub inner: Message, 
    // The preferred link to transmit the Message on
    pub link: Option<(Locator, Locator)>,
    pub notify: Option<Sender<ZResult<()>>>
}

// Structs to manage the sequence numbers of channels and conduits
struct ConduitTxSeqNum {
    reliable: SeqNumGenerator,
    unreliable: SeqNumGenerator
}

impl ConduitTxSeqNum {
    fn new(sn0_reliable: ZInt, sn0_unreliable: ZInt, resolution: ZInt) -> ConduitTxSeqNum {
        ConduitTxSeqNum {
            reliable: SeqNumGenerator::make(sn0_reliable, resolution).unwrap(),
            unreliable: SeqNumGenerator::make(sn0_unreliable, resolution).unwrap()
        }
    }
}

struct ConduitRxSeqNum {
    reliable: SeqNum,
    unreliable: SeqNum
}

impl ConduitRxSeqNum {
    fn new(sn0_reliable: ZInt, sn0_unreliable: ZInt, resolution: ZInt) -> ConduitRxSeqNum {
        ConduitRxSeqNum {
            reliable: SeqNum::make(sn0_reliable, resolution).unwrap(),
            unreliable: SeqNum::make(sn0_unreliable, resolution).unwrap()
        }
    }
}


// Struct implementing the transport
pub struct Transport {
    // The reference to the session
    session: RwLock<Option<Arc<SessionInner>>>,
    // The callback for Data messages
    callback: RwLock<Option<Arc<dyn MsgHandler + Send + Sync>>>,
    // The timeout after which the session is closed if no messages are received
    lease: AtomicZInt,
    // The resolution to be used for the SN
    resolution: AtomicZInt,
    // The list of transport links associated to this session
    links: Mutex<Vec<Link>>,
    // The queue of messages to be transmitted
    queue_tx: CreditQueue<MessageTx>,
    // The conduits sequence number generators
    conduit_tx_sn: Mutex<HashMap<ZInt, ConduitTxSeqNum>>,
    conduit_rx_sn: Mutex<HashMap<ZInt, ConduitRxSeqNum>>,
    // The channel endpoints for terminating the consume_loop task
    signal_send: Sender<bool>,
    signal_recv: Receiver<bool>
}

impl Transport {
    pub(crate) fn new(lease: ZInt, resolution: ZInt) -> Transport {
        // Build the transmission queue
        let queue_tx = vec![
            (SIZE_CTRL, CRED_CTRL),
            (SIZE_RETX, CRED_RETX),
            (SIZE_FRAG, CRED_FRAG),
            (SIZE_DATA, CRED_DATA)
        ];

        // Build the conduit zero sequence number manager
        let zero: ZInt = 0;
        // @TODO: randomly create the initial sequence numbers
        let mut conduit_tx_sn = HashMap::new();
        conduit_tx_sn.insert(zero, ConduitTxSeqNum::new(zero, zero, resolution));

        let mut conduit_rx_sn = HashMap::new();
        conduit_rx_sn.insert(zero, ConduitRxSeqNum::new(resolution-1, resolution-1, resolution));

        // Build the channel used to terminate the consume task
        let (sender, receiver) = channel::<bool>(1);

        Transport {
            // Session manager and Callback
            session: RwLock::new(None), 
            callback: RwLock::new(None), 
            // Session lease
            lease: AtomicZInt::new(lease),
            // Session sequence number resolution
            resolution: AtomicZInt::new(resolution),
            // The links
            links: Mutex::new(Vec::new()),
            // The transmission queue
            queue_tx: CreditQueue::new(queue_tx, CONCURRENCY),
            // The sequence number generator
            conduit_tx_sn: Mutex::new(conduit_tx_sn),
            conduit_rx_sn: Mutex::new(conduit_rx_sn),
            // The channels used to signal the termination of consume task
            signal_send: sender,
            signal_recv: receiver
        }
    }

    pub(crate) fn initialize(&self, session: Arc<SessionInner>, callback: Arc<dyn MsgHandler + Send + Sync>) {
        *self.session.try_write().unwrap() = Some(session);
        *self.callback.try_write().unwrap() = Some(callback);
    }

    pub(crate) fn set_lease(&self, lease: ZInt) {
        self.lease.store(lease, Ordering::Release);
    }

    pub(crate) async fn set_resolution(&self, resolution: ZInt) {
        let mut c_tx = zasynclock!(self.conduit_tx_sn);
        let mut c_rx = zasynclock!(self.conduit_rx_sn);
        for (_, v) in c_tx.iter_mut() {
            *v = ConduitTxSeqNum::new(
                v.reliable.get() % resolution, 
                v.unreliable.get() % resolution, 
                resolution
            );
        }
        for (_, v) in c_rx.iter_mut() {
            *v = ConduitRxSeqNum::new(
                v.reliable.get() % resolution, 
                v.unreliable.get() % resolution, 
                resolution
            );
        }
        self.resolution.store(resolution, Ordering::Release);
    }

    pub(crate) async fn get_links(&self) -> Vec<Link> {
        let mut vec = Vec::new();
        for l in self.links.lock().await.iter() {
            vec.push(l.clone());
        }
        vec
    }

    pub(crate) fn start(transport: Arc<Transport>) {
        task::spawn(consume_loop(transport));
    }

    pub(crate) async fn stop(&self) {
        self.signal_send.send(false).await;
    }


    /*************************************/
    /*               LINK                */
    /*************************************/
    fn find_link(&self, guard: &MutexGuard<'_, Vec<Link>>, src: &Locator, dst: &Locator) -> Option<usize> {
        let mut found = false;
        let mut index: usize = 0;

        for i in 0..guard.len() {
            if guard[i].get_src() == *src && guard[i].get_dst() == *dst {
                found = true;
                index = i;
                break
            }
        }

        if found {
            Some(index)
        } else {
            None
        }
    }

    pub(crate) async fn add_link(&self, link: Link) -> ZResult<()> {
        let mut guard = self.links.lock().await;
        match self.find_link(&guard, &link.get_src(), &link.get_dst()) {
            Some(_) => Err(zerror!(ZErrorKind::Other{
                descr: "Trying to delete a link that does not exist!".to_string()
            })),
            None => {
                guard.push(link);
                Ok(())
            }
        }
    }

    pub(crate) async fn del_link(&self, src: &Locator, dst: &Locator) -> ZResult<Link> {    
        let mut guard = self.links.lock().await;
        match self.find_link(&guard, src, dst) {
            Some(index) => Ok(guard.remove(index)),
            None => Err(zerror!(ZErrorKind::Other{
                descr: "Trying to delete a link that does not exist!".to_string()
            }))
        }
    }


    /*************************************/
    /*       SCHEDULE AND TRANSMIT       */
    /*************************************/
    // Schedule the message to be sent asynchronsly
    pub(crate) async fn schedule(&self, message: Message, link: Option<(Locator, Locator)>, priority: usize) {
        let message = MessageTx {
            inner: message,
            link,
            notify: None
        };
        // Wait for the queue to have space for the message
        self.queue_tx.push(message, priority).await;
    }

    // Schedule the message to be sent asynchronsly and notify once sent
    pub(crate) async fn send(&self, message: Message, link: Option<(Locator, Locator)>, priority: usize) -> ZResult<()> {
        let (sender, receiver) = channel::<ZResult<()>>(1);
        let message = MessageTx {
            inner: message,
            link,
            notify: Some(sender)
        };
        // Wait for the transmission queue to have space for the message
        self.queue_tx.push(message, priority).await;
        match receiver.recv().await {
            Some(res) => res,
            None => Err(zerror!(ZErrorKind::Other{
                descr: "Send failed unexpectedly!".to_string()
            }))
        }
    }
    
    async fn transmit(&self, message: &MessageTx) {
        // Send the message on the link(s)
        let guard = self.links.lock().await;
        let res = match &message.link {
            // Send the message to the indicated link
            Some((src, dst)) => {
                match self.find_link(&guard, &src, &dst) {
                    Some(index) => guard[index].send(&message.inner).await,
                    None => Err(zerror!(ZErrorKind::Other{
                        descr: format!("Message dropped because link ({} => {}) was not found!", &src, &dst)
                    }))
                }
            },
            None => {
                // Send the message on the first link
                // @TODO: Adopt an intelligent selection of the links to use
                match guard.get(0) {
                    Some(link) => link.send(&message.inner).await,
                    None =>  Err(zerror!(ZErrorKind::Other{
                        descr: "Message dropped because transport has no links!".to_string()
                    }))
                }
            }
        };

        if let Some(notify) = &message.notify {
            notify.send(res).await;
        }
    }

    /*************************************/
    /*   MESSAGE RECEIVED FROM THE LINK  */
    /*************************************/
    async fn process_reliable_message(&self, message: Message, sn: ZInt) {
        // @TODO: implement the reordering and wait for missing messages
        let mut l_guard = zasynclock!(self.conduit_rx_sn);
        // Messages with invalid CID or invalid SN are automatically dropped
        if let Some(rx_sn) = l_guard.get_mut(&message.cid) {
            if rx_sn.reliable.precedes(sn) && rx_sn.reliable.set(sn).is_ok() {
                let _ = zrwopt!(self.callback).handle_message(message).await;
            }
        }
    }

    async fn process_unreliable_message(&self, message: Message, sn: ZInt) {
        let mut l_guard = zasynclock!(self.conduit_rx_sn);
        // Messages with invalid CID or invalid SN are automatically dropped
        if let Some(rx_sn) = l_guard.get_mut(&message.cid) {
            if rx_sn.unreliable.precedes(sn) && rx_sn.unreliable.set(sn).is_ok() {
                let _ = zrwopt!(self.callback).handle_message(message).await;
            }
        }
    }

    async fn receive_full_message(&self, src: &Locator, dst: &Locator, message: Message) -> Option<Arc<Self>> {
        match &message.body {
            Body::Accept{whatami, opid, apid, lease} => {
                let c_lease = *lease;
                match zrwopt!(self.session).process_accept(src, dst, whatami, opid, apid, c_lease).await {
                    Ok(transport) => Some(transport),
                    Err(_) => None
                }
            },
            Body::AckNack{..} => {
                unimplemented!("Handling of AckNack Messages not yet implemented!");
            },
            Body::Close{pid, reason} => {
                let c_reason = *reason;
                zrwopt!(self.session).process_close(src, dst, pid, c_reason).await;
                None
            },
            Body::Hello{..} => {
                unimplemented!("Handling of Hello Messages not yet implemented!");
            },
            Body::KeepAlive{..} => {
                unimplemented!("Handling of KeepAlive Messages not yet implemented!");
            },
            Body::Open{version, whatami, pid, lease, locators} => {
                let c_version = *version;
                let c_lease = *lease;
                match zrwopt!(self.session).process_open(src, dst, c_version, whatami, pid, c_lease, locators).await {
                    Ok(transport) => Some(transport),
                    Err(_) => None
                }
            },
            Body::Ping{..} => {
                unimplemented!("Handling of Ping Messages not yet implemented!");
            },
            Body::Pong{..} => {
                unimplemented!("Handling of Pong Messages not yet implemented!");
            },
            Body::Scout{..} => {
                unimplemented!("Handling of Scout Messages not yet implemented!");
            },
            Body::Sync{..} => {
                unimplemented!("Handling of Sync Messages not yet implemented!");
            }
            Body::Data{reliable, sn, ..} |
            Body::Unit{reliable, sn} => {
                let c_sn = *sn;
                match reliable {
                    true => self.process_reliable_message(message, c_sn).await,
                    false => self.process_unreliable_message(message, c_sn).await,
                }
                None
            },
            Body::Declare{sn, ..} |
            Body::Pull{sn, ..} |
            Body::Query{sn, ..} => {
                let c_sn = *sn;
                self.process_reliable_message(message, c_sn).await;
                None
            }
        }
    }

    async fn receive_first_fragement(&self, _src: &Locator, _dst: &Locator, _message: Message, _number: Option<ZInt>) -> Option<Arc<Self>> {
        unimplemented!("Defragementation not implemented yet!");
    }

    async fn receive_middle_fragement(&self, _src: &Locator, _dst: &Locator, _message: Message) -> Option<Arc<Self>> {
        unimplemented!("Defragementation not implemented yet!");
    }

    async fn receive_last_fragement(&self, _src: &Locator, _dst: &Locator, _message: Message) -> Option<Arc<Self>> {
        unimplemented!("Defragementation not implemented yet!");
    }

    pub async fn receive_message(&self, src: &Locator, dst: &Locator, message: Message) -> Option<Arc<Self>> {
        match message.kind {
            MessageKind::FullMessage =>
                self.receive_full_message(src, dst, message).await,
            MessageKind::FirstFragment{n} =>
                self.receive_first_fragement(src, dst, message, n).await,
            MessageKind::InbetweenFragment => 
                self.receive_middle_fragement(src, dst, message).await,
            MessageKind::LastFragment => 
                self.receive_last_fragement(src, dst, message).await
        }
    }

    /*************************************/
    /*         CLOSE THE SESSION         */
    /*************************************/
    pub async fn close(&self) -> ZResult<()> {
        // Notify the callback
        zrwopt!(self.callback).close().await;

        // Stop the task
        self.stop().await;

        // Remove and close all the links
        for l in self.links.lock().await.drain(..) {
            let _ = l.close().await;
        }

        // Remove the reference to the session
        *self.session.write().await = None;       

        Ok(())
    }
}

impl Drop for Transport {
    fn drop(&mut self) {
        // @TODO: stop and close the transport
    }
}

impl fmt::Debug for Transport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let links = task::block_on(async {
            let mut s = String::new();
            for l in self.links.lock().await.iter() {
                s.push_str(&format!("\n\t[({:?}) => ({:?})]", l.get_src(), l.get_dst()));
            }
            s
        });
        write!(f, "Links:{}", links)
    }
}
