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
use std::fmt;
use std::sync::atomic::{
    AtomicUsize,
    Ordering
};

use crate::{
    zerror,
    zrwopt
};
use crate::core::{
    ZError,
    ZErrorKind,
    ZInt,
    ZResult
};
use crate::proto::{
    Body,
    Message,
    MessageKind
};
use crate::session::{
    MsgHandler,
    Session
};
use crate::session::queue::{
    OrderedQueue,
    PriorityQueue
};
use crate::link::{
    Link,
    Locator
};


// Constants
const QUEUE_RX_SIZE: usize = 64;

const QUEUE_TX_SIZE: usize = 16;
const QUEUE_TX_PRIO_NUM: usize = 3;
const QUEUE_TX_PRIO_CTRL: usize = 0;
const QUEUE_TX_PRIO_RETX: usize = 1;
const QUEUE_TX_PRIO_DATA: usize = 2;

const QUEUE_RETX_SIZE: usize = 16;


async fn consume_loop(transport: Arc<Transport>) {
    async fn consume(transport: &Arc<Transport>) -> Option<bool> {
        let (message, queue) = transport.queue_tx.pop().await;

        let mut new_sn: ZInt = 0;
        // Update the sequence number for data messages
        if queue == QUEUE_TX_PRIO_DATA {
            new_sn = match message.inner.is_reliable() {
                true => {
                    let mut guard = transport.sn_tx_reliable.lock().await;
                    *guard = *guard + 1;
                    *guard
                },
                false => {
                    let mut guard = transport.sn_tx_unreliable.lock().await;
                    *guard = *guard + 1;
                    *guard
                }
            };

            #[allow(unused_assignments,unused_variables)]
            match message.inner.body {
                Body::Data{reliable: _, mut sn, key: _, info: _, payload: _} |
                Body::Declare{mut sn, declarations: _} |
                Body::Pull{mut sn, key: _, pull_id: _, max_samples: _} |
                Body::Query{mut sn, key: _, predicate: _, qid: _, target: _, consolidation: _} => {
                    sn = new_sn;
                },
                _ => {}
            }
        }

        // Send the message on the link(s)
        let guard = transport.links.lock().await;
        let res = match &message.link {
            // Send the message to the indicated link
            Some((src, dst)) => {
                match transport.find_link(&guard, &src, &dst) {
                    Some(index) => guard[index].send(&message.inner).await,
                    None => Err(zerror!(ZErrorKind::Other{
                        descr: format!("Message dropped because link ({} => {}) was not found!", &src, &dst)
                    }))
                }
            },
            None => {
                // Send the message on the first link
                match guard.get(0) {
                    Some(link) => link.send(&message.inner).await,
                    None =>  Err(zerror!(ZErrorKind::Other{
                        descr: format!("Message dropped because transport has no links!")
                    }))
                }
            }
        };

        // Notify the sender in case of syncrhonous send and message unreliable
        // Reliable synchronous send is notified after an ack is received 
        if let Some(sender) = &message.notify {
            if !message.inner.is_reliable() {
                sender.send(res).await;
            }
        }

        // Add the message to the retransmission queue if the message is data or already retransmitted and reliable
        if (queue == QUEUE_TX_PRIO_DATA || queue == QUEUE_TX_PRIO_RETX) && message.inner.is_reliable() {
            transport.queue_retx.lock().await.try_push(message, new_sn);
        }

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
struct TxMessage {
    inner: Message, 
    link: Option<(Locator, Locator)>,
    notify: Option<Sender<ZResult<()>>>
}

pub struct Transport {
    // The reference to the session
    session: RwLock<Option<Arc<Session>>>,
    // The callback for Data messages
    callback: RwLock<Option<Arc<dyn MsgHandler + Send + Sync>>>,
    // The timeout after which the session is closed if no messages are received
    lease: RwLock<ZInt>,
    // The list of transport links associated to this session
    links: Mutex<Vec<Link>>,
    // The queue of messages to be transmitted
    queue_tx: PriorityQueue<TxMessage>,
    queue_rx: Mutex<OrderedQueue<Message>>,
    queue_retx: Mutex<OrderedQueue<TxMessage>>,
    // Counters
    count_rx: AtomicUsize,
    // Keeping track of the last unreliable sequence number
    sn_tx_reliable: Mutex<ZInt>,
    sn_tx_unreliable: Mutex<ZInt>,
    sn_rx_unreliable: Mutex<ZInt>,
    // The channel endpoints for terminating the consume_loop task
    signal_send: Sender<bool>,
    signal_recv: Receiver<bool>
}

impl Transport {
    pub(crate) fn new(lease: ZInt) -> Self {
        let (sender, receiver) = channel::<bool>(1);
        Self {
            session: RwLock::new(None), 
            callback: RwLock::new(None), 
            lease: RwLock::new(lease),
            links: Mutex::new(Vec::new()),
            queue_tx: PriorityQueue::new(QUEUE_TX_SIZE, QUEUE_TX_PRIO_NUM),
            queue_rx: Mutex::new(OrderedQueue::new(QUEUE_RX_SIZE)),
            queue_retx: Mutex::new(OrderedQueue::new(QUEUE_RETX_SIZE)),
            count_rx: AtomicUsize::new(0),
            sn_tx_reliable: Mutex::new(0),
            sn_tx_unreliable: Mutex::new(0),
            sn_rx_unreliable: Mutex::new(0),
            signal_send: sender,
            signal_recv: receiver
        }
    }

    pub(crate) fn initialize(&self, session: Arc<Session>, callback: Arc<dyn MsgHandler + Send + Sync>) {
        *self.session.try_write().unwrap() = Some(session);
        *self.callback.try_write().unwrap() = Some(callback);
    }

    pub(crate) async fn get_lease(&self) -> ZInt {
        *self.lease.read().await
    }

    pub(crate) async fn set_lease(&self, lease: ZInt) {
        *self.lease.write().await = lease
    }

    pub(crate) async fn get_links(&self) -> Vec<Link> {
        let mut vec = Vec::new();
        for l in self.links.lock().await.iter() {
            vec.push(l.clone());
        }
        vec
    }

    pub(crate) fn start(transport: Arc<Transport>) {
        task::spawn(async move {
            consume_loop(transport).await;
        });
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
            if guard[i].get_src() == *src &&
                    guard[i].get_dst() == *dst {
                found = true;
                index = i;
                break
            }
        }

        match found {
            true => Some(index),
            false => None
        }
    }

    pub(crate) async fn add_link(&self, link: Link) -> ZResult<()> {
        // println!("Adding link {} => {} to {:?}", link.get_src(), link.get_dst(), self.get_peer());
        let mut guard = self.links.lock().await;
        match self.find_link(&guard, &link.get_src(), &link.get_dst()) {
            Some(_) => Err(zerror!(ZErrorKind::Other{
                descr: format!("Trying to delete a link that does not exist!")
            })),
            None => Ok(guard.push(link))
        }
    }

    pub(crate) async fn del_link(&self, src: &Locator, dst: &Locator, _reason: Option<ZError>) -> ZResult<Link> {    
        // println!("Deleting link {} => {} from {:?}", src, dst, self.get_peer());
        let mut guard = self.links.lock().await;
        match self.find_link(&guard, src, dst) {
            Some(index) => Ok(guard.remove(index)),
            None => Err(zerror!(ZErrorKind::Other{
                descr: format!("Trying to delete a link that does not exist!")
            }))
        }
    }


    /*************************************/
    /*             SCHEDULE              */
    /*************************************/
    // Schedule the message to be sent asynchronsly
    pub(crate) async fn schedule_ctrl(&self, message: Message, link: Option<(Locator, Locator)>) {
        self.schedule_inner(message, QUEUE_TX_PRIO_CTRL, link).await;
    }

    pub(crate) async fn schedule_data(&self, message: Message) {
        self.schedule_inner(message, QUEUE_TX_PRIO_DATA, None).await;
    }

    async fn schedule_inner(&self, message: Message, priority: usize, link: Option<(Locator, Locator)>) {
        let msg = TxMessage {
            inner: message,
            link,
            notify: None
        };
        self.queue_tx.push(msg, priority).await;
    }

    // Send the message in a synchronous way. 
    // 1) Schedule the message 
    // 2) Be notified when the message is actually sent
    pub(crate) async fn send_ctrl(&self, message: Message, link: Option<(Locator, Locator)>) -> ZResult<()> {
        self.send_inner(message, QUEUE_TX_PRIO_CTRL, link).await
    }

    pub(crate) async fn send_data(&self, message: Message) -> ZResult<()> {
        self.send_inner(message, QUEUE_TX_PRIO_DATA, None).await
    }

    async fn send_inner(&self, message: Message, priority: usize, link: Option<(Locator, Locator)>) -> ZResult<()> {
        let (sender, receiver) = channel::<ZResult<()>>(1);
        let msg = TxMessage {
            inner: message,
            link,
            notify: Some(sender)
        };
        self.queue_tx.push(msg, priority).await;
        match receiver.recv().await {
            Some(res) => res,
            None => Err(zerror!(ZErrorKind::Other{
                descr: format!("Message dropped!")
            }))
        }
    }

    /*************************************/
    /*   MESSAGE RECEIVE FROM THE LINK   */
    /*************************************/
    async fn process_reliable_message(&self, message: Message, sn: ZInt) -> ZResult<()> {
        let mut l_guard = self.queue_rx.lock().await;

        match l_guard.try_push(message, sn) {
            None => {
                while let Some(message) = l_guard.try_pop() {
                    zrwopt!(self.callback).handle_message(message).await?;
                    self.count_rx.fetch_add(1, Ordering::Relaxed);
                }
                if self.count_rx.load(Ordering::Relaxed) >= QUEUE_RX_SIZE/2 {
                    true
                } else {
                    false
                }
            },
            Some(_) => {
                // The queue is out of synch, need to synchronize with the peer
                // Drop the message for the time being, it will be resent later
                true
            }
        };

        // if synch {     
        //     // Create the AckNack message
        //     let sn = l_guard.get_base();
        //     let mask = match l_guard.get_mask() {
        //         0 => None,
        //         m => Some(m)
        //     };
        //     let cid = None;
        //     let properties = None;
        //     let acknack = Arc::new(Message::make_ack_nack(sn, mask, cid, properties));

        //     // Schedule the AckNack message
        //     let link = None;
        //     self.schedule_ctrl(acknack, link).await;
        // }

        Ok(())
    }

    async fn process_unreliable_message(&self, message: Message, sn: ZInt) -> ZResult<()> {
        let mut l_guard = self.sn_rx_unreliable.lock().await;
        let gap = sn.wrapping_sub(*l_guard);
        if gap < ZInt::max_value()/2 {
            *l_guard = sn;
            zrwopt!(self.callback).handle_message(message).await?;
        }
        Ok(())
    }

    async fn process_acknack(&self, sn: ZInt, mask: Option<ZInt>) -> ZResult<()> {
        let mut guard = self.queue_retx.lock().await;

        // Set the base of the queue and receive back the removed messages 
        let mut messages = guard.set_base(sn);

        // Notify the synchronous send that the reliable messages have been acknowledged
        for m in messages.drain(..) {
            if let Some(sender) = &m.notify {
                sender.send(Ok(())).await;
            }
        }

        // If there is a mask, schedule the retransmission of messages
        if let Some(mut mask) = mask {
            let mut sn = guard.get_base();
            let count = mask.count_ones();
            for _ in 0..count {
                // Increment the sn and shift the mask
                while (mask & 1) != 1 {
                    sn = sn.wrapping_add(1);
                    mask = mask >> 1;
                }
                // Reschedule the message for retransmission
                if let Some(message) = guard.try_remove(sn) {
                    self.queue_tx.push(message, QUEUE_TX_PRIO_RETX).await;
                }
            }
        }

        Ok(())
    }

    async fn receive_full_message(&self, src: &Locator, dst: &Locator, message: Message) -> ZResult<()> {
        match &message.body {
            Body::Accept{opid, apid, lease} => {
                zrwopt!(self.session).process_accept(src, dst, opid, apid, lease).await?;
            },
            Body::AckNack{sn, mask} => {
                let c_sn = *sn;
                let c_mask = *mask;
                self.process_acknack(c_sn, c_mask).await?;
            },
            Body::Close{pid, reason} => {
                zrwopt!(self.session).process_close(src, dst, pid, reason).await?;
            },
            // Body::Hello{whatami, locators} => {},
            Body::Hello{..} => {},
            // Body::KeepAlive{pid} => {},
            Body::KeepAlive{..} => {},
            Body::Open{version, whatami, pid, lease, locators} => {
                zrwopt!(self.session).process_open(src, dst, version, whatami, pid, lease, locators).await?;
            },
            // Body::Ping{hash} => {},
            Body::Ping{..} => {},
            // Body::Pong{hash} => {},
            Body::Pong{..} => {},
            // Body::Scout{what} => {},
            Body::Scout{..} => {},
            // Body::Sync{sn, count} => {}
            Body::Sync{..} => {},
            Body::Data{reliable, sn, key: _, info: _, payload: _} => {
                let c_sn = *sn;
                match reliable {
                    true => self.process_reliable_message(message, c_sn).await?,
                    false => self.process_unreliable_message(message, c_sn).await?,
                }
            },
            Body::Declare{sn, declarations: _} |
            Body::Pull{sn, key: _, pull_id: _, max_samples: _} |
            Body::Query{sn, key: _, predicate: _, qid: _, target: _, consolidation: _} => {
                let c_sn = *sn;
                self.process_reliable_message(message, c_sn).await?;
            }
        }

        Ok(())
    }

    async fn receive_first_fragement(&self, _src: &Locator, _dst: &Locator, _message: Message, _number: Option<ZInt>) -> ZResult<()> {
        unimplemented!("Defragementation not implemented yet!");
    }

    async fn receive_middle_fragement(&self, _src: &Locator, _dst: &Locator, _message: Message) -> ZResult<()> {
        unimplemented!("Defragementation not implemented yet!");
    }

    async fn receive_last_fragement(&self, _src: &Locator, _dst: &Locator, _message: Message) -> ZResult<()> {
        unimplemented!("Defragementation not implemented yet!");
    }

    pub async fn receive_message(&self, src: &Locator, dst: &Locator, message: Message) -> ZResult<()> {
        match message.kind {
            MessageKind::FullMessage =>
                self.receive_full_message(src, dst, message).await?,
            MessageKind::FirstFragment{n} =>
                self.receive_first_fragement(src, dst, message, n).await?,
            MessageKind::InbetweenFragment => 
                self.receive_middle_fragement(src, dst, message).await?,
            MessageKind::LastFragment => 
                self.receive_last_fragement(src, dst, message).await?
        }
        Ok(())
    }

    /*************************************/
    /*         CLOSE THE SESSION         */
    /*************************************/
    pub async fn close(&self, _reason: Option<ZError>) -> ZResult<()> {
        // Notify the callback
        zrwopt!(self.callback).close().await;

        // Stop the task
        self.stop().await;

        // Remove and close all the links
        for l in self.links.lock().await.drain(..) {
            l.close(None).await?;
        }

        // Remove the reference to the session
        *self.session.write().await = None;       

        Ok(())
    }
}

// impl Drop for Transport {
//     fn drop(&mut self) {
//         println!("> Dropping Transport ({:?})", self);
//     }
// }

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
