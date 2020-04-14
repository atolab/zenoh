use async_std::sync::{channel, Arc, Mutex, MutexGuard, RwLock, Sender};
use async_std::task;
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::core::{AtomicZInt, ZError, ZErrorKind, ZInt, ZResult};
use crate::io::{RBuf, WBuf};
use crate::link::{Link, Locator};
use crate::proto::{Body, Message, MessageKind, SeqNum, SeqNumGenerator};
use crate::session::{MsgHandler, SessionInner};
use crate::zerror;
use zenoh_util::collections::{CreditBuffer, CreditQueue};
use zenoh_util::{zasynclock, zrwopt};

// Constants for transmission queue
pub(crate) const PRIO_CTRL: usize = 0;
const SIZE_CTRL: usize = 16;
const CRED_CTRL: isize = 1;

pub(crate) const _PRIO_RETX: usize = 1;
const SIZE_RETX: usize = 256;
const CRED_RETX: isize = 1;

pub(crate) const PRIO_DATA: usize = 2;
const SIZE_DATA: usize = 256;
const CRED_DATA: isize = 100;

const QUEUE_SIZE: usize = SIZE_CTRL + SIZE_RETX + SIZE_DATA;
const CONCURRENCY: usize = 16;

const WRITE_MSG_SLICE_SIZE: usize = 128;

async fn consume_loop(transport: Arc<Transport>) {
    // Update the sequence number
    async fn update_sn(tx: &mut MutexGuard<'_, TransportInnerTx>, message: &mut Message) -> bool {
        let is_reliable = message.is_reliable();
        match message.body {
            Body::Data { ref mut sn, .. }
            | Body::Unit { ref mut sn, .. }
            | Body::Declare { ref mut sn, .. }
            | Body::Pull { ref mut sn, .. }
            | Body::Query { ref mut sn, .. } => {
                // Update the sequence number
                let sn_gen = match tx.sn.get_mut(&message.cid) {
                    Some(sn_gen) => sn_gen,
                    // This CID does not exist, return
                    // @TODO: Perform the mapping of the CID?
                    None => return false,
                };
                *sn = if is_reliable {
                    sn_gen.reliable.get()
                } else {
                    sn_gen.unreliable.get()
                };
            }
            _ => {}
        }
        true
    }
    // The loop to consume the messages in the queue
    let batchsize = transport.batchsize.load(Ordering::Relaxed);
    let mut messages: Vec<MessageTx> = Vec::with_capacity(QUEUE_SIZE);
    // Create a buffer for the batching
    let mut batch = WBuf::new(WRITE_MSG_SLICE_SIZE);
    // Create a buffer to serialize each message on
    let mut buff = WBuf::new(WRITE_MSG_SLICE_SIZE);

    // Loop consuming the messages from the queue
    while transport.is_active() {
        // @TODO: Implement the reliability queue
        // @TODO: Implement the fragmentation

        // Drain all the messages from the queue
        // drain_into() waits for the queue to be non-empty
        transport.queue_tx.drain_into(&mut messages).await;

        // Keep draining
        while !messages.is_empty() {
            // Process all the messages just drained
            for mut msg in messages.drain(..) {
                // Acquire the tx lock for updating the SN and transmission
                let mut tx = zasynclock!(transport.tx);

                // Update the sequence number
                update_sn(&mut tx, &mut msg.inner).await;

                // Clear the message buffer
                buff.clear();
                // Serialize the message on the buffer
                buff.write_message(&msg.inner);

                // Create the RBuf out of batch and buff WBuff for transmission
                let batch_read = batch.as_rbuf();
                let buff_read = buff.as_rbuf();

                if msg.link.is_some() || msg.notify.is_some() {
                    // We are in a special case: a specific link has been indicated for the message
                    // or the message is sent in a synchronous way using the send()
                    // Transmit now the message
                    transport
                        .transmit(&tx.links, buff_read, msg.link, msg.notify)
                        .await;
                    // We are done with this message, continue with the following one
                    continue;
                } else if batch_read.len() + buff_read.len() > batchsize {
                    // The message does not fit in the batch, first transmit the current batch
                    transport.transmit(&tx.links, batch_read, None, None).await;
                    // Clear the batch buffer
                    batch.clear();
                }
                // Add the message to the batch
                let slices = buff_read.get_slices();
                for s in slices.iter() {
                    batch.add_slice(s.clone());
                }
            }
            // Try to drain more messages from the queue
            // The try_drain_into() does not wait for the queue to have messages as opposed to drain_into()
            transport.queue_tx.try_drain_into(&mut messages).await;
        }
        // The last try_drain_into() operation returned no messages
        // Transmit all the messages left in the batch
        let batch_read = batch.as_rbuf();
        if !batch_read.is_empty() {
            // Acquire the tx lock for transmission
            let tx = zasynclock!(transport.tx);
            // Transmit the last chunk of the batch
            transport
                .transmit(&tx.links, batch.as_rbuf(), None, None)
                .await;
            // Clear the batch buffer
            batch.clear();
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
    pub notify: Option<Sender<ZResult<()>>>,
}

// Structs to manage the sequence numbers of channels and conduits
struct ConduitTxSeqNum {
    reliable: SeqNumGenerator,
    unreliable: SeqNumGenerator,
}

impl ConduitTxSeqNum {
    fn new(sn0_reliable: ZInt, sn0_unreliable: ZInt, resolution: ZInt) -> ConduitTxSeqNum {
        ConduitTxSeqNum {
            reliable: SeqNumGenerator::make(sn0_reliable, resolution).unwrap(),
            unreliable: SeqNumGenerator::make(sn0_unreliable, resolution).unwrap(),
        }
    }
}

struct ConduitRxSeqNum {
    reliable: SeqNum,
    unreliable: SeqNum,
}

impl ConduitRxSeqNum {
    fn new(sn0_reliable: ZInt, sn0_unreliable: ZInt, resolution: ZInt) -> ConduitRxSeqNum {
        ConduitRxSeqNum {
            reliable: SeqNum::make(sn0_reliable, resolution).unwrap(),
            unreliable: SeqNum::make(sn0_unreliable, resolution).unwrap(),
        }
    }
}

// Define two different structs for Transmission and Reception
struct TransportInnerTx {
    // The conduits sequence number generators
    sn: HashMap<ZInt, ConduitTxSeqNum>,
    // The list of transport links associated to this session
    links: Vec<Link>,
}

impl TransportInnerTx {
    fn new(resolution: ZInt) -> TransportInnerTx {
        let mut tx = TransportInnerTx {
            sn: HashMap::new(),
            links: Vec::new(),
        };

        // Build the conduit zero sequence number manager
        let zero: ZInt = 0;
        tx.add_conduit(zero, resolution);

        tx
    }

    #[inline]
    fn add_conduit(&mut self, id: ZInt, resolution: ZInt) {
        // @TODO: randomly create the initial sequence numbers
        let zero: ZInt = 0;
        self.sn
            .insert(id, ConduitTxSeqNum::new(zero, zero, resolution));
    }
}

struct TransportInnerRx {
    // The conduits sequence numbers
    sn: HashMap<ZInt, ConduitRxSeqNum>,
}

impl TransportInnerRx {
    fn new(resolution: ZInt) -> TransportInnerRx {
        let mut rx = TransportInnerRx { sn: HashMap::new() };

        // Build the conduit zero sequence number manager
        let zero: ZInt = 0;
        rx.add_conduit(zero, resolution);

        rx
    }

    #[inline]
    fn add_conduit(&mut self, id: ZInt, resolution: ZInt) {
        self.sn.insert(
            id,
            ConduitRxSeqNum::new(resolution - 1, resolution - 1, resolution),
        );
    }
}

// Struct implementing the transport
pub struct Transport {
    // The reference to the session
    session: RwLock<Option<Arc<SessionInner>>>,
    // The callback for Data messages
    callback: RwLock<Option<Arc<dyn MsgHandler + Send + Sync>>>,
    // Determines if this transport is associated to the initial session
    is_initial: bool,
    // Session is active or not
    active: AtomicBool,
    // The default timeout after which the session is closed if no messages are received
    lease: AtomicZInt,
    // The default resolution to be used for the SN
    resolution: AtomicZInt,
    // The default mtu
    mtu: AtomicUsize,
    // The default batchsize
    batchsize: AtomicUsize,
    // The queue of messages to be transmitted
    queue_tx: CreditQueue<MessageTx>,
    // The transport and reception inner data structures
    tx: Mutex<TransportInnerTx>,
    rx: Mutex<TransportInnerRx>,
}

impl Transport {
    pub(crate) fn new(
        lease: ZInt,
        resolution: ZInt,
        batchsize: usize,
        is_initial: bool,
    ) -> Transport {
        // Build the transmission queue
        let ctrl = CreditBuffer::<MessageTx>::new(
            SIZE_CTRL,
            CRED_CTRL,
            CreditBuffer::<MessageTx>::spending_policy(|_msg| 0isize),
        );
        let retx = CreditBuffer::<MessageTx>::new(
            SIZE_RETX,
            CRED_RETX,
            CreditBuffer::<MessageTx>::spending_policy(|_msg| 0isize),
        );
        let data = CreditBuffer::<MessageTx>::new(
            SIZE_DATA,
            CRED_DATA,
            // @TODO: Once the reliability queue is implemented, update the spending policy
            CreditBuffer::<MessageTx>::spending_policy(|_msg| 0isize),
        );
        let queue_tx = vec![ctrl, retx, data];

        Transport {
            // Session manager and Callback
            session: RwLock::new(None),
            callback: RwLock::new(None),
            is_initial,
            // Session is active or not
            active: AtomicBool::new(false),
            // Session lease
            lease: AtomicZInt::new(lease),
            // Session sequence number resolution
            resolution: AtomicZInt::new(resolution),
            // The MTU is initialized to 0
            mtu: AtomicUsize::new(0),
            // The default batchsize
            batchsize: AtomicUsize::new(batchsize),
            // The transmission queue
            queue_tx: CreditQueue::new(queue_tx, CONCURRENCY),
            // The sequence number generator
            tx: Mutex::new(TransportInnerTx::new(resolution)),
            rx: Mutex::new(TransportInnerRx::new(resolution)),
        }
    }

    pub(crate) fn initialize(
        &self,
        session: Arc<SessionInner>,
        callback: Arc<dyn MsgHandler + Send + Sync>,
    ) {
        *self.session.try_write().unwrap() = Some(session);
        *self.callback.try_write().unwrap() = Some(callback);
    }

    pub(crate) fn start(transport: Arc<Transport>) {
        transport.active.store(true, Ordering::Relaxed);
        task::spawn(consume_loop(transport));
    }

    pub(crate) async fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    pub(crate) async fn set_lease(&self, lease: ZInt) {
        self.lease.store(lease, Ordering::Release);
    }

    #[allow(dead_code)]
    pub(crate) async fn get_lease(&self) -> ZInt {
        self.lease.load(Ordering::Relaxed)
    }

    #[allow(dead_code)]
    pub(crate) async fn set_batch_size(&self, size: usize) {
        // Acquire the lock on the links, this is necessary to ensure
        // that the right batch size is activated based on the available
        // MTUs of the various links
        let guard = zasynclock!(self.tx);
        self.batchsize.store(size, Ordering::Relaxed);
        self.update_mtu_and_batchsize(&guard.links);
    }

    #[allow(dead_code)]
    pub(crate) async fn set_resolution(&self, resolution: ZInt) {
        let mut g_tx = zasynclock!(self.tx);
        let mut g_rx = zasynclock!(self.rx);
        for (_, v) in g_tx.sn.iter_mut() {
            *v = ConduitTxSeqNum::new(
                v.reliable.get() % resolution,
                v.unreliable.get() % resolution,
                resolution,
            );
        }
        for (_, v) in g_rx.sn.iter_mut() {
            *v = ConduitRxSeqNum::new(
                v.reliable.get() % resolution,
                v.unreliable.get() % resolution,
                resolution,
            );
        }
        self.resolution.store(resolution, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    pub(crate) async fn get_resolution(&self) -> ZInt {
        self.resolution.load(Ordering::Acquire)
    }

    pub(crate) async fn get_links(&self) -> Vec<Link> {
        let mut vec = Vec::new();
        for l in self.tx.lock().await.links.iter() {
            vec.push(l.clone());
        }
        vec
    }

    /*************************************/
    /*               LINK                */
    /*************************************/
    fn find_link(&self, links: &[Link], src: &Locator, dst: &Locator) -> Option<usize> {
        for (i, l) in links.iter().enumerate() {
            if l.get_src() == *src && l.get_dst() == *dst {
                return Some(i);
            }
        }

        None
    }

    fn update_mtu_and_batchsize(&self, links: &[Link]) {
        // Find the minimum MTU
        let mut min_mtu: Option<usize> = None;
        for l in links {
            if let Some(mtu) = min_mtu {
                let l_mtu = l.get_mtu();
                if l_mtu < mtu {
                    min_mtu = Some(l_mtu);
                }
            } else {
                min_mtu = Some(l.get_mtu());
            }
        }
        // If the minimum MTU is found, update the batchsize accordingly.
        // The active batchsize must be <= min(MTU)
        if let Some(min) = min_mtu {
            // Update the minimum MTU
            self.mtu.store(min, Ordering::Release);
            // Update the batchsize
            let batchsize: usize = self.batchsize.load(Ordering::Acquire);
            if min < batchsize {
                self.batchsize.store(min, Ordering::Release);
            } else {
                self.batchsize.store(batchsize, Ordering::Release);
            }
        } else {
            // Update the minimum MTU
            self.mtu.store(0, Ordering::Release);
            // Update the batchsize
            self.batchsize.store(0, Ordering::Release);
        }
    }

    pub(crate) async fn add_link(&self, link: Link) -> ZResult<()> {
        // Acquire the lock on the links
        let mut guard = zasynclock!(self.tx);
        // Check if this link is not already present
        if self
            .find_link(&guard.links, &link.get_src(), &link.get_dst())
            .is_some()
        {
            return Err(zerror!(ZErrorKind::Other {
                descr: "Trying to add a link that already exists!".to_string()
            }));
        }

        // Add the link to the transport
        guard.links.push(link);

        // Upadate the active MTU and batch size according to the capabilities of the links
        self.update_mtu_and_batchsize(&guard.links);
        Ok(())
    }

    pub(crate) async fn del_link(&self, src: &Locator, dst: &Locator) -> ZResult<Link> {
        // Acquire the lock on the links
        let mut guard = zasynclock!(self.tx);
        // Check if the link is present in the transport
        let index = self.find_link(&guard.links, src, dst);
        // The link is not present, return an error
        let link = if let Some(i) = index {
            // Remove the link
            guard.links.remove(i)
        } else {
            return Err(zerror!(ZErrorKind::Other {
                descr: "Trying to delete a link that does not exist!".to_string()
            }));
        };
        // Check if there are links left
        if guard.links.is_empty() {
            // Drop the guard to allow the cleanup
            drop(guard);
            // If this transport is not associated to the initial session then
            // close the session, notify the manager, and notify the callback
            if !self.is_initial {
                // Remove the session from the manager
                let _ = zrwopt!(self.session).delete().await;
                let _ = self.close().await;
            }
        } else {
            // Update the active MTU and batch size according to the capabilities of the links
            self.update_mtu_and_batchsize(&guard.links);
        }

        Ok(link)
    }

    /*************************************/
    /*       SCHEDULE AND TRANSMIT       */
    /*************************************/
    // Schedule the message to be sent asynchronsly
    pub(crate) async fn schedule(
        &self,
        message: Message,
        link: Option<(Locator, Locator)>,
        priority: usize,
    ) {
        let message = MessageTx {
            inner: message,
            link,
            notify: None,
        };
        // Wait for the queue to have space for the message
        self.queue_tx.push(message, priority).await;
    }

    // Schedule the message to be sent asynchronsly and notify once sent
    pub(crate) async fn send(
        &self,
        message: Message,
        link: Option<(Locator, Locator)>,
        priority: usize,
    ) -> ZResult<()> {
        let (sender, receiver) = channel::<ZResult<()>>(1);
        let message = MessageTx {
            inner: message,
            link,
            notify: Some(sender),
        };
        // Wait for the transmission queue to have space for the message
        self.queue_tx.push(message, priority).await;
        // Wait for the message to be actually sent
        match receiver.recv().await {
            Some(res) => res,
            None => Err(zerror!(ZErrorKind::Other {
                descr: "Send failed unexpectedly!".to_string()
            })),
        }
    }

    async fn transmit(
        &self,
        links: &[Link],
        buffer: RBuf,
        link: Option<(Locator, Locator)>,
        notify: Option<Sender<ZResult<()>>>,
    ) {
        // Send the message on the link(s)
        let res = match link {
            // Send the message to the indicated link
            Some((src, dst)) => match self.find_link(links, &src, &dst) {
                Some(index) => links[index].send(buffer).await,
                None => Err(zerror!(ZErrorKind::Other {
                    descr: format!(
                        "Message dropped because link ({} => {}) was not found!",
                        &src, &dst
                    )
                })),
            },
            None => {
                // Send the message on the first link
                // @TODO: Adopt an intelligent selection of the links to use
                match links.get(0) {
                    Some(link) => link.send(buffer).await,
                    None => Err(zerror!(ZErrorKind::Other {
                        descr: "Message dropped because transport has no links!".to_string()
                    })),
                }
            }
        };

        if let Some(notify) = notify {
            notify.send(res).await;
        }
    }

    /*************************************/
    /*   MESSAGE RECEIVED FROM THE LINK  */
    /*************************************/
    async fn process_reliable_message(&self, message: Message, sn: ZInt) {
        // @TODO: implement the reordering and wait for missing messages
        let mut l_guard = zasynclock!(self.rx);
        // Messages with invalid CID or invalid SN are automatically dropped
        if let Some(rx_sn) = l_guard.sn.get_mut(&message.cid) {
            if rx_sn.reliable.precedes(sn) && rx_sn.reliable.set(sn).is_ok() {
                let _ = zrwopt!(self.callback).handle_message(message).await;
            }
        }
    }

    async fn process_unreliable_message(&self, message: Message, sn: ZInt) {
        let mut l_guard = zasynclock!(self.rx);
        // Messages with invalid CID or invalid SN are automatically dropped
        if let Some(rx_sn) = l_guard.sn.get_mut(&message.cid) {
            if rx_sn.unreliable.precedes(sn) && rx_sn.unreliable.set(sn).is_ok() {
                let _ = zrwopt!(self.callback).handle_message(message).await;
            }
        }
    }

    async fn receive_full_message(
        &self,
        src: &Locator,
        dst: &Locator,
        message: Message,
    ) -> Option<Arc<Self>> {
        match &message.body {
            Body::Accept {
                whatami,
                opid,
                apid,
                lease,
            } => {
                let c_lease = *lease;
                match zrwopt!(self.session)
                    .process_accept(src, dst, whatami, opid, apid, c_lease)
                    .await
                {
                    Ok(transport) => Some(transport),
                    Err(_) => None,
                }
            }
            Body::AckNack { .. } => {
                unimplemented!("Handling of AckNack Messages not yet implemented!");
            }
            Body::Close { pid, reason } => {
                let c_reason = *reason;
                zrwopt!(self.session)
                    .process_close(src, dst, pid, c_reason)
                    .await;
                None
            }
            Body::Hello { .. } => {
                unimplemented!("Handling of Hello Messages not yet implemented!");
            }
            Body::KeepAlive { .. } => {
                unimplemented!("Handling of KeepAlive Messages not yet implemented!");
            }
            Body::Open {
                version,
                whatami,
                pid,
                lease,
                locators,
            } => {
                let c_version = *version;
                let c_lease = *lease;
                match zrwopt!(self.session)
                    .process_open(src, dst, c_version, whatami, pid, c_lease, locators)
                    .await
                {
                    Ok(transport) => Some(transport),
                    Err(_) => None,
                }
            }
            Body::Ping { .. } => {
                unimplemented!("Handling of Ping Messages not yet implemented!");
            }
            Body::Pong { .. } => {
                unimplemented!("Handling of Pong Messages not yet implemented!");
            }
            Body::Scout { .. } => {
                unimplemented!("Handling of Scout Messages not yet implemented!");
            }
            Body::Sync { .. } => {
                unimplemented!("Handling of Sync Messages not yet implemented!");
            }
            Body::Data { reliable, sn, .. } | Body::Unit { reliable, sn } => {
                let c_sn = *sn;
                match reliable {
                    true => self.process_reliable_message(message, c_sn).await,
                    false => self.process_unreliable_message(message, c_sn).await,
                }
                None
            }
            Body::Declare { sn, .. } | Body::Pull { sn, .. } | Body::Query { sn, .. } => {
                let c_sn = *sn;
                self.process_reliable_message(message, c_sn).await;
                None
            }
        }
    }

    async fn receive_first_fragement(
        &self,
        _src: &Locator,
        _dst: &Locator,
        _message: Message,
        _number: Option<ZInt>,
    ) -> Option<Arc<Self>> {
        unimplemented!("Defragementation not implemented yet!");
    }

    async fn receive_middle_fragement(
        &self,
        _src: &Locator,
        _dst: &Locator,
        _message: Message,
    ) -> Option<Arc<Self>> {
        unimplemented!("Defragementation not implemented yet!");
    }

    async fn receive_last_fragement(
        &self,
        _src: &Locator,
        _dst: &Locator,
        _message: Message,
    ) -> Option<Arc<Self>> {
        unimplemented!("Defragementation not implemented yet!");
    }

    pub async fn receive_message(
        &self,
        src: &Locator,
        dst: &Locator,
        message: Message,
    ) -> Option<Arc<Self>> {
        match message.kind {
            MessageKind::FullMessage => self.receive_full_message(src, dst, message).await,
            MessageKind::FirstFragment { n } => {
                self.receive_first_fragement(src, dst, message, n).await
            }
            MessageKind::InbetweenFragment => {
                self.receive_middle_fragement(src, dst, message).await
            }
            MessageKind::LastFragment => self.receive_last_fragement(src, dst, message).await,
        }
    }

    /*************************************/
    /*        CLOSE THE TRANSPORT        */
    /*************************************/
    pub async fn close(&self) -> ZResult<()> {
        // Notify the callback
        zrwopt!(self.callback).close().await;

        // Stop the task
        self.stop().await;

        // Remove and close all the links
        for l in self.tx.lock().await.links.drain(..) {
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
            for l in self.tx.lock().await.links.iter() {
                s.push_str(&format!("\n\t[({:?}) => ({:?})]", l.get_src(), l.get_dst()));
            }
            s
        });
        write!(f, "Links:{}", links)
    }
}
