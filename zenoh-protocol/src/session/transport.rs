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
use async_std::task::{
    Context, 
    Poll,
    Waker
};
use crossbeam::queue::SegQueue;
use std::fmt;
use std::pin::Pin;
use std::sync::atomic::{
    // AtomicUsize,
    Ordering
};

use crate::{
    zerror,
    zrwopt
};
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
const QUEUE_TX_PRIO_NUM: usize = 1;
const QUEUE_TX_PRIO_DATA: usize = 0;

const QUEUE_RETX_SIZE: usize = 16;


async fn consume_loop(transport: Arc<Transport>) {
    async fn consume(transport: &Arc<Transport>) -> Option<bool> {
        let (mut message, _) = transport.queue_tx.pop().await;
        let is_reliable = message.inner.is_reliable();

        // Set the right sequence number for data messages
        let mut msg_sn: Option<ZInt> = None;
        match message.inner.body {
            Body::Data{reliable: _, ref mut sn, key: _, info: _, payload: _} |
            Body::Declare{ref mut sn, declarations: _} |
            Body::Pull{ref mut sn, key: _, pull_id: _, max_samples: _} |
            Body::Query{ref mut sn, key: _, predicate: _, qid: _, target: _, consolidation: _} => {
                // Update the sequence number if this message is not beinf retransmitted
                *sn = match is_reliable {
                    true => transport.sn_tx_reliable.fetch_add(1, Ordering::Relaxed),
                    false => transport.sn_tx_unreliable.fetch_add(1, Ordering::Relaxed),
                };
                msg_sn = Some(*sn);
            },
            _ => {}
        }

        println!("\n> Sending: {:?} {:?}", msg_sn, message.inner);

        // Transmit the message on the link(s)
        let _ = transport.transmit(&message).await;

        if message.inner.is_reliable() {
            // Don't notify the syncrhonous sender, it will be notified when an ACK is received 
            println!("\n> Consume: OH YES {:?}", msg_sn);
            // Add the message to the retransmission queue if the message is data or 
            // already retransmitted and reliable
            if let Some(sn) = msg_sn {
                // If the retransmission queue is full, we need to send a SYNC message 
                if let Some(_) = transport.queue_retx.write().await.try_push(message, sn) {
                    println!("> Consume: OH SHIT");
                    // Trigger the synchronization
                    transport.synchronize().await;
                    println!("> Consume: LET'S PROCEED");
                }
            }
        }

        println!("> Consume: RETURN");

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

// Struct to add additional fields to the message
struct TxMessage {
    // The inner message to transmit
    inner: Message, 
    // The preferred link to transmit the Message on
    link: Option<(Locator, Locator)>
}


// Struct implementing the transport
pub struct Transport {
    // The reference to the session
    session: RwLock<Option<Arc<Session>>>,
    // The callback for Data messages
    callback: RwLock<Option<Arc<dyn MsgHandler + Send + Sync>>>,
    // The timeout after which the session is closed if no messages are received
    lease: AtomicZInt,
    // The list of transport links associated to this session
    links: Mutex<Vec<Link>>,
    // The queue of messages to be transmitted
    queue_tx: PriorityQueue<TxMessage>,
    queue_rx: Mutex<OrderedQueue<Message>>,
    queue_retx: RwLock<OrderedQueue<TxMessage>>,
    // Store the wakers for the synchronization future
    w_sync: SegQueue<Waker>,
    // Keeping track of the last sequence numbers
    sn_tx_reliable: AtomicZInt,
    sn_tx_unreliable: AtomicZInt,
    sn_rx_unreliable: Mutex<ZInt>, // A Mutex is required to avoid race conditions (same as queue_rx)
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
            lease: AtomicZInt::new(lease),
            links: Mutex::new(Vec::new()),
            queue_tx: PriorityQueue::new(QUEUE_TX_SIZE, QUEUE_TX_PRIO_NUM),
            queue_rx: Mutex::new(OrderedQueue::new(QUEUE_RX_SIZE)),
            queue_retx: RwLock::new(OrderedQueue::new(QUEUE_RETX_SIZE)),
            w_sync: SegQueue::new(),
            sn_tx_reliable: AtomicZInt::new(0),
            sn_tx_unreliable: AtomicZInt::new(0),
            sn_rx_unreliable: Mutex::new(0),
            signal_send: sender,
            signal_recv: receiver
        }
    }

    pub(crate) fn initialize(&self, session: Arc<Session>, callback: Arc<dyn MsgHandler + Send + Sync>) {
        *self.session.try_write().unwrap() = Some(session);
        *self.callback.try_write().unwrap() = Some(callback);
    }

    pub(crate) fn get_lease(&self) -> ZInt {
        self.lease.load(Ordering::Acquire)
    }

    pub(crate) fn set_lease(&self, lease: ZInt) {
        self.lease.store(lease, Ordering::Release);
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

        match found {
            true => Some(index),
            false => None
        }
    }

    pub(crate) async fn add_link(&self, link: Link) -> ZResult<()> {
        let mut guard = self.links.lock().await;
        match self.find_link(&guard, &link.get_src(), &link.get_dst()) {
            Some(_) => Err(zerror!(ZErrorKind::Other{
                descr: format!("Trying to delete a link that does not exist!")
            })),
            None => Ok(guard.push(link))
        }
    }

    pub(crate) async fn del_link(&self, src: &Locator, dst: &Locator, _reason: Option<ZError>) -> ZResult<Link> {    
        let mut guard = self.links.lock().await;
        match self.find_link(&guard, src, dst) {
            Some(index) => Ok(guard.remove(index)),
            None => Err(zerror!(ZErrorKind::Other{
                descr: format!("Trying to delete a link that does not exist!")
            }))
        }
    }


    /*************************************/
    /*    SCHEDULE, SEND AND TRANSMIT    */
    /*************************************/
    // Schedule the message to be sent asynchronsly
    pub(crate) async fn schedule(&self, message: Message, link: Option<(Locator, Locator)>) {
        let message = TxMessage {
            inner: message,
            link
        };
        if message.inner.is_reliable() {
            // Wait for the queue to have space for the message
            self.queue_tx.push(message, QUEUE_TX_PRIO_DATA).await;
        } else {
            // If the queue is full, the message is automatically dropped
            self.queue_tx.try_push(message, QUEUE_TX_PRIO_DATA);
        }
    }

    pub(crate) async fn send(&self, message: Message, link: Option<(Locator, Locator)>) -> ZResult<()> {
        let message = TxMessage {
            inner: message,
            link
        };
        self.transmit(&message).await
    }
    
    async fn transmit(&self, message: &TxMessage) -> ZResult<()> {
        // TODO: Fragement the message if too large

        // Send the message on the link(s)
        let guard = self.links.lock().await;
        match &message.link {
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
                // This might change in the future
                match guard.get(0) {
                    Some(link) => link.send(&message.inner).await,
                    None =>  Err(zerror!(ZErrorKind::Other{
                        descr: format!("Message dropped because transport has no links!")
                    }))
                }
            }
        }
    }

    /*************************************/
    /* SYNCHRONIZE THE RELIABLE CHANNEL  */
    /*************************************/
    async fn synchronize(&self) {
        struct FutureSynchronized<'a> {
            transport: &'a Transport
        }
        
        impl<'a> Future for FutureSynchronized<'a> {
            type Output = ();
        
            fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
                if let Some(guard) = self.transport.queue_retx.try_read() {
                    match guard.len() {
                        0 => return Poll::Ready(()),
                        _ => {
                            self.transport.w_sync.push(ctx.waker().clone());
                            return Poll::Pending
                        }
                    }
                }
                Poll::Pending
            }
        }

        let reliable = true;
        let sn = self.queue_retx.read().await.get_base();
        let count = Some(self.queue_retx.read().await.len() as ZInt);
        let cid = None;     // TODO: Retrive the right conduit ID
        let properties = None;
        let message = TxMessage{
            inner: Message::make_sync(reliable, sn, count, cid, properties),
            link: None
        };

        let _ = self.transmit(&message).await;

        // Wait to be synchronized: queue_retx.len() == 0
        FutureSynchronized {
            transport: self
        }.await
    }

    async fn acknowledge(&self, ) {
        println!("> Acknowledge: YES");
        let l_guard = self.queue_rx.lock().await;
        // Create the AckNack message
        let sn = l_guard.get_base();
        let mask = match l_guard.get_mask() {
            0 => None,
            m => Some(m)
        };
        let cid = None;
        let properties = None;
        let message = TxMessage {
            inner: Message::make_ack_nack(sn, mask, cid, properties),
            link: None
        };

        // Transmit the AckNack message
        let _ = self.transmit(&message).await;

        println!("> Acknowledge: RETURN");
    }

    /*************************************/
    /*   MESSAGE RECEIVED FROM THE LINK  */
    /*************************************/
    async fn process_reliable_message(&self, message: Message, sn: ZInt) {
        let mut l_guard = self.queue_rx.lock().await;
        // Add the message to the receiving queue and trigger an AckNack when necessary
        match l_guard.try_push(message, sn) {
            None => {
                println!("> Receive: OK");
                while let Some(message) = l_guard.try_pop() {
                    println!("> Receive: Forward");
                    let _ = zrwopt!(self.callback).handle_message(message).await;
                }
                // Try to avoid filling up the queue by sending an ack_nack earlier
                if l_guard.len() > QUEUE_RETX_SIZE/2 {
                    // Drop the guard to allow the acknowledge to access to the queue
                    drop(l_guard);
                    // Send an acknowledgment triggering the retransmission
                    self.acknowledge().await;
                }
            },
            Some(_) => {
                // Drop the guard to allow the acknowledge to access to the queue
                drop(l_guard);
                // Send an acknowledgment triggering the retransmission
                self.acknowledge().await;
            }
        }
    }

    async fn process_unreliable_message(&self, message: Message, sn: ZInt) {
        let mut l_guard = self.sn_rx_unreliable.lock().await;
        let gap = sn.wrapping_sub(*l_guard);
        if gap < ZInt::max_value()/2 {
            *l_guard = sn;
            let _ = zrwopt!(self.callback).handle_message(message).await;
        }
    }

    async fn process_acknack(&self, sn: &ZInt, mask: &Option<ZInt>) {
        println!("> ACKNACK: YES {} {:?}", sn, mask);
        let mut w_guard = self.queue_retx.write().await;
        // Set the base of the queue  
        w_guard.set_base(*sn);

        // If there is a mask, schedule the retransmission of requested messages
        if let Some(mut mask) = mask {
            println!("> ACKNACK: RETRANSMISSION");
            let mut sn = w_guard.get_base();
            let count = mask.count_ones();
            for _ in 0..count {
                // Increment the sn and shift the mask
                while (mask & 1) != 1 {
                    sn = sn.wrapping_add(1);
                    mask = mask >> 1;
                }
                // Retransmit the messages
                let res = w_guard.try_remove(sn);
                if let Some(message) = res {
                    // Transmit the message
                    let _ = self.transmit(&message).await;
                    // Reinsert the message in the queue 
                    w_guard.try_push(message, sn);
                }
            }
        }

        // Drop the guard allowing the read lock in the FutureSychronized
        drop(w_guard);

        // Wake up any pending futures for synchronization
        while let Ok(waker) = self.w_sync.pop() {
            waker.wake();
        }
    }

    async fn process_sync(&self, sn: &ZInt, count: &Option<ZInt>) {
        self.queue_rx.lock().await.set_base(*sn);
        if let Some(_) = count {
            self.acknowledge().await;
        }
    }

    async fn receive_full_message(&self, src: &Locator, dst: &Locator, message: Message) {
        match &message.body {
            Body::Accept{opid, apid, lease} => {
                zrwopt!(self.session).process_accept(src, dst, opid, apid, lease).await;
            },
            Body::AckNack{sn, mask} => {
                self.process_acknack(sn, mask).await;
            },
            Body::Close{pid, reason} => {
                zrwopt!(self.session).process_close(src, dst, pid, reason).await;
            },
            Body::Hello{whatami: _, locators: _} => {
                unimplemented!("Handling of Hello Messages not yet implemented!");
            },
            Body::KeepAlive{pid: _} => {
                unimplemented!("Handling of KeepAlive Messages not yet implemented!");
            },
            Body::Open{version, whatami, pid, lease, locators} => {
                zrwopt!(self.session).process_open(src, dst, version, whatami, pid, lease, locators).await;
            },
            Body::Ping{hash: _} => {
                unimplemented!("Handling of Ping Messages not yet implemented!");
            },
            Body::Pong{hash: _} => {
                unimplemented!("Handling of Pong Messages not yet implemented!");
            },
            Body::Scout{what: _} => {
                unimplemented!("Handling of Scout Messages not yet implemented!");
            },
            Body::Sync{sn, count} => {
                self.process_sync(sn, count).await;
            }
            Body::Data{reliable, sn, key: _, info: _, payload: _} => {
                let c_sn = *sn;
                match reliable {
                    true => self.process_reliable_message(message, c_sn).await,
                    false => self.process_unreliable_message(message, c_sn).await,
                }
            },
            Body::Declare{sn, declarations: _} |
            Body::Pull{sn, key: _, pull_id: _, max_samples: _} |
            Body::Query{sn, key: _, predicate: _, qid: _, target: _, consolidation: _} => {
                let c_sn = *sn;
                self.process_reliable_message(message, c_sn).await;
            }
        }
    }

    async fn receive_first_fragement(&self, _src: &Locator, _dst: &Locator, _message: Message, _number: Option<ZInt>) {
        unimplemented!("Defragementation not implemented yet!");
    }

    async fn receive_middle_fragement(&self, _src: &Locator, _dst: &Locator, _message: Message) {
        unimplemented!("Defragementation not implemented yet!");
    }

    async fn receive_last_fragement(&self, _src: &Locator, _dst: &Locator, _message: Message) {
        unimplemented!("Defragementation not implemented yet!");
    }

    pub async fn receive_message(&self, src: &Locator, dst: &Locator, message: Message) {
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
