use async_std::prelude::*;
use async_std::sync::{Arc, Barrier, Mutex, RwLock, Weak};
use async_std::task;
use async_trait::async_trait;
use futures::stream::{FuturesUnordered, StreamExt};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::core::{PeerId, ZError, ZErrorKind, ZInt, ZResult};
use crate::io::WBuf;
use crate::link::Link;
use crate::proto::{FramePayload, SessionBody, SessionMessage, SeqNum, SeqNumGenerator, WhatAmI, ZenohMessage, smsg};
use crate::session::{Action, MsgHandler, SessionManagerInner, TransportTrait};
use crate::session::defaults::{
    // Control buffer
    QUEUE_PRIO_CTRL,
    QUEUE_SIZE_CTRL,
    QUEUE_CRED_CTRL,
    // Retransmission buffer
    // QUEUE_PRIO_RETX,
    QUEUE_SIZE_RETX,
    QUEUE_CRED_RETX,
    // Data buffer
    QUEUE_PRIO_DATA,
    QUEUE_SIZE_DATA,
    QUEUE_CRED_DATA,
    // Queue size
    QUEUE_CONCURRENCY,
    // Default slice size when serializing a message that needs to be fragmented
    // WRITE_MSG_SLICE_SIZE
};
use crate::zerror;
use zenoh_util::collections::{CreditBuffer, CreditQueue};
use zenoh_util::collections::credit_queue::Drain as CreditQueueDrain;
use zenoh_util::{zasynclock, zasyncread, zasyncwrite};

// Macro to access the session Weak pointer
// macro_rules! zsession {
//     ($var:expr) => (
//         if let Some(inner) = $var.upgrade() { 
//             inner
//         } else { 
//             return Action::Close
//         }
//     );
// }

#[derive(Debug)]
enum MessageInner {
    Session(SessionMessage),
    Zenoh(ZenohMessage),
    Stop
}

struct MessageTx {
    // The inner message to transmit
    inner: MessageInner,
    // The preferred link to transmit the Message on
    link: Option<Link>
}

/*************************************/
/*           CHANNEL TASK            */
/*************************************/

// Always send on the first link for the time being
const DEFAULT_LINK_INDEX: usize = 0; 
const TWO_BYTES: [u8; 2] = [0u8, 0u8];

struct MBFContext {
    // The list of messages to transmit
    message: Option<MessageTx>,
    // Last sn
    sn: Option<ZInt>,
    // The buffer to fragment a message on
    fragment: Option<WBuf>
}

impl MBFContext {
    fn new() -> MBFContext {
        MBFContext {
            message: None,           
            fragment: None,
            sn: None
        }
    }

    fn clear(&mut self) {
        self.message = None;
        self.sn = None;
        self.fragment = None;
    }
}

struct SerializedBatch {
    buffer: WBuf,
    reserved: bool,
    empty: bool
}

impl SerializedBatch {
    fn new(size: usize, reserved: bool) -> SerializedBatch {
        let mut sb = SerializedBatch {
            buffer: WBuf::new(size, true), // Non-expandable batch
            reserved,
            empty: true
        };
        if sb.reserved {
            sb.buffer.write_bytes(&TWO_BYTES);
        }
        sb
    }

    fn write_session_message(&mut self, message: &SessionMessage) -> bool {
        let res = self.buffer.write_session_message(message);
        if res {
            self.empty = false;
        }
        res
    }

    fn write_zenoh_message(&mut self, message: &ZenohMessage) -> bool {
        let res = self.buffer.write_zenoh_message(message);
        if res {
            self.empty = false;
        }
        res
    }

    fn mark(&mut self) {
        self.buffer.mark();
    }

    fn revert(&mut self) {
        self.buffer.revert();
    }

    fn is_empty(&self) -> bool {
        self.empty
    }

    fn get_buffer(&self) -> &[u8] {
        self.buffer.get_first_slice(..)
    }

    fn clear(&mut self) {
        self.buffer.clear();        
        if self.reserved {
            self.buffer.write_bytes(&TWO_BYTES);
        }
        self.empty = true;
    }

    fn write_length(&mut self) {
        if self.reserved {
            let length: u16 = self.buffer.len() as u16 - 2;
            println!("\tWriting {} bytes", length);
            // Write the length on the first 16 bits
            let bits = self.buffer.get_first_slice_mut(..2);
            bits.copy_from_slice(&length.to_le_bytes());
        }
    }
}

async fn map_batch_fragment(
    links: &ChannelLinks,
    inner: &mut ChannelInnerTx,
    drain: &mut CreditQueueDrain<'_, MessageTx>,
    batches: &mut Vec<SerializedBatch>,
    context: &mut MBFContext
) -> Option<bool> {
    // None means that there is no current frame
    let mut is_current_frame_reliable = None;

    // @TODO: Implement fragmentation here

    // Check first if we have some message that did not fit in the previous batch
    if let Some(msg) = context.message.take() {
        // Find the right index for the link
        let index = if let Some(link) = &msg.link {
            // Check if the target link exists, otherwise fallback on the main link
            if let Some(index) = links.find_link_index(&link) {
                index
            } else {                              
                // Drop this message and clear the context
                context.clear();
                return Some(true)
            } 
        } else {
            DEFAULT_LINK_INDEX
        };

        match &msg.inner {
            MessageInner::Session(m) => {
                if !batches[index].write_session_message(&m) {
                    // This is a session message, nothing we can do for the time being
                    println!("!!! Session message dropped due to failed serialization");
                    // Clear the context
                    context.clear();
                }
            },
            MessageInner::Zenoh(m) => {
                // Check if this message was the first message of a frame or not
                let reliable = m.is_reliable();
                let sn = if let Some(sn) = context.sn.take() {
                    sn
                } else if reliable {
                    inner.sn.reliable.get()
                } else {
                    inner.sn.best_effort.get()
                };

                let payload = FramePayload::Messages { messages: vec![] };
                let attachment = None;
                let frame = SessionMessage::make_frame(reliable, sn, payload, attachment);

                batches[index].mark();

                let res = batches[index].write_session_message(&frame) &&
                          batches[index].write_zenoh_message(&m); 
                         
                if !res {       
                    // Revert the batch index
                    batches[index].revert();
                    // The frame header or the zenoh message does not fit in the current batch, 
                    // try to serialize them in the next serialization iteration
                    println!("!!! Zenoh message should be fragmented. However, fragmentation is not yet implemented. Dropping Zenoh message.");
                    // Clear the context
                    context.clear();
                }

                // Mark the first reliable zenoh message we are expecting
                is_current_frame_reliable = Some(reliable);
            },
            _ => {
                context.clear();
                return None
            }
        }
    }

    // Drain the messages from the queue and perform the mapping and serialization
    for msg in drain {
        // Find the right index for the link
        let index = if let Some(link) = &msg.link {
            // Check if the target link exists, otherwise fallback on the main link
            if let Some(index) = links.find_link_index(&link) {
                index
            } else {
                // Drop this message, continue with the following one
                continue;
            } 
        } else {
            DEFAULT_LINK_INDEX
        };

        // Serialize the message
        match &msg.inner {
            MessageInner::Session(m) => {
                if !batches[index].write_session_message(&m) {
                    // The message does not fit in the current batch, send it in the next iteration
                    context.message = Some(msg);
                    context.sn = None;
                    return Some(false)
                }               
            },
            MessageInner::Zenoh(m) => {
                // The message is reliable or not
                let reliable = m.is_reliable();
                // Get a new sequence number based on the current status
                let sn = if let Some(current_reliable) = is_current_frame_reliable {
                    // A frame is already being serialized. Check if we need to create a new frame
                    //  or we can just append the message to the current frame
                    if current_reliable && !reliable {
                        // We have been serialiazing a reliable frame but the message is best effort
                        // Need to create a new best effort frame
                        is_current_frame_reliable = Some(false);
                        Some(inner.sn.best_effort.get())
                    } else if !current_reliable && reliable {
                        // We have been serialiazing a best effor frame but the message is reliable
                        // Need to create a new reliable frame
                        is_current_frame_reliable = Some(true);
                        Some(inner.sn.reliable.get())
                    } else {
                        // The message is as reliable/best effort as the current frame
                        // We do not need to create any new frame
                        None
                    }
                } else if reliable {
                    // This is a new reliable frame
                    is_current_frame_reliable = Some(true);
                    Some(inner.sn.reliable.get())
                } else {
                    // This is a new best effort frame
                    is_current_frame_reliable = Some(false);
                    Some(inner.sn.best_effort.get())
                };

                // If a new sequence number has been provided, it means we are in the case we need 
                // to start a new frame. Write a new frame header.
                if let Some(sn) = sn {
                    // Serialize the new frame and the zenoh message
                    let payload = FramePayload::Messages { messages: vec![] };
                    let attachment = None;
                    let frame = SessionMessage::make_frame(reliable, sn, payload, attachment);

                    batches[index].mark();

                    let res = batches[index].write_session_message(&frame) &&
                                batches[index].write_zenoh_message(&m); 
                         
                    if !res {
                        // Revert the batch index
                        batches[index].revert();
                        // The frame header or the zenoh message does not fit in the current batch, 
                        // try to serialize them in the next serialization iteration
                        context.message = Some(msg);
                        context.sn = Some(sn);                          
                        return Some(false)
                    }  
                } else {
                    // Serialize only the zenoh message
                    let res = batches[index].write_zenoh_message(&m);
                    
                    if !res {
                        // The zenoh message does not fit in the current batch, 
                        // try to serialize them in the next serialization iteration
                        context.message = Some(msg);
                        context.sn = None;                          
                        return Some(false)
                    }
                }
            },
            MessageInner::Stop => {
                return None
            }
        }
    }

    Some(true)
}

// Consume function
async fn consume_task(ch: Arc<Channel>) {
    // @TODO: Implement the reliability queue
    // @TODO: Implement the fragmentation

    // Acquire the lock on the links
    let guard = zasyncread!(ch.links); 

    // Use double buffering to allow parallel serialization and transmission
    let mut batches_one: Vec<SerializedBatch> = Vec::with_capacity(guard.links.len());
    let mut batches_two: Vec<SerializedBatch> = Vec::with_capacity(guard.links.len());

    // Initialize the batches based on the current links parameters      
    for link in guard.links.iter() {
        let batchsize = guard.batchsize.min(link.get_mtu());
        let needs_write_length = link.is_streamed();
        batches_one.push(SerializedBatch::new(batchsize, needs_write_length));
        batches_two.push(SerializedBatch::new(batchsize, needs_write_length));
    }

    let links_serialize: ChannelLinks = guard.clone();
    let links_transmit: Vec<Link> = guard.links.clone();

    // Drop the mutex guard
    drop(guard);

    // Create a context for handling failed serialization or fragmentation
    let mut context = MBFContext::new();

    // Double buffer references
    let mut serialize = &mut batches_one;
    let mut transmit = &mut batches_two;

    // Keep the lock on the inner transmission structure
    let mut inner = zasynclock!(ch.tx);

    // Control variable
    let mut active = true; 

    while active {       
        println!("\n&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&"); 

        // Explicit scope for transmit mutability
        {
            // Serialization future
            let f_sr = async {                             
                // Get a Drain iterator for the queue
                // drain() waits for the queue to be non-empty
                // @TODO: add a timeout to the drain() future to trigger the 
                //        transmission of KEEP_ALIVE messages
                let mut drain = ch.queue.drain().await;
                
                // Try to always fill the batch
                loop {
                    // Map the messages on the links. This operation drains messages from the Drain iterator
                    let res = map_batch_fragment(&links_serialize, &mut inner, &mut drain, &mut serialize, &mut context).await;
                    
                    // The drop() on Drain object needs to be manually called since an async
                    // destructor is not yet supported in Rust. More information available at:
                    // https://internals.rust-lang.org/t/asynchronous-destructors/11127/47 
                    drain.drop().await;
                    
                    if let Some(iterate) = res {
                        if iterate {
                            // Deschedule the task to allow other tasks to be scheduled and eventually push on the queue
                            task::yield_now().await;
                            
                            // Try to drain messages from the queue
                            // try_drain does not wait for the queue to be non-empty
                            drain = ch.queue.try_drain().await;
                            // Check if we can drain from the Drain iterator
                            let (min, _) = drain.size_hint();
                            if min == 0 {
                                // The drop() on Drain object needs to be manually called since an async
                                // destructor is not yet supported in Rust. More information available at:
                                // https://internals.rust-lang.org/t/asynchronous-destructors/11127/47
                                drain.drop().await;
                                break
                            }
                        } else {
                            // No more messages to serialize
                            break
                        }
                    } else {
                        // We received a stop signal, finish this iteration and exit
                        active = false;
                        break
                    }
                }

                // Update the length (16 bits)
                for b in serialize.iter_mut() {
                    b.write_length();
                }
            };

            // Transmission future
            let futs: FuturesUnordered<_> = FuturesUnordered::new();
            // Concurrently send on all the selected links
            for (i, link) in links_transmit.iter().enumerate() {                
                if !transmit[i].is_empty() {
                    let buffer = transmit[i].get_buffer();
                    // Push the future to the future list
                    futs.push(async move {
                        let res = link.send(buffer).await;
                        res
                    });                    
                }
            }
            let f_tx = futs.into_future();            

            // Join serialization and transmission futures
            let _ = f_sr.join(f_tx).await;

            // @TODO: Handle res
        }

        // Clean transmit batches for the next round of serialization
        for b in transmit.iter_mut() {
            b.clear();
        }

        // Swap the serialize and transmit pointers for the next iteration
        std::mem::swap(&mut serialize, &mut transmit);

        // Deschedule the task to allow other tasks to be scheduled and eventually push on the queue
        task::yield_now().await;
    }

    // Send any leftover on the transmission batch
    // Transmission future
    let futs: FuturesUnordered<_> = FuturesUnordered::new();
    // Concurrently send on all the selected links
    for (i, link) in links_transmit.iter().enumerate() {                
        if !transmit[i].is_empty() {
            let buffer = transmit[i].get_buffer();
            // Push the future to the future list
            futs.push(async move {
                let res = link.send(buffer).await;
                res
            });                    
        }
    }
    let _ = futs.into_future().await;

    // Synchronize with the stop()
    ch.barrier.wait().await;
}

/*************************************/
/*      CHANNEL INNER TX STRUCT      */
/*************************************/

// Structs to manage the sequence numbers of channels
struct SeqNumTx {
    reliable: SeqNumGenerator,
    best_effort: SeqNumGenerator,
}

impl SeqNumTx {
    fn new(sn_resolution: ZInt, initial_sn: ZInt) -> SeqNumTx {
        SeqNumTx {
            reliable: SeqNumGenerator::make(initial_sn, sn_resolution).unwrap(),
            best_effort: SeqNumGenerator::make(initial_sn, sn_resolution).unwrap(),
        }
    }
}

// Store the mutable data that need to be used for transmission
struct ChannelInnerTx {
    sn: SeqNumTx
}

impl ChannelInnerTx {
    fn new(sn_resolution: ZInt, initial_sn: ZInt) -> ChannelInnerTx {
        ChannelInnerTx {
            sn: SeqNumTx::new(sn_resolution, initial_sn)
        } 
    }
}

// Store the mutable data that need to be used for transmission
#[derive(Clone)]
struct ChannelLinks {
    batchsize: usize,
    links: Vec<Link>
}

impl ChannelLinks {
    fn new(batchsize: usize) -> ChannelLinks {
        ChannelLinks {
            batchsize,
            links: Vec::new()
        }
    }

    /*************************************/
    /*               LINK                */
    /*************************************/
    #[inline]
    fn find_link_index(&self, link: &Link) -> Option<usize> {
        self.links.iter().position(|x| x == link)
    }

    pub(super) async fn add_link(&mut self, link: Link) -> ZResult<()> {
        // Check if this link is not already present
        if self.links.contains(&link) {
            return Err(zerror!(ZErrorKind::InvalidLink {
                descr: "Trying to add a link that already exists!".to_string()
            }));
        }

        // Add the link to the channel
        self.links.push(link);

        Ok(())
    }

    pub(super) async fn del_link(&mut self, link: &Link) -> ZResult<()> {
        // Find the index of the link
        let mut index = self.find_link_index(&link);

        // Return error if the link was not found
        if index.is_none() {
            return Err(zerror!(ZErrorKind::InvalidLink {
                descr: format!("{}", link)
            }));
        }

        // Remove the link from the channel
        let index = index.take().unwrap();
        self.links.remove(index);

        Ok(())
    }
}


/*************************************/
/*     CHANNEL INNER RX STRUCT       */
/*************************************/

// Structs to manage the sequence numbers of channels
struct SeqNumRx {
    reliable: SeqNum,
    best_effort: SeqNum,
}

impl SeqNumRx {
    fn new(sn_resolution: ZInt, initial_sn: ZInt) -> SeqNumRx {
        // Set the sequence number in the state as it had 
        // received a message with initial_sn - 1
        let initial_sn = if initial_sn == 0 {
            sn_resolution
        } else {
            initial_sn - 1
        };
        SeqNumRx {
            reliable: SeqNum::make(initial_sn, sn_resolution).unwrap(),
            best_effort: SeqNum::make(initial_sn, sn_resolution).unwrap(),
        }
    }
}

// Store the mutable data that need to be used for transmission
struct ChannelInnerRx {    
    lease: ZInt,
    sn: SeqNumRx,
    callback: Option<Arc<dyn MsgHandler + Send + Sync>>
}

impl ChannelInnerRx {
    fn new(
        lease: ZInt,
        sn_resolution: ZInt,
        initial_sn: ZInt
    ) -> ChannelInnerRx {
        // @TODO: Randomly initialize the SN generator
        ChannelInnerRx {
            lease,
            sn: SeqNumRx::new(sn_resolution, initial_sn),
            callback: None
        }
    }
}


/*************************************/
/*           CHANNEL STRUCT          */
/*************************************/

pub(super) struct Channel {
    // The manager this channel is associated to
    manager: Arc<SessionManagerInner>,
    // The remote peer id
    pid: PeerId,
    // The session lease in seconds
    lease: ZInt,
    // The SN resolution 
    sn_resolution: ZInt,
    // Keep track whether the consume task is active
    active: AtomicBool,
    // The callback has been set or not
    has_callback: AtomicBool,
    // The message queue
    queue: CreditQueue<MessageTx>,
    // The links associated to the channel
    links: RwLock<ChannelLinks>,
    // The mutable data struct for transmission
    tx: Mutex<ChannelInnerTx>,
    // The mutable data struct for reception
    rx: Mutex<ChannelInnerRx>,
    // Barrier for syncrhonizing the stop() with the consume_task
    barrier: Arc<Barrier>,
    // Weak reference to self
    w_self: RwLock<Option<Weak<Self>>>
}

impl Channel {
    pub(super) fn new(
        manager: Arc<SessionManagerInner>,
        pid: PeerId, 
        whatami: WhatAmI,
        lease: ZInt,
        sn_resolution: ZInt, 
        initial_sn_tx: ZInt,
        initial_sn_rx: ZInt,
        batchsize: usize        
    ) -> Channel {
        // The buffer to send the Control messages. High priority
        let ctrl = CreditBuffer::<MessageTx>::new(
            *QUEUE_SIZE_CTRL,
            *QUEUE_CRED_CTRL,
            CreditBuffer::<MessageTx>::spending_policy(|_msg| 0isize),
        );
        // The buffer to send the retransmission of messages. Medium priority
        let retx = CreditBuffer::<MessageTx>::new(
            *QUEUE_SIZE_RETX,
            *QUEUE_CRED_RETX,
            CreditBuffer::<MessageTx>::spending_policy(|_msg| 0isize),
        );
        // The buffer to send the Data messages. Low priority
        let data = CreditBuffer::<MessageTx>::new(
            *QUEUE_SIZE_DATA,
            *QUEUE_CRED_DATA,
            // @TODO: Once the reliability queue is implemented, update the spending policy
            CreditBuffer::<MessageTx>::spending_policy(|_msg| 0isize),
        );
        // Build the vector of buffer for the transmission queue.
        // A lower index in the vector means higher priority in the queue.
        // The buffer with index 0 has the highest priority.
        let queue_tx = vec![ctrl, retx, data];

        Channel{
            manager,
            pid,
            lease,
            sn_resolution,
            has_callback: AtomicBool::new(false),
            queue: CreditQueue::new(queue_tx, *QUEUE_CONCURRENCY),
            active: AtomicBool::new(false),
            links: RwLock::new(ChannelLinks::new(batchsize)),
            tx: Mutex::new(ChannelInnerTx::new(sn_resolution, initial_sn_tx)),
            rx: Mutex::new(ChannelInnerRx::new(lease, sn_resolution, initial_sn_rx)),
            barrier: Arc::new(Barrier::new(2)),
            w_self: RwLock::new(None)
        }
    }

    pub(super) fn initialize(&self, w_self: Weak<Self>) {
        *self.w_self.try_write().unwrap() = Some(w_self);
    }

    /*************************************/
    /*            ACCESSORS              */
    /*************************************/
    pub(super) fn get_peer(&self) -> PeerId {
        self.pid.clone()
    }

    pub(super) fn get_lease(&self) -> ZInt {
        self.lease
    }

    pub(super) fn get_sn_resolution(&self) -> ZInt {
        self.sn_resolution
    }

    pub(super) fn has_callback(&self) -> bool {
        self.has_callback.load(Ordering::Relaxed)
    }

    pub(super) async fn set_callback(&self, callback: Arc<dyn MsgHandler + Send + Sync>) {        
        let mut guard = zasynclock!(self.rx);
        self.has_callback.store(true, Ordering::Relaxed);
        guard.callback = Some(callback);
    }

    pub(super) async fn close(&self) -> ZResult<()> {
        // Mark the channel as inactive
        if self.active.swap(false, Ordering::Relaxed) {
            let peer_id = Some(self.manager.config.pid.clone());
            let reason_id = smsg::close_reason::GENERIC;              
            let link_only = false;  // This is should always be false for user-triggered close              
            let attachment = None;  // No attachment here
            let message = SessionMessage::make_close(peer_id, reason_id, link_only, attachment);

            let close = MessageTx {
                inner: MessageInner::Session(message),
                link: None
            };
            let stop = MessageTx {
                inner: MessageInner::Stop,
                link: None
            };
            // Atomically push the close and stop messages to the queue
            self.queue.push_batch(vec![close, stop], *QUEUE_PRIO_DATA).await;

            // Wait for the consume_task to stop
            self.barrier.wait().await;

            // Delete the session on the manager
            self.manager.del_session(&self.pid).await?;
        }
        
        Ok(())
    }

    /*************************************/
    /*        SCHEDULE AND SEND TX       */
    /*************************************/
    // Schedule the message to be sent asynchronsly
    pub(super) async fn schedule(&self, message: ZenohMessage, link: Option<Link>) {
        let message = MessageTx {
            inner: MessageInner::Zenoh(message),
            link
        };
        // Wait for the queue to have space for the message
        self.queue.push(message, *QUEUE_PRIO_DATA).await;
    }

    // Schedule a batch of messages to be sent asynchronsly
    pub(super) async fn schedule_batch(&self, mut messages: Vec<ZenohMessage>, link: Option<Link>) {
        let messages = messages.drain(..).map(|x| {
            MessageTx {
                inner: MessageInner::Zenoh(x),
                link: link.clone(),
            }
        }).collect();
        // Wait for the queue to have space for the message
        self.queue.push_batch(messages, *QUEUE_PRIO_DATA).await;
    }

    /*************************************/
    /*               LINK                */
    /*************************************/
    pub(super) async fn add_link(&self, link: Link) -> ZResult<()> {
        self.stop().await?;
        zasyncwrite!(self.links).add_link(link).await?;
        self.start().await?;
        Ok(())
    }

    pub(super) async fn del_link(&self, link: &Link) -> ZResult<()> {
        self.stop().await?;
        let mut guard = zasyncwrite!(self.links);
        guard.del_link(link).await?;
        // Start the channel only if there are links left
        if !guard.links.is_empty() {
            self.start().await?;
        } else {
            // @TODO: Remove the else statement once the lease is implemented
            self.manager.del_session(&self.pid).await?;
        }
        Ok(())    
    }

    pub(super) async fn get_links(&self) -> Vec<Link> {
        let guard = zasyncread!(self.links);
        guard.links.to_vec()
    }

    /*************************************/
    /*               TASK                */
    /*************************************/
    pub(super) async fn start(&self) -> ZResult<()> {
        // Get the Arc to the channel
        let ch = if let Some(ch) = zasyncread!(self.w_self).as_ref() {
            if let Some(ch) = ch.upgrade() {
                ch
            } else {
                return Err(zerror!(ZErrorKind::Other{
                    descr: "The channel does not longer exist".to_string()
                }))
            }
        } else {
            panic!("Channel is uninitialized");
        };

        // If not already active, start the transmission loop
        if !self.active.swap(true, Ordering::Relaxed) {
            // Spawn the transmission loop
            task::spawn(consume_task(ch.clone()));
        }

        Ok(())
    }

    pub(super) async fn stop(&self) -> ZResult<()> {        
        if self.active.swap(false, Ordering::Relaxed) {
            let msg = MessageTx {
                inner: MessageInner::Stop,
                link: None
            };
            self.queue.push(msg, *QUEUE_PRIO_CTRL).await;
            self.barrier.wait().await;
        }

        Ok(())
    }


    /*************************************/
    /*   MESSAGE RECEIVED FROM THE LINK  */
    /*************************************/
    async fn process_reliable_frame(&self, sn: ZInt, payload: FramePayload) -> Action {
        // @TODO: implement the reordering and wait for missing messages
        let mut guard = zasynclock!(self.rx);        
        if !(guard.sn.reliable.precedes(sn) && guard.sn.reliable.set(sn).is_ok()) {
            println!("!!! Reliable frame with invalid SN dropped");
            return Action::Read
        }

        let callback = if let Some(callback) = &guard.callback {
            callback
        } else {
            println!("!!! Frame dropped because callback is unitialized");
            return Action::Read
        };

        match payload {
            FramePayload::Fragment { .. } => {
                unimplemented!("!!! Fragmentation not implemented");
            },
            FramePayload::Messages { mut messages } => {
                // Pass all the messages concurrently to the callback
                let futs: FuturesUnordered<_> = messages.drain(..).map(|msg|
                    callback.handle_message(msg)
                ).collect();
                let _ = futs.into_future().await;                
            }
        }
        
        Action::Read
    }

    async fn process_best_effort_frame(&self, sn: ZInt, payload: FramePayload) -> Action {
        let mut guard = zasynclock!(self.rx);        
        if !(guard.sn.best_effort.precedes(sn) && guard.sn.best_effort.set(sn).is_ok()) {
            println!("!!! Best effort frame with invalid SN dropped");
            return Action::Read
        }

        let callback = if let Some(callback) = &guard.callback {
            callback
        } else {
            println!("!!! Frame dropped because callback is unitialized");
            return Action::Read
        };

        match payload {
            FramePayload::Fragment { .. } => {
                unimplemented!("!!! Fragmentation not implemented");
            },
            FramePayload::Messages { mut messages } => {
                // Pass all the messages concurrently to the callback
                let futs: FuturesUnordered<_> = messages.drain(..).map(|msg|
                    callback.handle_message(msg)
                ).collect();
                let _ = futs.into_future().await;                
            }
        }
        
        Action::Read
    }

    async fn process_close(&self, link: &Link, pid: Option<PeerId>, reason: u8, link_only: bool) -> Action {
        // Check if the PID is correct when provided
        if let Some(pid) = pid {
            if pid != self.pid {
                let s = format!("!!! Received a Close message from a wrong peer ({:?}) with reason ({}). Ignoring.", pid, reason);
                println!("{}", s);
                return Action::Read
            }
        }
        // Delete the link
        let _ = self.del_link(link).await;
        // Close all the session if this close message is not for the link only
        if !link_only {
            let _ = self.manager.del_session(&self.pid).await;
        }
        
        Action::Close
    }
}

#[async_trait]
impl TransportTrait for Channel {
    async fn receive_message(&self, link: &Link, message: SessionMessage) -> Action {
        match message.body {
            SessionBody::AckNack { .. } => {
                unimplemented!("Handling of AckNack Messages not yet implemented!");
            },
            SessionBody::Close { pid, reason, link_only } => {
                self.process_close(link, pid, reason, link_only).await
            },
            SessionBody::Hello { .. } => {
                unimplemented!("Handling of Hello Messages not yet implemented!");
            },
            SessionBody::KeepAlive { .. } => {
                unimplemented!("Handling of KeepAlive Messages not yet implemented!");
            },            
            SessionBody::Ping { .. } => {
                unimplemented!("Handling of Ping Messages not yet implemented!");
            }
            SessionBody::Pong { .. } => {
                unimplemented!("Handling of Pong Messages not yet implemented!");
            }
            SessionBody::Scout { .. } => {
                unimplemented!("Handling of Scout Messages not yet implemented!");
            }
            SessionBody::Sync { .. } => {
                unimplemented!("Handling of Sync Messages not yet implemented!");
            }
            SessionBody::Frame { ch, sn, payload } => {
                match ch {
                    true => self.process_reliable_frame(sn, payload).await,
                    false => self.process_best_effort_frame(sn, payload).await
                }
            }
            _ => {
                // unimplemented!("Handling of Invalid Messages not yet implemented!");
                Action::Close
            }
        }        
    }

    async fn link_err(&self, link: &Link) {
        let _ = self.del_link(link).await;
    }
}
