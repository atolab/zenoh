use async_std::sync::{Arc, Barrier, Mutex, RwLock, Weak};
use async_std::task;
use async_trait::async_trait;
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
    QUEUE_SIZE_TOT,
    QUEUE_CONCURRENCY,
    // Default slice size when serializing a message that needs to be fragmented
    // WRITE_MSG_SLICE_SIZE
};
use crate::zerror;
use zenoh_util::collections::{CreditBuffer, CreditQueue};
use zenoh_util::collections::credit_queue::Drain as CreditQueueDrain;
use zenoh_util::{zasynclock, zasyncread, zasyncwrite};


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

struct SerializedBatch {
    // The buffer to perform the batching on
    buffer: WBuf,
    // The link this batch is associated to
    link: Link
}

impl SerializedBatch {
    fn new(link: Link, size: usize) -> SerializedBatch {
        let size = size.min(link.get_mtu());
        // Create the buffer
        let mut buffer = WBuf::new(size, true);
        // Reserve two bytes if the link is streamed
        if link.is_streamed() {
            buffer.write_bytes(&TWO_BYTES);
        }

        SerializedBatch {
            buffer,
            link
        }
    }

    fn clear(&mut self) {
        self.buffer.clear();        
        if self.link.is_streamed() {
            self.buffer.write_bytes(&TWO_BYTES);
        }
    }

    async fn transmit(&mut self) -> ZResult<()> {
        let mut length: u16 = self.buffer.len() as u16;
        if self.link.is_streamed() {
            // Remove from the total the 16 bits used for the length
            length -= 2;
            // Write the length on the first 16 bits
            let bits = self.buffer.get_first_slice_mut(..2);
            bits.copy_from_slice(&length.to_le_bytes());
        }

        if length > 0 {
            let res = self.link.send(self.buffer.get_first_slice(..)).await;
            self.clear();            
            return res
        }

        Ok(())
    }
}

fn map_messages_on_links(    
    drain: &mut CreditQueueDrain<'_, MessageTx>,
    batches: &[SerializedBatch],
    messages: &mut Vec<Vec<MessageInner>>
) {    
    // Drain all the messages from the queue and map them on the links
    for msg in drain {      
        // Find the right index for the message
        let index = if let Some(link) = &msg.link {
            // Check if the target link exists, otherwise fallback on the main link            
            if let Some(index) = batches.iter().position(|x| &x.link == link) {
                index
            } else {
                // Drop the message            
                continue
            } 
        } else {
            DEFAULT_LINK_INDEX
        };
        // Add the message to the right link
        messages[index].push(msg.inner);
    }
}

enum CurrentFrame {
    Reliable,
    BestEffort,
    None
}

async fn batch_fragment_transmit(
    inner: &mut ChannelInnerTx,
    messages: &mut Vec<MessageInner>,
    batch: &mut SerializedBatch
) -> bool {  
    let mut current_frame = CurrentFrame::None;
    
    for msg in messages.drain(..) {
        let mut has_failed = false;
        let mut current_sn = None;
        let mut is_first = true;

        loop {
            // Mark the write operation       
            batch.buffer.mark();
            // Try to serialize the message on the current batch
            let res = match &msg {
                MessageInner::Zenoh(m) => {
                    // The message is reliable or not
                    let reliable = m.is_reliable();
                    // Eventually update the current frame and sn based on the current status
                    match current_frame {
                        CurrentFrame::Reliable => {
                            if !reliable {
                                // A new best-effort frame needs to be started
                                current_frame = CurrentFrame::BestEffort;
                                current_sn = Some(inner.sn.best_effort.get());
                                is_first = true;
                            }
                        }, 
                        CurrentFrame::BestEffort => {
                            if reliable {    
                                // A new reliable frame needs to be started
                                current_frame = CurrentFrame::Reliable;
                                current_sn = Some(inner.sn.reliable.get());
                                is_first = true;
                            }
                        },
                        CurrentFrame::None => {
                            if !has_failed || !is_first {
                                if reliable {
                                    // A new reliable frame needs to be started
                                    current_frame = CurrentFrame::Reliable;
                                    current_sn = Some(inner.sn.reliable.get());
                                } else {
                                    // A new best-effort frame needs to be started
                                    current_frame = CurrentFrame::BestEffort;
                                    current_sn = Some(inner.sn.best_effort.get());
                                }
                                is_first = true;
                            }
                        }
                    }

                    // If a new sequence number has been provided, it means we are in the case we need 
                    // to start a new frame. Write a new frame header.
                    if let Some(sn) = current_sn {                        
                        // Serialize the new frame and the zenoh message
                        batch.buffer.write_frame_header(reliable, sn, None, None)
                        && batch.buffer.write_zenoh_message(&m)
                    } else {
                        is_first = false;
                        batch.buffer.write_zenoh_message(&m)
                    }                    
                },
                MessageInner::Session(m) => {
                    current_frame = CurrentFrame::None;
                    batch.buffer.write_session_message(&m)
                },
                MessageInner::Stop => {
                    let _ = batch.transmit().await;
                    return false
                }
            };

            // We have been succesfull in writing on the current batch, exit the loop
            if res {
                break
            }

            // An error occured, revert the batch buffer
            batch.buffer.revert();
            // Reset the current frame but not the sn which is carried over the next iteration
            current_frame = CurrentFrame::None; 

            // This is the second time that the serialization fails, we should fragment
            if has_failed {
                // @TODO: implement the fragmentation here
                // Drop the message for the time being
                batch.clear();
                break
            }

            // Send the batch
            let _ = batch.transmit().await;

            // Mark that the serialization attempt has failed
            has_failed = true;
        }
    }

    true
}

// Consume function
async fn consume_task(ch: Arc<Channel>) {
    // @TODO: Implement the reliability queue
    // @TODO: Implement the fragmentation

    // Acquire the lock on the links
    let guard = zasyncread!(ch.links); 

    // Use double buffering to allow parallel serialization and transmission    
    let mut batches: Vec<SerializedBatch> = Vec::with_capacity(guard.links.len());
    let mut messages: Vec<Vec<MessageInner>> = Vec::with_capacity(guard.links.len());

    // Initialize the batches based on the current links parameters      
    for link in guard.links.iter() {
        batches.push(SerializedBatch::new(link.clone(), guard.batchsize));
        messages.push(Vec::with_capacity(*QUEUE_SIZE_TOT));
    }

    // Drop the mutex guard
    drop(guard);

    // Keep the lock on the inner transmission structure
    let mut inner = zasynclock!(ch.tx);

    // Control variable
    let mut active = true; 

    // let mut even = false;
    while active {                       
        // Get a Drain iterator for the queue
        // drain() waits for the queue to be non-empty
        // @TODO: add a timeout to the drain() future to trigger the 
        //        transmission of KEEP_ALIVE messages
        let mut drain = ch.queue.drain().await;
        
        // Try to always fill the batch
        while active {
            // Map the messages on the links. This operation drains messages from the Drain iterator
            // active = map_batch_fragment(&links, &mut inner, &mut drain, &mut batches, &mut context).await;
            map_messages_on_links(&mut drain, &batches, &mut messages);
            
            // The drop() on Drain object needs to be manually called since an async
            // destructor is not yet supported in Rust. More information available at:
            // https://internals.rust-lang.org/t/asynchronous-destructors/11127/47 
            drain.drop().await;
            
            // Concurrently send on all the selected links
            for (i, mut batch) in batches.iter_mut().enumerate() {
                // active = batch_fragment_transmit(link, &mut inner, &mut context[i]);
                // Check if the batch is ready to send      
                let res = batch_fragment_transmit(&mut inner, &mut messages[i], &mut batch).await;
                if !res {
                    // There was an error while transmitting. Exit.
                    active = false;
                    break
                }
            }

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
        }

        // Send any leftover on the batches
        for batch in batches.iter_mut() {
            // Check if the batch is ready to send      
            let _ = batch.transmit().await;        
        }

        // Deschedule the task to allow other tasks to be scheduled and eventually push on the queue
        task::yield_now().await;
    }

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
    _lease: ZInt,
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
            _lease: lease,
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
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        manager: Arc<SessionManagerInner>,
        pid: PeerId, 
        _whatami: WhatAmI,
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
            task::spawn(consume_task(ch));
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
                for msg in messages.drain(..) {
                    let _ = callback.handle_message(msg).await;
                }
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
                for msg in messages.drain(..) {
                    let _ = callback.handle_message(msg).await;
                }              
            }
        }
        
        Action::Read
    }

    async fn process_close(&self, link: &Link, pid: Option<PeerId>, reason: u8, link_only: bool) -> Action {
        // Check if the PID is correct when provided
        if let Some(pid) = pid {
            if pid != self.pid {
                println!("!!! Received a Close message from a wrong peer ({:?}) with reason ({}). Ignoring.", pid, reason);
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
