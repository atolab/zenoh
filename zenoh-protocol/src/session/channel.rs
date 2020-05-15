use async_std::prelude::*;
use async_std::sync::{channel, Arc, Mutex, Receiver, Sender};
use async_std::task;
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::core::{PeerId, ZError, ZErrorKind, ZInt, ZResult};
use crate::io::{RBuf, WBuf};
use crate::link::Link;
use crate::proto::{SessionBody, SessionMessage, SeqNum, SeqNumGenerator, WhatAmI, ZenohMessage};
use crate::session::{Action, MsgHandler, SessionManagerInner, TransportTrait};
use crate::session::defaults::{
    // Control buffer
    QUEUE_SIZE_CTRL,
    QUEUE_CRED_CTRL,
    // Retransmission buffer
    QUEUE_SIZE_RETX,
    QUEUE_CRED_RETX,
    // Data buffer
    QUEUE_SIZE_DATA,
    QUEUE_CRED_DATA,
    // Queue size
    QUEUE_SIZE_TOT,
    QUEUE_CONCURRENCY,
    // Default slice size when serializing a message
    WRITE_MSG_SLICE_SIZE
};
use crate::zerror;
use zenoh_util::collections::{CreditBuffer, CreditQueue};
use zenoh_util::collections::credit_queue::Drain as CreditQueueDrain;
use zenoh_util::zasynclock;

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

pub(super) enum MessageInner {
    SessionMessage,
    ZenohMessage
}

/*************************************/
/*           CHANNEL TASK            */
/*************************************/

struct LinkContext {
    // The list of messages to transmit
    messages: Vec<MessageInner>,
    // The buffer to perform the batching on
    batch: WBuf,
    // The buffer to serialize each message on
    buffer: WBuf
}

impl LinkContext {
    fn new(batchsize: usize) -> LinkContext {
        LinkContext {
            messages: Vec::with_capacity(*QUEUE_SIZE_TOT),
            batch: WBuf::new(batchsize, true),
            buffer: WBuf::new(*WRITE_MSG_SLICE_SIZE, true)
        }
    }
}

// Update the sequence number
// #[inline]
// async fn update_sn(sn_gen: &mut SeqNumTx, message: &mut SessionMessage) {
    // let is_reliable = message.is_reliable();
    // match message.body {
    //     Body::Data { ref mut sn, .. }
    //     | Body::Unit { ref mut sn, .. }
    //     | Body::Declare { ref mut sn, .. }
    //     | Body::Pull { ref mut sn, .. }
    //     | Body::Query { ref mut sn, .. } => {
    //         // Update the sequence number
    //         *sn = if is_reliable {
    //             sn_gen.reliable.get()
    //         } else {
    //             sn_gen.unreliable.get()
    //         };
    //     }
    //     _ => {}
    // }
// }

// Mapping
// async fn map_messages_on_links(
//     inner: &mut ChannelInnerTx,
//     drain: &mut CreditQueueDrain<'_, MessageInner>,
//     context: &mut Vec<LinkContext>
// ) {
    // if let Some(main_idx) = inner.main_idx {
    //     // If there is a main link, it means that there is at least one link
    //     // associated to the conduit. Drain the messages and perform the mapping
    //     for mut msg in drain {
    //         // Update the sequence number while draining
    //         update_sn(&mut inner.sn, &mut msg.inner).await;
    
    //         // @TODO: implement the reliability queue
    
    //         // Find the right index for the link
    //         let index = if let Some(link) = &msg.link {
    //             // Check if the target link exists, otherwise fallback on the main link
    //             if let Some(index) = inner.find_link_index(&link) {
    //                 index
    //             } else {
    //                 if let Some(notify) = &msg.notify {
    //                     // Notify now the result 
    //                     let res = Err(zerror!(ZErrorKind::InvalidLink {
    //                         descr: format!("Can not send message on unexsiting link ({})!", link)
    //                     }));
    //                     notify.send(res).await;
    //                 }
    //                 // Drop this message, continue with the following one
    //                 continue;
    //             } 
    //         } else {
    //             main_idx
    //         };
    //         // Add the message to the right link
    //         context[index].messages.push(msg);
    //     }
    // } else {
    //     // There are no links associated to the conduits. Drain the messages, keep the 
    //     // reliable messages and drop the unreliable ones
    //     for mut msg in drain {
    //         if msg.inner.is_reliable() {
    //             // Update the sequence number while draining
    //             update_sn(&mut inner.sn, &mut msg.inner).await;
    //             // @TODO: implement the reliability queue
    //         }
    //     }
    // }
// }

// async fn batch_fragement_transmit(link: &Link, context: &mut LinkContext, batchsize: usize) -> ZResult<()> {
    // let batchsize = batchsize.min(link.get_mtu());
    // let mut batchlen = 0;
    // // Process all the messages just drained
    // for msg in context.messages.drain(..) {        
    //     // Clear the message buffer
    //     context.buffer.clear();
    //     // Serialize the message on the buffer
    //     context.buffer.write_message(&msg.inner);

    //     // Create the RBuf out of batch and buff WBuff for transmission
    //     let buff_read = context.buffer.as_rbuf();

    //     if let Some(notify) = msg.notify {
    //         // Transmit the current batch if present
    //         if batchlen > 0 { 
    //             let batch_read = context.batch.as_rbuf();
    //             if let Err(e) = link.send(batch_read).await {
    //                 // Clear the batch buffer
    //                 context.batch.clear();
    //                 return Err(e)
    //             }
    //             // Clear the batch buffer
    //             context.batch.clear();
    //             // Reset the batchlen
    //             batchlen = 0;
    //         }
    //         // Transmit now the message
    //         let res = link.send(buff_read).await;
    //         // Notify now the result 
    //         notify.send(res).await;
    //         // We are done with this message, continue with the following one
    //         continue
    //     } else if batchlen + buff_read.len() > batchsize {
    //         // The message does not fit in the batch, first transmit the current batch
    //         let batch_read = context.batch.as_rbuf();
    //         if let Err(e) = link.send(batch_read).await {
    //             // Clear the batch buffer
    //             context.batch.clear();
    //             return Err(e)
    //         }
    //         // Clear the batch buffer
    //         context.batch.clear();
    //         // Reset the batchlen
    //         batchlen = 0;
    //     }

    //     // Add the message to the batch
    //     let slices = buff_read.get_slices();
    //     for s in slices.iter() {
    //         context.batch.add_slice(s.clone());
    //     }
    //     batchlen += buff_read.len();
    // }

//     Ok(())
// }

// async fn flush_batch(link: &Link, context: &mut LinkContext) -> ZResult<()> {
    // let batch_read = RBuf::from(&context.batch);
    // if !batch_read.is_empty() {
    //     // Transmit the batch on the link
    //     if let Err(e) = link.send(&context.batch).await {
    //         // Clear the batch buffer
    //         context.batch.clear();
    //         return Err(e)
    //     }
    // }
    // // Clear the batch buffer
    // context.batch.clear();

//     Ok(())
// }

// Consuming function
// async fn consume_loop(conduit: &Arc<Channel>) -> Option<bool> {
    // @TODO: Implement the reliability queue
    // @TODO: Implement the fragmentation

    // Create a buffer for the batching
    // let mut context: Vec<LinkContext> = Vec::new();
    // while conduit.is_active() {
    //     // Get a Drain iterator for the queue
    //     // drain() waits for the queue to be non-empty
    //     let mut drain = conduit.queue.drain().await;

    //     // Acquire the lock on the inner conduit data structure
    //     let mut inner = zasynclock!(conduit.tx);
    //     // Create or remove link context if needed
    //     if context.len() != inner.links.len() {
    //         context.resize_with(inner.links.len(), || LinkContext::new(inner.batchsize));
    //     }

    //     // Keep mapping, batching, fragmenting and transmitting until there are
    //     // messages in the queue
    //     while conduit.is_active() {  
    //         // Map the messages on the links. This operation drains messages from the Drain iterator
    //         map_messages_on_links(&mut inner, &mut drain, &mut context).await;
    //         // The drop() on Drain object needs to be manually called since an async
    //         // destructor is not yet supported in Rust. More information available at:
    //         // https://internals.rust-lang.org/t/asynchronous-destructors/11127/47 
    //         drain.drop().await;
    //         // Batch/Fragmenet and transmit the messages
    //         for (i, mut c) in context.iter_mut().enumerate() {
    //             let res = batch_fragement_transmit(&inner.links[i], &mut c, inner.batchsize).await;
    //             if res.is_err() {
    //                 // There was an error while transmitting. Deschedule the task to allow other tasks 
    //                 // to be scheduled and eventually break this loop because the task needs to be stopeed
    //                 task::yield_now().await;
    //                 break
    //             }
    //         }
    //         // Try to drain messages from the queue
    //         // try_drain does not wait for the queue to be non-empty
    //         drain = conduit.queue.try_drain().await;
    //         // Check if we can drain from the Drain iterator
    //         let (min, _) = drain.size_hint();
    //         if min == 0 {
    //             // The drop() on Drain object needs to be manually called since an async
    //             // destructor is not yet supported in Rust. More information available at:
    //             // https://internals.rust-lang.org/t/asynchronous-destructors/11127/47
    //             drain.drop().await;
    //             break
    //         }
    //     }

    //     // Transmit all the messages left in the batch if any
    //     for (i, mut c) in context.iter_mut().enumerate() {
    //         let _ = flush_batch(&inner.links[i], &mut c).await;
    //     }

    //     // Deschedule the task to allow other tasks to be scheduled and eventually push on the queue
    //     task::yield_now().await;
    // }

//     None
// }

async fn consume_task(conduit: Arc<Channel>, receiver: Receiver<bool>) {
    // // Create the consume future
    // let consume = consume_loop(&conduit);
    // // Create the signal future
    // let signal = receiver.recv();
    // // Wait for the stop signal
    // let _ = consume.race(signal).await;
}

/*************************************/
/*      CHANNEL INNER TX STRUCT      */
/*************************************/

// Structs to manage the sequence numbers of channels
struct SeqNumTx {
    reliable: SeqNumGenerator,
    unreliable: SeqNumGenerator,
}

impl SeqNumTx {
    fn new(sn_resolution: ZInt, initial_sn: ZInt) -> SeqNumTx {
        SeqNumTx {
            reliable: SeqNumGenerator::make(initial_sn, sn_resolution).unwrap(),
            unreliable: SeqNumGenerator::make(initial_sn, sn_resolution).unwrap(),
        }
    }
}

// Store the mutable data that need to be used for transmission
struct ChannelInnerTx {
    sn: SeqNumTx,
    batchsize: usize,
    links: Vec<Link>,
    main_idx: Option<usize>
}

impl ChannelInnerTx {
    fn new(sn_resolution: ZInt, initial_sn: ZInt, batchsize: usize) -> ChannelInnerTx {
        // @TODO: Randomly initialize the SN generator
        let zero: ZInt = 0;

        ChannelInnerTx {
            sn: SeqNumTx::new(sn_resolution, initial_sn),
            batchsize,
            links: Vec::new(),
            main_idx: None
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

        // Add the link to the conduit
        self.links.push(link);
        // Select the main link for this conduit
        if self.main_idx.is_none() {
            // @TODO: Adopt a more intelligent link selection
            //        Selecting the first link on the list
            self.main_idx = Some(0);
        }

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

        // Remove the link
        let index = index.take().unwrap();
        self.links.remove(index);

        // Reset the main link if no links left
        if self.links.is_empty() {
            self.main_idx = None;
        }

        Ok(())
    }
}


/*************************************/
/*     CHANNEL INNER RX STRUCT       */
/*************************************/

// Structs to manage the sequence numbers of channels
struct SeqNumRx {
    reliable: SeqNum,
    unreliable: SeqNum,
}

impl SeqNumRx {
    fn new(sn_resolution: ZInt, initial_sn: ZInt) -> SeqNumRx {
        SeqNumRx {
            reliable: SeqNum::make(initial_sn, sn_resolution).unwrap(),
            unreliable: SeqNum::make(initial_sn, sn_resolution).unwrap(),
        }
    }
}

// Store the mutable data that need to be used for transmission
struct ChannelInnerRx {    
    manager: Arc<SessionManagerInner>,
    lease: ZInt,
    sn: SeqNumRx,
    callback: Option<Arc<dyn MsgHandler + Send + Sync>>
}

impl ChannelInnerRx {
    fn new(
        manager: Arc<SessionManagerInner>,
        lease: ZInt,
        sn_resolution: ZInt,
        initial_sn: ZInt
    ) -> ChannelInnerRx {
        // @TODO: Randomly initialize the SN generator
        ChannelInnerRx {
            manager,
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
    pid: PeerId,
    lease: ZInt,
    sn_resolution: ZInt,
    active: AtomicBool,
    // The callback has been set or not
    has_callback: AtomicBool,
    queue: CreditQueue<MessageInner>,
    tx: Mutex<ChannelInnerTx>,
    rx: Mutex<ChannelInnerRx>,
    signal: Mutex<Option<Sender<bool>>>
}

impl Channel {
    pub(super) fn new(
        pid: PeerId, 
        whatami: WhatAmI,
        lease: ZInt,
        sn_resolution: ZInt, 
        initial_sn_tx: ZInt,
        initial_sn_rx: ZInt,
        batchsize: usize, 
        manager: Arc<SessionManagerInner>
    ) -> Channel {
        // The buffer to send the Control messages. High priority
        let ctrl = CreditBuffer::<MessageInner>::new(
            *QUEUE_SIZE_CTRL,
            *QUEUE_CRED_CTRL,
            CreditBuffer::<MessageInner>::spending_policy(|_msg| 0isize),
        );
        // The buffer to send the retransmission of messages. Medium priority
        let retx = CreditBuffer::<MessageInner>::new(
            *QUEUE_SIZE_RETX,
            *QUEUE_CRED_RETX,
            CreditBuffer::<MessageInner>::spending_policy(|_msg| 0isize),
        );
        // The buffer to send the Data messages. Low priority
        let data = CreditBuffer::<MessageInner>::new(
            *QUEUE_SIZE_DATA,
            *QUEUE_CRED_DATA,
            // @TODO: Once the reliability queue is implemented, update the spending policy
            CreditBuffer::<MessageInner>::spending_policy(|_msg| 0isize),
        );
        // Build the vector of buffer for the transmission queue.
        // A lower index in the vector means higher priority in the queue.
        // The buffer with index 0 has the highest priority.
        let queue_tx = vec![ctrl, retx, data];

        Channel{
            pid,
            lease,
            sn_resolution,
            has_callback: AtomicBool::new(false),
            queue: CreditQueue::new(queue_tx, *QUEUE_CONCURRENCY),
            active: AtomicBool::new(false),
            tx: Mutex::new(ChannelInnerTx::new(sn_resolution, initial_sn_tx, batchsize)),
            rx: Mutex::new(ChannelInnerRx::new(manager, lease, sn_resolution, initial_sn_rx)),
            signal: Mutex::new(None)
        }
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
        Ok(())
    }

    /*************************************/
    /*        SCHEDULE AND SEND TX       */
    /*************************************/
    // Schedule the message to be sent asynchronsly
    pub(super) async fn schedule(&self, message: ZenohMessage,link: Option<Link>) {
        // let message = MessageTx {
        //     inner: message,
        //     notify: None,
        //     link
        // };
        // // Wait for the queue to have space for the message
        // self.push_on_conduit_queue(message, priority).await;
    }

    // Schedule a batch of messages to be sent asynchronsly
    pub(super) async fn schedule_batch(&self, messages: Vec<ZenohMessage>, link: Option<Link>) {
        // let cid: ZInt = cid.unwrap_or(0);
        // let messages = messages.drain(..).map(|mut x| {
        //     x.cid = cid;
        //     MessageTx {
        //         inner: x,
        //         link: link.clone(),
        //         notify: None
        //     }
        // }).collect();
        // // Wait for the queue to have space for the message
        // self.push_batch_on_conduit_queue(messages, priority, cid).await;
    }

    async fn push_on_conduit_queue(&self, message: MessageInner, priority: usize) {
        // Push the message on the conduit queue
        // If the conduit does not exist, create it on demand
        // let guard = zasyncread!(self.conduits);
        // if let Some(conduit) = guard.get(&message.inner.cid) {
        //     conduit.queue.push(message, priority).await;
        // } else {
        //     // Drop the guard to dynamically add the new conduit
        //     drop(guard);
        //     // Add the new conduit
        //     if let Ok(conduit) = self.new_conduit(message.inner.cid).await {
        //         conduit.queue.push(message, priority).await
        //     }
        // }
    }

    async fn push_batch_on_conduit_queue(&self, messages: Vec<MessageInner>, priority: usize, cid: ZInt) {
        // Push the message on the conduit queue
        // If the conduit does not exist, create it on demand
        // let guard = zasyncread!(self.conduits);
        // if let Some(conduit) = guard.get(&cid) {
        //     conduit.queue.push_batch(messages, priority).await;
        // } else {
        //     // Drop the guard to dynamically add the new conduit
        //     drop(guard);
        //     // Add the new conduit
        //     if let Ok(conduit) = self.new_conduit(cid).await {
        //         conduit.queue.push_batch(messages, priority).await;
        //     }
        // }
    }

    /*************************************/
    /*               LINK                */
    /*************************************/
    pub(super) async fn add_link(&self, link: Link) -> ZResult<()> {
        zasynclock!(self.tx).add_link(link).await
    }

    pub(super) async fn del_link(&self, link: &Link) -> ZResult<()> {
        let mut guard = zasynclock!(self.tx);
        let res = guard.del_link(link).await;
        // Stop the conduit if it has no links left
        if guard.links.is_empty() {
            self.stop().await;
        }
        res
    }

    pub(super) async fn get_links(&self) -> Vec<Link> {
        let guard = zasynclock!(self.tx);
        guard.links.to_vec()
    }

    /*************************************/
    /*               TASK                */
    /*************************************/
    #[inline]
    pub(super) fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    pub(super) async fn start(conduit: &Arc<Self>) {
        // If not already active, start the transmission loop
        if !conduit.active.swap(true, Ordering::Relaxed) {
            // Create the signal channel
            let (sender, receiver) = channel::<bool>(1);
            // Store the sender
            let mut guard = zasynclock!(conduit.signal);
            // Store the sender needed to stop the transmission loop
            *guard = Some(sender);
            // Drop the guard that borrows the conduit
            drop(guard);
            // Spawn the transmission loop
            task::spawn(consume_task(conduit.clone(), receiver));
        }
    }

    pub(super) async fn stop(&self) {
        if self.active.swap(false, Ordering::Relaxed) {
            // Take the sender for the signal channel
            let signal = zasynclock!(self.signal).take();
            // If the transmission loop is active, send the stop signal
            if let Some(sender) = signal {
                // Send the stop command in case the transmission loop
                // was waiting for messages
                sender.send(true).await;
            }
        }
    }


    /*************************************/
    /*   MESSAGE RECEIVED FROM THE LINK  */
    /*************************************/
    async fn process_reliable_frame(&self, messages: Vec<ZenohMessage>, sn: ZInt) -> Action {
        // @TODO: implement the reordering and wait for missing messages
        // let mut guard = zasynclock!(self.rx);
        // if guard.callback.is_some() {
        //     // Messages with invalid SN are automatically dropped
        //     if guard.sn.reliable.precedes(sn) && guard.sn.reliable.set(sn).is_ok() {
        //         let _ = guard.callback.as_ref().unwrap().handle_message(message).await;
        //     } else {
        //         println!("!!! Message with invalid SN dropped: {:?}", message.body);
        //     }
        // } else {
        //     println!("!!! Message dropped because callback is unitialized: {:?}", message.body);
        // }
        Action::Read
    }

    async fn process_unreliable_frame(&self, messages: Vec<ZenohMessage>, sn: ZInt) -> Action {
        // let mut guard = zasynclock!(self.rx);
        // if guard.callback.is_some() {
        //     // Messages with invalid SN are automatically dropped
        //     if guard.sn.unreliable.precedes(sn) && guard.sn.unreliable.set(sn).is_ok() {
        //         let _ = guard.callback.as_ref().unwrap().handle_message(message).await;
        //     } else {
        //         println!("!!! Message with invalid SN dropped: {:?}", message.body);
        //     }
        // } else {
        //     println!("!!! Message dropped because callback is unitialized: {:?}", message.body);
        // }
        Action::Read
    }
}

#[async_trait]
impl TransportTrait for Channel {
    async fn receive_message(&self, link: &Link, message: SessionMessage) -> Action {
        match message.get_body() {
            SessionBody::AckNack { .. } => {
                unimplemented!("Handling of AckNack Messages not yet implemented!");
            },
            SessionBody::Close { pid, reason, link_only } => {
                println!("CHANNEL CLOSE");
                // let guard = zasynclock!(self.rx);
                // let session = zsession!(guard.session);
                // let c_reason = *reason;
                // session.process_close(link, pid, c_reason).await
                unimplemented!("Handling of Close Messages not yet implemented!");
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
            SessionBody::Frame { .. } => {
                unimplemented!("Handling of Frame Messages not yet implemented!");
                // let c_sn = *sn;
                // match reliable {
                //     true => self.process_reliable_message(message, c_sn).await,
                //     false => self.process_unreliable_message(message, c_sn).await,
                // }
            }
            _ => {
                // unimplemented!("Handling of Invalid Messages not yet implemented!");
                Action::Close
            }
        }        
    }

    async fn link_err(&self, link: &Link) {

    }
}
