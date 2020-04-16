use async_std::prelude::*;
use async_std::sync::{channel, Arc, Mutex, Receiver, Sender};
use async_std::task;

use crate::core::{ZError, ZErrorKind, ZInt, ZResult};
use crate::io::{RBuf, WBuf};
use crate::link::{Link, Locator};
use crate::proto::{Body, Message, MessageKind, SeqNum, SeqNumGenerator};
use crate::session::{MessageTx, MsgHandler, SessionInner, Transport};
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
use zenoh_util::zasynclock;

/*************************************/
/*         CONDUIT TX TASK           */
/*************************************/

// Command to operate on the transmission loop
enum Command {
    Stop,
    Continue,
}

// Update the sequence number
async fn update_sn(sn_gen: &mut SeqNumTx, message: &mut Message) {
    let is_reliable = message.is_reliable();
    match message.body {
        Body::Data { ref mut sn, .. }
        | Body::Unit { ref mut sn, .. }
        | Body::Declare { ref mut sn, .. }
        | Body::Pull { ref mut sn, .. }
        | Body::Query { ref mut sn, .. } => {
            // Update the sequence number
            *sn = if is_reliable {
                sn_gen.reliable.get()
            } else {
                sn_gen.unreliable.get()
            };
        }
        _ => {}
    }
}

// Consuming function
async fn consume(
    conduit: &Arc<ConduitTx>,
    mut messages: &mut Vec<MessageTx>,
    batch: &mut WBuf,
    buff: &mut WBuf,
) -> Option<Command> {
    // @TODO: Implement the reliability queue
    // @TODO: Implement the fragmentation

    // Drain all the messages from the queue
    // drain_into() waits for the queue to be non-empty
    conduit.queue.drain_into(&mut messages).await;

    // Keep draining
    while !messages.is_empty() {
        // Process all the messages just drained
        for mut msg in messages.drain(..) {
            // Acquire the tx lock for updating the SN and transmission
            let mut inner = zasynclock!(conduit.inner);

            // Update the sequence number
            update_sn(&mut inner.sn, &mut msg.inner).await;

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
                conduit
                    .transmit(&inner.links, buff_read, msg.link, msg.notify)
                    .await;
                // We are done with this message, continue with the following one
                continue;
            } else if batch_read.len() + buff_read.len() > inner.batchsize {
                // The message does not fit in the batch, first transmit the current batch
                conduit.transmit(&inner.links, batch_read, None, None).await;
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
        conduit.queue.try_drain_into(&mut messages).await;
    }
    // The last try_drain_into() operation returned no messages
    // Transmit all the messages left in the batch
    let batch_read = batch.as_rbuf();
    if !batch_read.is_empty() {
        // Acquire the tx lock for transmission
        let tx = zasynclock!(conduit.inner);
        // Transmit the last chunk of the batch
        conduit
            .transmit(&tx.links, batch.as_rbuf(), None, None)
            .await;
        // Clear the batch buffer
        batch.clear();
    }

    Some(Command::Continue)
}

async fn transmission_loop(conduit: Arc<ConduitTx>, receiver: Receiver<Command>) {
    // The loop to consume the messages in the queue
    let mut messages: Vec<MessageTx> = Vec::with_capacity(QUEUE_SIZE_TOT);
    // Create a buffer for the batching
    let mut batch = WBuf::new(WRITE_MSG_SLICE_SIZE);
    // Create a buffer to serialize each message on
    let mut buff = WBuf::new(WRITE_MSG_SLICE_SIZE);
    loop {
        // Create the consume future
        let consume = consume(&conduit, &mut messages, &mut batch, &mut buff);
        // Create the signal future
        let signal = receiver.recv();

        match consume.race(signal).await {
            Some(cmd) => match cmd {
                Command::Stop => break,
                Command::Continue => continue,
            },
            None => break,
        }
    }
}

/*************************************/
/*         CONDUIT TX STRUCT         */
/*************************************/

// Structs to manage the sequence numbers of channels
struct SeqNumTx {
    reliable: SeqNumGenerator,
    unreliable: SeqNumGenerator,
}

impl SeqNumTx {
    fn new(sn0_reliable: ZInt, sn0_unreliable: ZInt, resolution: ZInt) -> SeqNumTx {
        SeqNumTx {
            reliable: SeqNumGenerator::make(sn0_reliable, resolution).unwrap(),
            unreliable: SeqNumGenerator::make(sn0_unreliable, resolution).unwrap(),
        }
    }
}

// Store the mutable data that need to be used for transmission
struct ConduitInnerTx {
    sn: SeqNumTx,
    batchsize: usize,
    links: Vec<Link>,
}

impl ConduitInnerTx {
    fn new(resolution: ZInt, batchsize: usize) -> ConduitInnerTx {
        // @TODO: Randomly initialize the SN generator
        let zero: ZInt = 0;

        ConduitInnerTx {
            sn: SeqNumTx::new(zero, zero, resolution),
            batchsize,
            links: Vec::new(),
        }
    }
}

pub(crate) struct ConduitTx {
    pub(crate) queue: CreditQueue<MessageTx>,
    inner: Mutex<ConduitInnerTx>,
    signal: Mutex<Option<Sender<Command>>>,
}

impl ConduitTx {
    pub(crate) fn new(resolution: ZInt, batchsize: usize) -> ConduitTx {
        // The buffer to send the Control messages. High priority
        let ctrl = CreditBuffer::<MessageTx>::new(
            QUEUE_SIZE_CTRL,
            QUEUE_CRED_CTRL,
            CreditBuffer::<MessageTx>::spending_policy(|_msg| 0isize),
        );
        // The buffer to send the retransmission of messages. Medium priority
        let retx = CreditBuffer::<MessageTx>::new(
            QUEUE_SIZE_RETX,
            QUEUE_CRED_RETX,
            CreditBuffer::<MessageTx>::spending_policy(|_msg| 0isize),
        );
        // The buffer to send the Data messages. Low priority
        let data = CreditBuffer::<MessageTx>::new(
            QUEUE_SIZE_DATA,
            QUEUE_CRED_DATA,
            // @TODO: Once the reliability queue is implemented, update the spending policy
            CreditBuffer::<MessageTx>::spending_policy(|_msg| 0isize),
        );
        // Build the vector of buffer for the transmission queue.
        // A lower index in the vector means higher priority in the queue.
        // The buffer with index 0 has the highest priority.
        let queue_tx = vec![ctrl, retx, data];

        ConduitTx {
            queue: CreditQueue::new(queue_tx, QUEUE_CONCURRENCY),
            inner: Mutex::new(ConduitInnerTx::new(resolution, batchsize)),
            signal: Mutex::new(None),
        }
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

    pub(crate) async fn add_link(&self, link: Link) -> ZResult<()> {
        // Acquire the lock on the links
        let mut guard = zasynclock!(self.inner);
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
        guard.links.push(link.clone());

        Ok(())
    }
    pub(crate) async fn del_link(&self, link: Link) -> ZResult<Link> {
        // If the conduit is active, add it to the transmission_loop
        let mut guard = zasynclock!(self.inner);
        // Check if the link is present in the transport
        let index = self.find_link(&guard.links, &link.get_src(), &link.get_dst());
        // The link is not present, return an error
        let link = if let Some(i) = index {
            // Remove the link
            guard.links.remove(i)
        } else {
            return Err(zerror!(ZErrorKind::Other {
                descr: "Trying to delete a link that does not exist!".to_string()
            }));
        };

        Ok(link)
    }

    pub(crate) async fn start(conduit: Arc<ConduitTx>) {
        let mut guard = zasynclock!(conduit.signal);
        // If not already active, start the transmission loop
        if guard.is_none() {
            // Create the signal channel
            let (sender, receiver) = channel::<Command>(1);
            // Store the sender needed to stop the transmission loop
            *guard = Some(sender);
            // Drop the guard that borrows the conduit
            drop(guard);
            // Spawn the transmission loop
            task::spawn(transmission_loop(conduit, receiver));
        }
    }

    pub(crate) async fn stop(&self) {
        // Take the sender for the signal channel
        let signal = zasynclock!(self.signal).take();
        // If the transmission loop is active, send the stop signal
        if let Some(sender) = signal {
            sender.send(Command::Stop).await;
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
}

/*************************************/
/*         CONDUIT RX STRUCT         */
/*************************************/

// Structs to manage the sequence numbers of channels
struct SeqNumRx {
    reliable: SeqNum,
    unreliable: SeqNum,
}

impl SeqNumRx {
    fn new(sn0_reliable: ZInt, sn0_unreliable: ZInt, resolution: ZInt) -> SeqNumRx {
        SeqNumRx {
            reliable: SeqNum::make(sn0_reliable, resolution).unwrap(),
            unreliable: SeqNum::make(sn0_unreliable, resolution).unwrap(),
        }
    }
}

// Store the mutable data that need to be used for transmission
struct ConduitInnerRx {
    callback: Arc<dyn MsgHandler + Send + Sync>,
    sn: SeqNumRx,
}

impl ConduitInnerRx {
    fn new(resolution: ZInt, callback: Arc<dyn MsgHandler + Send + Sync>) -> ConduitInnerRx {
        // @TODO: Randomly initialize the SN generator
        ConduitInnerRx {
            callback,
            sn: SeqNumRx::new(resolution - 1, resolution - 1, resolution),
        }
    }
}

pub struct ConduitRx {
    session: Arc<SessionInner>,
    inner: Mutex<ConduitInnerRx>,
}

impl ConduitRx {
    pub(crate) fn new(
        resolution: ZInt,
        session: Arc<SessionInner>,
        callback: Arc<dyn MsgHandler + Send + Sync>,
    ) -> ConduitRx {
        ConduitRx {
            session,
            inner: Mutex::new(ConduitInnerRx::new(resolution, callback)),
        }
    }

    // /*************************************/
    // /*   MESSAGE RECEIVED FROM THE LINK  */
    // /*************************************/
    async fn process_reliable_message(&self, message: Message, sn: ZInt) {
        // @TODO: implement the reordering and wait for missing messages
        let mut guard = zasynclock!(self.inner);
        // Messages with invalid CID or invalid SN are automatically dropped
        if guard.sn.reliable.precedes(sn) && guard.sn.reliable.set(sn).is_ok() {
            let _ = guard.callback.handle_message(message).await;
        }
    }

    async fn process_unreliable_message(&self, message: Message, sn: ZInt) {
        let mut guard = zasynclock!(self.inner);
        // Messages with invalid CID or invalid SN are automatically dropped
        if guard.sn.unreliable.precedes(sn) && guard.sn.unreliable.set(sn).is_ok() {
            let _ = guard.callback.handle_message(message).await;
        }
    }

    async fn receive_full_message(
        &self,
        src: &Locator,
        dst: &Locator,
        message: Message,
    ) -> Option<Arc<Transport>> {
        match &message.body {
            Body::Accept {
                whatami,
                opid,
                apid,
                lease,
            } => {
                let c_lease = *lease;
                match self
                    .session
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
                self.session.process_close(src, dst, pid, c_reason).await;
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
                match self
                    .session
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
    ) -> Option<Arc<Transport>> {
        unimplemented!("Defragementation not implemented yet!");
    }

    async fn receive_middle_fragement(
        &self,
        _src: &Locator,
        _dst: &Locator,
        _message: Message,
    ) -> Option<Arc<Transport>> {
        unimplemented!("Defragementation not implemented yet!");
    }

    async fn receive_last_fragement(
        &self,
        _src: &Locator,
        _dst: &Locator,
        _message: Message,
    ) -> Option<Arc<Transport>> {
        unimplemented!("Defragementation not implemented yet!");
    }

    pub async fn receive_message(
        &self,
        src: &Locator,
        dst: &Locator,
        message: Message,
    ) -> Option<Arc<Transport>> {
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
}
