use async_std::prelude::Future;
use async_std::sync::{
    Arc,
    Mutex,
    Sender
};
use async_std::task::{
    Context, 
    Poll,
    Waker
};
use crossbeam::queue::{
    ArrayQueue,
    PushError,
    SegQueue
};
use std::fmt;
use std::pin::Pin;
use std::sync::atomic::Ordering;

use crate::core::{
    AtomicZInt,
    ZInt,
    ZResult
};
use crate::link::Locator;
use crate::proto::{
    Body,
    Message,
    MessageKind
};


// Struct to add additional fields to the message
pub struct MessageTxPush {
    // The inner message to transmit
    pub inner: Message, 
    // The preferred link to transmit the Message on
    pub link: Option<(Locator, Locator)>,
    pub notify: Option<Sender<ZResult<()>>>
}

pub struct MessageTxPop {
    // The inner message to transmit
    pub inner: Arc<Message>, 
    // The preferred link to transmit the Message on
    pub link: Option<(Locator, Locator)>,
    pub notify: Option<Sender<ZResult<()>>>
}

impl Clone for MessageTxPop {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            link: match &self.link {
                Some((src, dst)) => Some((src.clone(), dst.clone())),
                None => None
            },
            notify: match &self.notify {
                Some(sender) => Some(sender.clone()),
                None => None
            }
        }
    }
}

pub enum MessageTxKind {
    SyncAck,
    Control,
    Fragment,
    Data
}

// Enum for the result of ZenohQueueTx push operation
pub enum QueueTxPopResult {
    Ok(MessageTxPop),
    NeedSync(MessageTxPop)
}

pub enum QueueTxTryPopResult {
    Ok(MessageTxPop),
    NeedSync(MessageTxPop),
    Empty
}

// ************************ //
//        RX QUEUE          //
// ************************ //
pub type QueueRx<T> = Mutex<OrderedQueue<T>>;

// ************************ //
//        TX QUEUE          //
// ************************ //
pub struct QueueTx {
    // The first 4 queues might be replaced by a priority queue (min-heap)?
    syncack: ArrayQueue<MessageTxPush>,
    control: ArrayQueue<MessageTxPush>,
    retransmission: ArrayQueue<MessageTxPop>,
    fragment: ArrayQueue<MessageTxPush>,
    // This is a separate queue
    data: ArrayQueue<MessageTxPush>,
    reliability: Mutex<OrderedQueue<MessageTxPop>>,
    // The sequence number for the reliable and unreliable channels
    sn_tx_reliable: AtomicZInt,
    sn_tx_unreliable: AtomicZInt,
    // The unbounded queues storing the wakers for the futures
    w_pop: SegQueue<Waker>,
    w_push: SegQueue<Waker>,
    w_retx: SegQueue<Waker>
}

impl QueueTx {
    pub fn new(capacity: usize) -> Self {
        Self {
            syncack: ArrayQueue::new(capacity),
            control: ArrayQueue::new(capacity),
            retransmission: ArrayQueue::new(capacity),
            fragment: ArrayQueue::new(capacity),
            data: ArrayQueue::new(capacity),
            reliability: Mutex::new(OrderedQueue::new(capacity)),
            sn_tx_reliable: AtomicZInt::new(0),
            sn_tx_unreliable: AtomicZInt::new(0),
            w_pop: SegQueue::new(),
            w_push: SegQueue::new(),
            w_retx: SegQueue::new()
        }
    }

    pub async fn get_reliability_base(&self) -> ZInt {
        self.reliability.lock().await.get_base()
    }

    pub async fn get_reliability_base_and_count(&self) -> (ZInt, ZInt) {
        let guard = self.reliability.lock().await;
        (guard.get_base(), guard.len() as ZInt)
    }

    pub async fn set_reliability_base(&self, sn: ZInt) {
        self.reliability.lock().await.set_base(sn);
        // We might have freed some space in the reliability queue
        // Notify the pending pop operations
        while let Ok(waker) = self.w_pop.pop() {
            waker.wake();
        }
    }

    fn try_pop(&self) -> QueueTxTryPopResult {
        if let Ok(msg) = self.syncack.pop() {
            let msg = MessageTxPop {
                inner: Arc::new(msg.inner),
                link: msg.link,
                notify: msg.notify
            };
            return QueueTxTryPopResult::Ok(msg)
        }
        if let Ok(msg) = self.control.pop() {
            let msg = MessageTxPop {
                inner: Arc::new(msg.inner),
                link: msg.link,
                notify: msg.notify
            };
            return QueueTxTryPopResult::Ok(msg)
        }
        if let Ok(msg) = self.retransmission.pop() {
            return QueueTxTryPopResult::Ok(msg)
        }
        if let Ok(msg) = self.fragment.pop() {
            let msg = MessageTxPop {
                inner: Arc::new(msg.inner),
                link: msg.link,
                notify: msg.notify
            };
            return QueueTxTryPopResult::Ok(msg)
        }
        // Try to access the reliability queue before trying to pop from data
        if let Some(mut guard) = self.reliability.try_lock() {
            // If the reliability queue is not full, we are ready to try to pop from the data queue
            if !guard.is_full() {
                // Try to pop from the data queue
                if let Ok(mut msg) = self.data.pop() {
                    // Update the sequence number 
                    let mut new_sn: Option<ZInt> = None;
                    // Check if the message is reliable
                    let is_reliable = msg.inner.is_reliable();
                    match msg.inner.body {
                        Body::Data{reliable: _, ref mut sn, key: _, info: _, payload: _} |
                        Body::Declare{ref mut sn, declarations: _} |
                        Body::Pull{ref mut sn, key: _, pull_id: _, max_samples: _} |
                        Body::Query{ref mut sn, key: _, predicate: _, qid: _, target: _, consolidation: _} => {
                            // Update the sequence number
                            *sn = if is_reliable {
                                self.sn_tx_reliable.fetch_add(1, Ordering::Relaxed)
                            } else {
                                self.sn_tx_unreliable.fetch_add(1, Ordering::Relaxed)
                            };
                            new_sn = Some(*sn);
                        },
                        _ => {}
                    }

                    // Create the message for reliability queue
                    let msg = MessageTxPop {
                        inner: Arc::new(msg.inner),
                        link: msg.link,
                        notify: msg.notify
                    };

                    // If the message is reliable, add it to the reliability queue
                    // @TODO: finalize the reliability queue free, then uncomment the 
                    //     following code block to add reliable messages to the queue
                    // if is_reliable {
                    //     if let Some(sn) = new_sn {
                    //         guard.try_push(msg.clone(), sn);
                    //     }
                    // }

                    // If the reliability queue 
                    if guard.is_full() {
                        return QueueTxTryPopResult::NeedSync(msg)
                    } else {
                        return QueueTxTryPopResult::Ok(msg)
                    }
                }
            }
        } 

        QueueTxTryPopResult::Empty
    }

    pub async fn pop(&self) -> QueueTxPopResult {
        struct FuturePop<'a> {
            queue: &'a QueueTx
        }
        
        impl<'a> Future for FuturePop<'a> {
            type Output = QueueTxPopResult;
        
            fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
                self.queue.w_pop.push(ctx.waker().clone());
                match self.queue.try_pop() {
                    QueueTxTryPopResult::Ok(message) => Poll::Ready(QueueTxPopResult::Ok(message)),
                    QueueTxTryPopResult::NeedSync(message) => Poll::Ready(QueueTxPopResult::NeedSync(message)),
                    QueueTxTryPopResult::Empty => Poll::Pending
                }
            }
        }

        impl Drop for FuturePop<'_> {
            fn drop(&mut self) {
                while let Ok(waker) = self.queue.w_retx.pop() {
                    waker.wake();
                }
                while let Ok(waker) = self.queue.w_push.pop() {
                    waker.wake();
                }
            }
        }

        FuturePop {
            queue: self
        }.await
    }
    
    fn try_push(&self, message: MessageTxPush) -> Option<MessageTxPush> {
        let (queue, _is_data) = match message.inner.kind {
            MessageKind::FullMessage => match message.inner.body {
                // SyncAck messages
                Body::AckNack{..} |
                Body::Sync{..} => (&self.syncack, false),
                // Control messages
                Body::Accept{..} |
                Body::Hello{..} |
                Body::KeepAlive{..} |
                Body::Open{..} |
                Body::Ping{..} |
                Body::Pong{..} |
                Body::Scout{..} => (&self.control, false),
                // Close message needs to be sent last
                Body::Close{..} => (&self.data, false),
                // Data messages
                Body::Data{..} |
                Body::Declare{..} |
                Body::Pull{..} |
                Body::Query{..} => (&self.data, true),
            },
            MessageKind::FirstFragment{..} |
            MessageKind::InbetweenFragment |
            MessageKind::LastFragment => (&self.fragment, false),
        };

        match queue.push(message) {
            Ok(_) => None,
            Err(PushError(message)) => {
                Some(message)
                // @TODO: Currently the action  does not depend on the branch,
                // once the proper action will be in place we'll put back in the branch.
                //
                // if message.inner.is_reliable() || !is_data {
                //    Some(message)
                // } else {
                //     // Don't drop the message for the time being
                //     // Return None to drop the message //
                // }
            }
        }
    }

    pub async fn push(&self, message: MessageTxPush) {
        struct FuturePush<'a> {
            queue: &'a QueueTx,
            message: Option<MessageTxPush>
        }

        impl Unpin for FuturePush<'_> {}
        
        impl<'a> Future for FuturePush<'a> {
            type Output = ();
        
            fn poll(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
                self.queue.w_push.push(ctx.waker().clone());
                let message = self.message.take().unwrap();
                match self.queue.try_push(message) {
                    None => Poll::Ready(()),
                    Some(message) => {
                        self.message = Some(message);
                        Poll::Pending
                    }
                }
            }
        }

        impl Drop for FuturePush<'_> {
            fn drop(&mut self) {
                while let Ok(waker) = self.queue.w_pop.pop() {
                    waker.wake();
                }
            }
        }

        FuturePush {
            queue: self,
            message: Some(message)
        }.await
    }

    fn try_reschedule(&self, sn: ZInt) -> bool {
        if let Some(guard) = self.reliability.try_lock() {
            if let Some(message) = guard.try_get(sn) {
                // Drop the guard
                drop(guard);
                // Reschedule 
                match self.retransmission.push(message) {
                    Ok(_) => return true,
                    Err(PushError(_)) => return false
                }
            }
        }
        false
    }

    pub async fn reschedule(&self, sn: ZInt) {
        struct FutureRetx<'a> {
            queue: &'a QueueTx,
            sn: ZInt
        }
        
        impl<'a> Future for FutureRetx<'a> {
            type Output = ();
        
            fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
                self.queue.w_retx.push(ctx.waker().clone());
                if self.queue.try_reschedule(self.sn) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
        }

        impl Drop for FutureRetx<'_> {
            fn drop(&mut self) {
                while let Ok(waker) = self.queue.w_pop.pop() {
                    waker.wake();
                }
            }
        }

        FutureRetx {
            queue: self,
            sn
        }.await
    }
}


// Structs for OrderedQueue
pub struct OrderedElement<T> {
    element: T,
    sn: ZInt
}

impl<T> OrderedElement<T> {
    fn new(element: T, sn: ZInt) -> Self {
        Self {
            element,
            sn
        }
    }

    fn into_inner(self) -> T {
        self.element
    }
}

impl<T: Clone> OrderedElement<T> {
    fn inner_clone(&self) -> T {
        self.element.clone()
    }
}


pub struct OrderedQueue<T> {
    buff: Vec<Option<OrderedElement<T>>>,
    pointer: usize,
    counter: usize,
    first: ZInt,
    last: ZInt,
}

impl<T> OrderedQueue<T> {
    pub fn new(capacity: usize) -> Self {
        let mut buff = Vec::with_capacity(capacity);
        buff.resize_with(capacity, || None);
        Self {
            buff,
            pointer: 0,
            counter: 0,
            first: 0,
            last: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_full(&self) -> bool {
        self.len() == self.capacity()
    }

    pub fn capacity(&self) -> usize {
        self.buff.capacity()
    }

    pub fn len(&self) -> usize {
        self.counter
    }

    pub fn get_mask(&self) -> ZInt {
        let mut mask: ZInt = 0;

        // Check if the queue is empty
        if self.is_empty() {
            return mask
        }

        // Create the bitmask
        let mut iteration = 0;
        let mut index = self.pointer;
        loop {
            match &self.buff[index] {
                Some(element) => if element.sn == self.last {
                    break
                },
                None => mask |= 1 << iteration
            }
            iteration += 1;
            index = (index + 1) % self.capacity();
        }
        
        mask
    }

    pub fn get_base(&self) -> ZInt {
        self.first
    }

    pub fn set_base(&mut self, base: ZInt) {
        // Compute the circular gaps
        let gap_base = base.wrapping_sub(self.first) as usize;
        let gap_last = self.last.wrapping_sub(self.first) as usize;
        
        let count = if gap_base <= gap_last {
            gap_base 
        } else {
            self.capacity()
        };

        // Iterate over the queue and consume the inner elements
        for _ in 0..count {
            if self.buff[self.pointer].take().is_some() {
                // Decrement the counter
                self.counter -= 1;
            }
            // Increment the pointer
            self.pointer = (self.pointer + 1) % self.capacity();
        }

        // Align the first and last sequence numbers
        self.first = base;
        if self.is_empty() {
            self.last = self.first;
        }
    }

    // This operation does not modify the base or the pointer
    // It simply removes an element if it matches the sn 
    pub fn try_remove(&mut self, sn: ZInt) -> Option<T> {
        if !self.is_empty() {
            let gap = sn.wrapping_sub(self.first) as usize;
            let index = (self.pointer + gap) % self.capacity();
            if let Some(element) = &self.buff[index] {
                if element.sn == sn {
                    // The element is the right one, take with unwrap
                    let element = self.buff[index].take().unwrap();
                    // Decrement the counter
                    self.counter -= 1;
                    // Align the last sequence number if the queue is empty
                    if self.is_empty() {
                        self.last = self.first;
                    }
                    return Some(element.into_inner())
                }
            }
        }
        None
    }

    pub fn try_pop(&mut self) -> Option<T> {
        if !self.is_empty() {
            if let Some(element) = self.buff[self.pointer].take() {
                // Update the pointer in the buffer
                self.pointer = (self.pointer + 1) % self.capacity();
                // Decrement the counter
                self.counter -= 1;
                // Increment the next target sequence number
                self.first = self.first.wrapping_add(1);
                // Align the last sequence number if the queue is empty
                if self.is_empty() {
                    self.last = self.first;
                }
                return Some(element.into_inner())
            }
        }
        None
    }

    pub fn try_push(&mut self, element: T, sn: ZInt) -> Option<T> {
        // Do a modulo substraction
        let gap = sn.wrapping_sub(self.first) as usize;
        // Return error if the gap is larger than the capacity
        if gap >= self.capacity() {
            return Some(element)
        }

        // Increment the counter
        self.counter += 1;

        // Update the sequence number
        if sn > self.last {
            self.last = sn;
        }
        // Insert the element in the queue
        let index = (self.pointer + gap) % self.capacity();
        self.buff[index] = Some(OrderedElement::new(element, sn));
        
        None
    }
}

impl<T: Clone> OrderedQueue<T> {
    // This operation does not modify the base or the pointer
    // It simply gets a clone of an element if it matches the sn 
    pub fn try_get(&self, sn: ZInt) -> Option<T> {
        if !self.is_empty() {
            let gap = sn.wrapping_sub(self.first) as usize;
            let index = (self.pointer + gap) % self.capacity();
            if let Some(element) = &self.buff[index] {
                if element.sn == sn {
                    // The element is the right one, take with unwrap
                    return Some(self.buff[index].as_ref().unwrap().inner_clone())
                }
            }
        }
        None
    }
}

impl<T> fmt::Debug for OrderedQueue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = "[".to_string();
        let mut first = true;
        let mut index = self.pointer;
        for _ in 0..self.buff.capacity() {
            if first {
                first = false;
            } else {
                s.push_str(", ");
            }
            match &self.buff[index] {
                Some(e) => s.push_str(&format!("{}", e.sn)),
                None => s.push_str("None")
            }
            index = (index + 1) % self.capacity();
        }
        s.push_str("]\n");
        write!(f, "{}", s)
    }
}
