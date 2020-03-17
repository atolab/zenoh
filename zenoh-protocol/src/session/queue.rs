use async_std::prelude::Future;
use async_std::sync::{
    Arc,
    Mutex,
    Sender
};
use async_std::task::{
    Context, 
    Poll
};
use crossbeam::queue::{
    ArrayQueue,
    PushError
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
use crate::session::WakerSet;


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
    syncack: ArrayQueue<MessageTxPush>,
    control: ArrayQueue<MessageTxPush>,
    retransmission: ArrayQueue<MessageTxPop>,
    fragment: ArrayQueue<MessageTxPush>,
    data: ArrayQueue<MessageTxPush>,
    reliability: Mutex<OrderedQueue<MessageTxPop>>,
    sn_tx_reliable: AtomicZInt,
    sn_tx_unreliable: AtomicZInt,
    w_pop: WakerSet,
    w_push: WakerSet,
    w_retx: WakerSet
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
            w_pop: WakerSet::new(),
            w_push: WakerSet::new(),
            w_retx: WakerSet::new()
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
                            *sn = match is_reliable {
                                true => self.sn_tx_reliable.fetch_add(1, Ordering::Relaxed),
                                false => self.sn_tx_unreliable.fetch_add(1, Ordering::Relaxed),
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
                    if let Some(sn) = new_sn {
                        guard.try_push(msg.clone(), sn);
                    }

                    // If the reliability queue 
                    match guard.is_full() {
                        true => return QueueTxTryPopResult::NeedSync(msg),
                        false => return QueueTxTryPopResult::Ok(msg)
                    }
                }
            }
        } 

        QueueTxTryPopResult::Empty
    }

    pub async fn pop(&self) -> QueueTxPopResult {
        struct FuturePop<'a> {
            queue: &'a QueueTx,
            opt_key: Option<usize>
        }
        
        impl<'a> Future for FuturePop<'a> {
            type Output = QueueTxPopResult;
        
            fn poll(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
                // If the current task is in the set, remove it.
                if let Some(key) = self.opt_key.take() {
                    self.queue.w_pop.remove(key);
                }
                match self.queue.try_pop() {
                    QueueTxTryPopResult::Ok(message) => {
                        self.queue.w_retx.notify_any();
                        self.queue.w_push.notify_any();
                        Poll::Ready(QueueTxPopResult::Ok(message))
                    },
                    QueueTxTryPopResult::NeedSync(message) => {
                        self.queue.w_retx.notify_any();
                        self.queue.w_push.notify_any();
                        Poll::Ready(QueueTxPopResult::NeedSync(message))
                    },
                    QueueTxTryPopResult::Empty => {
                        self.opt_key = Some(self.queue.w_pop.insert(ctx));
                        Poll::Pending
                    }
                }
            }
        }

        impl Drop for FuturePop<'_> {
            fn drop(&mut self) {
                if let Some(key) = self.opt_key {
                    self.queue.w_pop.cancel(key);
                }
            }
        }

        FuturePop {
            queue: self,
            opt_key: None
        }.await
    }
    
    fn try_push(&self, message: MessageTxPush) -> Option<MessageTxPush> {
        let (queue, is_data) = match message.inner.kind {
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
                if message.inner.is_reliable() || !is_data {
                    Some(message)
                } else {
                    // Drop the message for non reliable data messages
                    None
                }
            }
        }
    }

    pub async fn push(&self, message: MessageTxPush) {
        struct FuturePush<'a> {
            queue: &'a QueueTx,
            message: Option<MessageTxPush>,
            opt_key: Option<usize>
        }

        impl Unpin for FuturePush<'_> {}
        
        impl<'a> Future for FuturePush<'a> {
            type Output = ();
        
            fn poll(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
                // If the current task is in the set, remove it.
                if let Some(key) = self.opt_key.take() {
                    self.queue.w_pop.remove(key);
                }
                let message = self.message.take().unwrap();
                match self.queue.try_push(message) {
                    None => {
                        self.queue.w_pop.notify_any();
                        Poll::Ready(())
                    },
                    Some(message) => {
                        self.message = Some(message);
                        self.opt_key = Some(self.queue.w_push.insert(ctx));
                        Poll::Pending
                    }
                }
            }
        }

        impl Drop for FuturePush<'_> {
            fn drop(&mut self) {
                if let Some(key) = self.opt_key {
                    self.queue.w_pop.cancel(key);
                }
            }
        }

        FuturePush {
            queue: self,
            message: Some(message),
            opt_key: None
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
            sn: ZInt,
            opt_key: Option<usize>
        }
        
        impl<'a> Future for FutureRetx<'a> {
            type Output = ();
        
            fn poll(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
                // If the current task is in the set, remove it.
                if let Some(key) = self.opt_key.take() {
                    self.queue.w_retx.remove(key);
                }
                match self.queue.try_reschedule(self.sn) {
                    true => {
                        self.queue.w_pop.notify_any();
                        Poll::Ready(())
                    }
                    false => {
                        self.opt_key = Some(self.queue.w_push.insert(ctx));
                        Poll::Pending
                    }
                }
            }
        }

        impl Drop for FutureRetx<'_> {
            fn drop(&mut self) {
                if let Some(key) = self.opt_key {
                    self.queue.w_pop.cancel(key);
                }
            }
        }

        FutureRetx {
            queue: self,
            sn,
            opt_key: None
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
        if self.len() == 0 {
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
                None => mask = mask | (1 << iteration)
            }
            iteration = iteration + 1;
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
            if let Some(_) = self.buff[self.pointer].take() {
                // Decrement the counter
                self.counter = self.counter - 1;
            }
            // Increment the pointer
            self.pointer = (self.pointer + 1) % self.capacity();
        }

        // Align the first and last sequence numbers
        self.first = base;
        if self.len() == 0 {
            self.last = self.first;
        }
    }

    // This operation does not modify the base or the pointer
    // It simply removes an element if it matches the sn 
    pub fn try_remove(&mut self, sn: ZInt) -> Option<T> {
        if self.len() > 0 {
            let gap = sn.wrapping_sub(self.first) as usize;
            let index = (self.pointer + gap) % self.capacity();
            if let Some(element) = &self.buff[index] {
                if element.sn == sn {
                    // The element is the right one, take with unwrap
                    let element = self.buff[index].take().unwrap();
                    // Decrement the counter
                    self.counter = self.counter - 1;
                    // Align the last sequence number if the queue is empty
                    if self.len() == 0 {
                        self.last = self.first;
                    }
                    return Some(element.into_inner())
                }
            }
        }
        None
    }

    pub fn try_pop(&mut self) -> Option<T> {
        if self.len() > 0 {
            if let Some(element) = self.buff[self.pointer].take() {
                // Update the pointer in the buffer
                self.pointer = (self.pointer + 1) % self.capacity();
                // Decrement the counter
                self.counter = self.counter - 1;
                // Increment the next target sequence number
                self.first = self.first.wrapping_add(1);
                // Align the last sequence number if the queue is empty
                if self.len() == 0 {
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
        self.counter = self.counter + 1;

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
        if self.len() > 0 {
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
            match first {
                true => first = false,
                false => s.push_str(", ")
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



// ************************ //
//    PRIORITY QUEUE        //
// ************************ //
// pub struct PriorityQueue<T> {
//     buff: Vec<ArrayQueue<T>>,
//     w_pop: SegQueue<Waker>,
//     w_push: SegQueue<Waker>
// }

// impl<T> PriorityQueue<T> {
//     pub fn new(capacity: usize, priorities: usize) -> Self {
//         let mut v = Vec::new();
//         for _ in 0..priorities {
//             v.push(ArrayQueue::new(capacity))
//         }
//         Self {
//             buff: v,
//             w_pop: SegQueue::new(),
//             w_push: SegQueue::new()
//         }
//     }

//     pub fn try_pop(&self) -> Option<(T, usize)> {
//         for i in 0..self.buff.len() {
//             if let Ok(msg) = self.buff[i].pop() {
//                 return Some((msg, i))
//             }
//         }
//         None
//     }

//     pub fn try_push(&self, message: T, priority: usize) -> Option<T> {
//         let queue = if priority < self.buff.len() {
//             &self.buff[priority]
//         } else {
//             &self.buff[self.buff.len()-1]
//         };
//         match queue.push(message) {
//             Ok(_) => None,
//             Err(PushError(message)) => Some(message)
//         }
//     }

//     pub async fn push(&self, message: T, priority: usize) {
//         struct FuturePush<'a, U> {
//             queue: &'a PriorityQueue<U>,
//             message: Mutex<Option<U>>,
//             priority: usize
//         }
        
//         impl<'a, U> Future for FuturePush<'a, U> {
//             type Output = ();
        
//             fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
//                 if let Some(mut guard) = self.message.try_lock() {
//                     if let Some(message) = guard.take() {
//                         self.queue.w_push.push(ctx.waker().clone());
//                         match self.queue.try_push(message, self.priority) {
//                             None => return Poll::Ready(()),
//                             Some(message) => *guard = Some(message)
//                         }
//                     }
//                 }
//                 Poll::Pending
//             }
//         }

//         impl<U> Drop for FuturePush<'_, U> {
//             fn drop(&mut self) {
//                 while let Ok(waker) = self.queue.w_pop.pop() {
//                     waker.wake();
//                 }
//             }
//         }

//         FuturePush {
//             queue: self,
//             message: Mutex::new(Some(message)),
//             priority
//         }.await
//     }

    
//     pub async fn pop(&self) -> (T, usize) {
//         struct FuturePop<'a, U> {
//             queue: &'a PriorityQueue<U>
//         }
        
//         impl<'a, U> Future for FuturePop<'a, U> {
//             type Output = (U, usize);
        
//             fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
//                 self.queue.w_pop.push(ctx.waker().clone());
//                 match self.queue.try_pop() {
//                     Some((msg, prio)) => Poll::Ready((msg, prio)),
//                     None => Poll::Pending
//                 }
//             }
//         }

//         impl<U> Drop for FuturePop<'_, U> {
//             fn drop(&mut self) {
//                 while let Ok(waker) = self.queue.w_push.pop() {
//                     waker.wake();
//                 }
//             }
//         }

//         FuturePop {
//             queue: self
//         }.await
//     }

// }