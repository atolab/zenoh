use async_std::prelude::Future;
use async_std::sync::Mutex;
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
use std::collections::BinaryHeap;
use std::cmp::Ordering;
use std::pin::Pin;

use crate::core::ZInt;


pub struct PriorityQueue<T> {
    buff: Vec<ArrayQueue<T>>,
    w_pop: SegQueue<Waker>,
    w_push: SegQueue<Waker>
}

impl<T> PriorityQueue<T> {
    pub fn new(capacity: usize, priorities: usize) -> Self {
        let mut v = Vec::new();
        for _ in 0..priorities {
            v.push(ArrayQueue::new(capacity))
        }
        Self {
            buff: v,
            w_pop: SegQueue::new(),
            w_push: SegQueue::new()
        }
    }

    pub fn try_pop(&self) -> Option<T> {
        for i in 0..self.buff.len() {
            if let Ok(msg) = self.buff[i].pop() {
                return Some(msg)
            }
        }
        None
    }

    pub fn try_push(&self, message: T, priority: usize) -> Option<T> {
        let queue = if priority < self.buff.len() {
            &self.buff[priority]
        } else {
            &self.buff[self.buff.len()-1]
        };
        match queue.push(message) {
            Ok(_) => None,
            Err(PushError(message)) => Some(message)
        }
    }

    pub async fn push(&self, message: T, priority: usize) {
        struct FuturePush<'a, U> {
            queue: &'a PriorityQueue<U>,
            message: Mutex<Option<U>>,
            priority: usize
        }
        
        impl<'a, U> Future for FuturePush<'a, U> {
            type Output = ();
        
            fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
                if let Some(mut guard) = self.message.try_lock() {
                    if let Some(message) = guard.take() {
                        self.queue.w_push.push(ctx.waker().clone());
                        match self.queue.try_push(message, self.priority) {
                            None => return Poll::Ready(()),
                            Some(message) => *guard = Some(message)
                        }
                    }
                }
                Poll::Pending
            }
        }

        impl<U> Drop for FuturePush<'_, U> {
            fn drop(&mut self) {
                while let Ok(waker) = self.queue.w_pop.pop() {
                    waker.wake();
                }
            }
        }

        FuturePush {
            queue: self,
            message: Mutex::new(Some(message)),
            priority
        }.await
    }

    
    pub async fn pop(&self) -> T {
        struct FuturePop<'a, U> {
            queue: &'a PriorityQueue<U>
        }
        
        impl<'a, U> Future for FuturePop<'a, U> {
            type Output = U;
        
            fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
                self.queue.w_pop.push(ctx.waker().clone());
                match self.queue.try_pop() {
                    Some(msg) => Poll::Ready(msg),
                    None => Poll::Pending
                }
            }
        }

        impl<U> Drop for FuturePop<'_, U> {
            fn drop(&mut self) {
                while let Ok(waker) = self.queue.w_push.pop() {
                    waker.wake();
                }
            }
        }

        FuturePop {
            queue: self
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

impl<T> Eq for OrderedElement<T> {}

impl<T> PartialEq for OrderedElement<T> {
    fn eq(&self, other: &Self) -> bool {
        self.sn == other.sn
    }
}

impl<T> Ord for OrderedElement<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Invert the comparison orther to implement a min-heap
        other.sn.cmp(&self.sn)
    }
}

impl<T> PartialOrd for OrderedElement<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}


pub enum OrderedPushError<T> {
    Full(T),
    OutOfSync(T)
}

pub struct OrderedQueue<T> {
    buff: BinaryHeap<OrderedElement<T>>,
    head: ZInt,
    tail: ZInt,
    next: ZInt,
    mask: ZInt,
}

impl<T> OrderedQueue<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            buff: BinaryHeap::with_capacity(capacity),
            head: 0,
            tail: 0,
            next: 0,
            mask: 0
        }
    }

    pub fn capacity(&self) -> ZInt {
        // THIS IS A CAST!!! 
        // NEED TO CHECK IF ZInt usize otherwise we loose information
        self.buff.capacity() as ZInt
    }

    pub fn free(&self) -> ZInt {
        self.capacity() - self.len()
    }

    pub fn len(&self) -> ZInt {
        // THIS IS A CAST!!! 
        // NEED TO CHECK IF ZInt > usize otherwise we loose information
        self.buff.len() as ZInt
    }

    pub fn get_mask(&self) -> ZInt {
        let window = !(ZInt::max_value() << (self.tail - self.head + 1));
        self.mask & window
    }

    pub fn get_base(&self) -> ZInt {
        self.head
    }

    pub fn set_base(&mut self, base: ZInt) {
        if base < self.head || base >= self.head + self.capacity() {
            // RESET
            self.head = base;
            self.tail = base;
            self.next = base;
            self.mask = 0;
            self.buff.clear();
        } else {
            // SHIFT
            self.head = base;
            self.next = base;
            self.mask = self.mask >> (base - self.head);
            while let Some(element) = self.buff.peek() {
                if element.sn < base {
                    self.buff.pop();
                }
            }
        }
    }

    pub fn try_pop(&mut self) -> Option<T> {
        if let Some(element) = self.buff.peek() {
            if element.sn == self.next {
                let element = self.buff.pop().unwrap();
                self.next = self.next + 1;
                return Some(element.into_inner())
            }
        }
        None
    }

    pub fn try_push(&mut self, element: T, sn: ZInt) -> Result<(), OrderedPushError<T>> {
        if self.free() == 0 {
            return Err(OrderedPushError::Full(element))
        }
        if sn < self.head || sn >= self.head + self.capacity() {
            return Err(OrderedPushError::OutOfSync(element))
        }

        // Update the pointer
        if sn > self.tail {
            self.tail = sn;
        }
        // Update the bitmask
        self.mask = self.mask | (1 << (sn - self.head));
        // Push the element on the heap
        self.buff.push(OrderedElement::new(element, sn));
        
        Ok(())
    }
}