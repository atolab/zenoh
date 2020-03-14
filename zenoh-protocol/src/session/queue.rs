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
use std::fmt;
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

    pub fn try_pop(&self) -> Option<(T, usize)> {
        for i in 0..self.buff.len() {
            if let Ok(msg) = self.buff[i].pop() {
                return Some((msg, i))
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

    
    pub async fn pop(&self) -> (T, usize) {
        struct FuturePop<'a, U> {
            queue: &'a PriorityQueue<U>
        }
        
        impl<'a, U> Future for FuturePop<'a, U> {
            type Output = (U, usize);
        
            fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
                self.queue.w_pop.push(ctx.waker().clone());
                match self.queue.try_pop() {
                    Some((msg, prio)) => Poll::Ready((msg, prio)),
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