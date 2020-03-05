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
use std::pin::Pin;


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

    pub async fn push(&self, message: T, priority: Option<usize>) {
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

        let priority = match priority {
            Some(prio) => prio,
            None => self.buff.len()-1 
        };
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
