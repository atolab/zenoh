use async_std::prelude::Future;
use async_std::task::{
    Context, 
    Poll,
    Waker
};
use crossbeam::queue::{
    ArrayQueue,
    SegQueue
};

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
            buff: v
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

    pub fn try_push(&self, message: T, priority: usize) -> bool {
        let queue = match message.priority {
            Some(0) => &self.high,
            _ => &self.low
        };
        match queue.push(message) {
            Ok(_) => true,
            Err(_) => false
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
                self.queue.w_push.push(ctx.waker().clone());
                match self.queue.queue.push(self.message.clone()) {
                    true => Poll::Ready(0),
                    false => Poll::Pending
                }
            }
        }

        impl<U> Drop for FuturePush<'_, U> {
            fn drop(&mut self) {
                while let Some(waker) = self.queue.w_pop.pop() {
                    waker.wake();
                }
            }
        }

        FutureSchedule {
            queue: self,
            message: Mutex::new(Some(message)),
            priority
        }.await
    }

    
    pub async fn pop(&self) -> Arc<QueueMessage> {
        struct FuturePop<'a> {
            transport: &'a Transport
        }
        
        impl<'a> Future for FuturePop<'a> {
            type Output = Arc<QueueMessage>;
        
            fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
                self.transport.c_waker.store(Some(ctx.waker().clone()));
                match self.transport.queue.pop() {
                    Some(msg) => Poll::Ready(msg),
                    None => Poll::Pending
                }
            }
        }

        impl Drop for FuturePop<'_> {
            fn drop(&mut self) {
                while let Ok(waker) = self.transport.s_waker.pop() {
                    waker.wake();
                }
            }
        }

        FutureConsume {
            transport: self
        }.await
    }

}
