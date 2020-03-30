use async_std::sync::Mutex;
use std::collections::VecDeque;
use crate::sync::Condition;

pub(crate) struct CQueue<T> {
    buffer: VecDeque<T>,
    capacity: usize,
    n: usize
}

impl<T> CQueue<T> {
    pub(crate) fn new(capacity: usize) -> CQueue<T> {
        let buffer = VecDeque::<T>::with_capacity(capacity);        
        CQueue {buffer, capacity, n: 0}
    }

    pub(crate) fn push(&mut self, elem: T) -> bool {
        if self.n < self.capacity {
            self.buffer.push_back(elem);
            self.n += 1;
            true             
        } else { false }
    }

    #[inline]
    pub(crate) fn pull(&mut self) -> Option<T> {
        let x = self.buffer.pop_front();
        if x.is_some() {
            self.n -= 1;
        }
        x
    }
    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub(crate) fn is_full(&self) -> bool{
        self.n == self.capacity
    }

    pub(crate) fn len(&self) -> usize {
        self.n
    }
}

pub struct CircularQueue<T: Copy> {
    state: Mutex<CQueue<T>>,
    not_empty: Condition,
    not_full: Condition
}

impl<T:Copy> CircularQueue<T> {
    pub fn new(capacity: usize, concurrency_level: usize) -> CircularQueue<T> {
        CircularQueue { 
            state: Mutex::new(CQueue::new(capacity)),
            not_empty: Condition::new(concurrency_level),
            not_full: Condition::new(concurrency_level)            
        }
    }

    pub async fn push(&self, x: T) {
        loop {
            let mut q = if let Some(g) = self.state.try_lock() { g } else { self.state.lock().await };
            if !q.is_full() {
                q.push(x);
                if self.not_empty.has_waiting_list() {
                    self.not_empty.notify(q).await;
                }                    
                return;                                    
            }
            self.not_full.wait(q).await;            
        }            
    }

    pub async fn pull(&self) -> T {
        loop {
            let mut q = if let Some(g) = self.state.try_lock() { g } else { self.state.lock().await };
            if let Some(e) = q.pull() {
                if self.not_full.has_waiting_list() {
                    self.not_full.notify(q).await;
                }                   
                return e;
            }          
            self.not_empty.wait(q).await;
        }                
    }

    pub async fn drain(&self) -> Vec<T> {
        let mut q = if let Some(g) = self.state.try_lock() { g } else { self.state.lock().await };        
        let mut xs = Vec::with_capacity(q.len());        
        while let Some(x) = q.pull() {
            xs.push(x);
        }         
        if self.not_full.has_waiting_list() {
            self.not_full.notify_all(q).await;
          }                   
        xs
    }

    pub async fn drain_into(&self, xs: &mut Vec<T>){
        let mut q = if let Some(g) = self.state.try_lock() { g } else { self.state.lock().await };
        while let Some(x) = q.pull() {
            xs.push(x);
        }                 
        if self.not_full.has_waiting_list() {
            self.not_full.notify_all(q).await;
        }                   
    }
}
