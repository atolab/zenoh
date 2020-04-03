use async_std::sync::Mutex;

use crate::zasynclock;
use crate::collections::CircularBuffer;
use crate::sync::Condition;


pub struct FifoQueue<T: Copy> {
    state: Mutex<CircularBuffer<T>>,
    not_empty: Condition,
    not_full: Condition
}

impl<T:Copy> FifoQueue<T> {
    pub fn new(capacity: usize, concurrency_level: usize) -> FifoQueue<T> {
        FifoQueue { 
            state: Mutex::new(CircularBuffer::new(capacity)),
            not_empty: Condition::new(concurrency_level),
            not_full: Condition::new(concurrency_level)            
        }
    }

    pub async fn push(&self, x: T) {
        loop {
            let mut q = zasynclock!(self.state);
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
            let mut q = zasynclock!(self.state);
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
        let mut q = zasynclock!(self.state);
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
        let mut q = zasynclock!(self.state);
        while let Some(x) = q.pull() {
            xs.push(x);
        }                 
        if self.not_full.has_waiting_list() {
            self.not_full.notify_all(q).await;
        }                   
    }
}