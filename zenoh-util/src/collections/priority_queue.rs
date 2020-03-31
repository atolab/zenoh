use async_std::sync::Mutex;

use crate::zasynclock;
use crate::collections::CQueue;
use crate::sync::Condition;


pub struct PriorityQueue<T> {
    state: Mutex<Vec<CQueue<T>>>,
    not_full: Condition,
    not_empty: Condition
}

impl<T> PriorityQueue<T> {
    pub fn new(capacity: Vec<usize>, concurrency_level: usize) -> PriorityQueue<T> {
        let mut state = Vec::with_capacity(capacity.len());
        for c in capacity.iter() {
            state.push(CQueue::new(*c));
        }
         
        PriorityQueue { 
            state: Mutex::new(state),
            not_full: Condition::new(concurrency_level),
            not_empty: Condition::new(concurrency_level)
        }
    }

    pub async fn push(&self, t: T, priority: usize) {
        loop {
            let mut q = zasynclock!(self.state);
            // Push on the queue if it is not full
            if !q[priority].is_full() {
                q[priority].push(t);
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
            for priority in 0usize..q.len() {
                if let Some(e) = q[priority].pull() {
                    if self.not_full.has_waiting_list() {
                        self.not_full.notify(q).await;
                    }                   
                    return e;
                }
            }
            self.not_empty.wait(q).await;
        }
    }
}