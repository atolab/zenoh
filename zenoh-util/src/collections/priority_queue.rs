use async_std::sync::Mutex;
use crossbeam::utils::Backoff;

use crate::collections::CQueue;
use crate::sync::Condition;


pub struct PriorityQueue<T> {
    state: Vec<Mutex<CQueue<T>>>,
    not_full: Vec<Condition>,
    not_empty: Condition,
    not_empty_lock: Mutex<bool>
}

impl<T> PriorityQueue<T> {
    pub fn new(capacity: Vec<usize>, concurrency_level: usize) -> PriorityQueue<T> {
        let mut state = Vec::with_capacity(capacity.len());
        let mut not_full = Vec::with_capacity(capacity.len());
        for c in capacity.iter() {
            state.push(Mutex::new(CQueue::new(*c)));
            not_full.push(Condition::new(concurrency_level));
        }
         
        PriorityQueue { 
            state,
            not_full,
            not_empty: Condition::new(concurrency_level),
            not_empty_lock: Mutex::new(true)
        }
    }

    // The try_lock in this function requires the returned guard to stay in scope
    // Using .is_some() instead of let Some(_) results in the lock guard being dropped
    #[allow(clippy::redundant_pattern_matching)]
    pub async fn push(&self, t: T, priority: usize) {
        let backoff = Backoff::new();
        loop {
            // Spinlock to access the queue
            let mut q = loop {
                if let Some(q) = self.state[priority].try_lock() {
                    break q
                }
                backoff.spin();
            };
            // Push on the queue if it is not full
            if !q.is_full() {
                q.push(t);
                // Spinlock before notifying
                backoff.reset();
                loop {
                    if let Some(_) = self.not_empty_lock.try_lock() {
                        if self.not_empty.has_waiting_list() {
                            self.not_empty.notify(q).await;
                        }  
                        break;
                    }
                    backoff.spin();
                }
                return;
            }                            
            self.not_full[priority].wait(q).await; 
            backoff.reset();     
        }            
    }

    pub async fn pull(&self) -> T {
        loop {
            // First, we attempt to pull from the queues with a try_lock
            // If a queue is locked by a push, a pull from the next queue is attempted 
            for priority in 0usize..self.state.len() {
                if let Some(mut q) = self.state[priority].try_lock() {
                    if let Some(e) = q.pull() {
                        if self.not_full[priority].has_waiting_list() {
                            self.not_full[priority].notify(q).await;
                        }                   
                        return e;
                    }
                }
            }
            // try_lock has failed, this is due to one of the following two possibilities:
            //  1) All the queues where blocked by simultaneous pushes
            //  2) All the queues were empty
            // Therefore, we perform a lock().await and we try to pull from the queue
            // If this succedes, it means that we were in case 1) otherwise in case 2) 
            for priority in 0usize..self.state.len() {
                let mut q = self.state[priority].lock().await;
                if let Some(e) = q.pull() {
                    if self.not_full[priority].has_waiting_list() {
                        self.not_full[priority].notify(q).await;
                    }                   
                    return e;
                }
            }
            // The blocking pull did not succeed. The queue is empty.
            // We block here and wait for a not_empty notification
            
            let _g = self.not_empty_lock.lock().await;            
            self.not_empty.wait(_g).await;
        }
    }
}