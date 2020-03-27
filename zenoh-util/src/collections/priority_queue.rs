use async_std::sync::Mutex;

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

    pub async fn push(&self, t: T, priority: usize) {
        loop {
            {
                let mut q = self.state[priority].lock().await;
                if !q.is_full() {
                    q.push(t);
                    // Lock before notifying
                    let _g = self.not_empty_lock.lock().await;
                    if self.not_empty.has_waiting_list() {
                        self.not_empty.notify().await;
                    }  
                    return;
                }
                self.not_full[priority].going_to_waiting_list();
            }
            self.not_full[priority].wait().await;      
        }            
    }

    pub async fn pull(&self) -> T {
        loop {
            // First attempt a pull with a try_lock
            // If a queue is locked by a push, a pull from the next queue is attempted 
            for priority in 0usize..self.state.len() {
                if let Some(mut q) = self.state[priority].try_lock() {
                    if let Some(e) = q.pull() {
                        if self.not_full[priority].has_waiting_list() {
                            self.not_full[priority].notify().await;
                        }                   
                        return e;
                    }
                }
            }
            // Try lock has failed, this might be due to two possibilities:
            //  1) All the queues where blocked by simultaneous push
            //  2) The queue was empty
            // Therefore, we perform a blocking lock and try to pull from the queue
            // If this succedes, it means that we were in case 1) otherwise in case 2) 
            for priority in 0usize..self.state.len() {
                let mut q = self.state[priority].lock().await;
                if let Some(e) = q.pull() {
                    if self.not_full[priority].has_waiting_list() {
                        self.not_full[priority].notify().await;
                    }                   
                    return e;
                }
            }
            // The blocking pull did not succeed. The queue is empty.
            // We block here and wait for a notification
            {
                let _g = self.not_empty_lock.lock().await;
                self.not_empty.going_to_waiting_list();
            }
            self.not_empty.wait().await;
        }
    }
}