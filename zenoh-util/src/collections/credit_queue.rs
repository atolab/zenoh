use async_std::sync::Mutex;
use crossbeam::utils::Backoff;
use std::sync::atomic::{
    AtomicIsize,
    Ordering
};

use crate::collections::CQueue;
use crate::sync::Condition;


pub struct CreditQueue<T> {
    state: Vec<Mutex<CQueue<T>>>,
    credit: Vec<AtomicIsize>,
    not_full: Vec<Condition>,
    not_empty: Condition,
    not_empty_lock: Mutex<bool>
}

impl<T> CreditQueue<T> {
    pub fn new(capacity: Vec<(usize, isize)>, concurrency_level: usize) -> CreditQueue<T> {
        let mut state = Vec::with_capacity(capacity.len());
        let mut credit = Vec::with_capacity(capacity.len());
        let mut not_full = Vec::with_capacity(capacity.len());
        for (cap, cre) in capacity.iter() {
            state.push(Mutex::new(CQueue::new(*cap)));
            credit.push(AtomicIsize::new(*cre));
            not_full.push(Condition::new(concurrency_level));
        }
         
        CreditQueue { 
            state,
            credit,
            not_full,
            not_empty: Condition::new(concurrency_level),
            not_empty_lock: Mutex::new(true)
        }
    }

    #[inline]
    pub fn get_credit(&self, priority: usize) -> isize {
        self.credit[priority].load(Ordering::Acquire)
    }

    #[inline]
    pub async fn spend(&self, priority: usize, amount: isize) {
        self.credit[priority].fetch_sub(amount, Ordering::Release);
    }

    // The try_lock in this function requires the returned guard to stay in scope
    // Using .is_some() instead of let Some(_) results in the lock guard being dropped
    #[allow(clippy::redundant_pattern_matching)]
    pub async fn recharge(&self, priority: usize, amount: isize) {
        // Spinlock before notifying
        self.credit[priority].fetch_add(amount, Ordering::Release);
        let backoff = Backoff::new();
        loop {
            if let Some(_) = self.not_empty_lock.try_lock() {
                // Queue refilled, we might be able to pull now
                if self.not_empty.has_waiting_list() {
                    self.not_empty.notify().await;
                }  
                break;
            }
            backoff.spin();
        }
    }

    // The try_lock in this function requires the returned guard to stay in scope
    // Using .is_some() instead of let Some(_) results in the lock guard being dropped
    #[allow(clippy::redundant_pattern_matching)]
    pub async fn push(&self, t: T, priority: usize) {
        let backoff = Backoff::new();
        loop {
            {
                // Spinlock to access the queue
                let mut q = loop {
                    if let Some(q) = self.state[priority].try_lock() {
                        break q
                    }
                    backoff.spin();
                };
                if !q.is_full() {
                    q.push(t);
                    // Spinlock before notifying
                    backoff.reset();
                    loop {
                        if let Some(_) = self.not_empty_lock.try_lock() {
                            if self.not_empty.has_waiting_list() {
                                self.not_empty.notify().await;
                            }  
                            break;
                        }
                        backoff.spin();
                    }
                    return;
                }
                self.not_full[priority].going_to_waiting_list();
            }
            self.not_full[priority].wait().await;
            backoff.reset();   
        }            
    }

    pub async fn pull(&self) -> T {
        loop {
            // First attempt a pull with a try_lock
            // If a queue is locked by a push, a pull from the next queue is attempted 
            for priority in 0usize..self.state.len() {
                if self.credit[priority].load(Ordering::Acquire) > 0 {
                    if let Some(mut q) = self.state[priority].try_lock() {
                        if let Some(e) = q.pull() {
                            if self.not_full[priority].has_waiting_list() {
                                self.not_full[priority].notify().await;
                            }
                            return e;
                        }
                    }
                }
            }
            // Try lock has failed, this might be due to two possibilities:
            //  1) All the queues where blocked by simultaneous push
            //  2) The queue was empty
            // Therefore, we perform a blocking lock and try to pull from the queue
            // If this succedes, it means that we were in case 1) otherwise in case 2) 
            for priority in 0usize..self.state.len() {
                if self.credit[priority].load(Ordering::Acquire) > 0 {
                    let mut q = self.state[priority].lock().await;
                    if let Some(e) = q.pull() {
                        if self.not_full[priority].has_waiting_list() {
                            self.not_full[priority].notify().await;
                        }
                        return e;
                    }
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