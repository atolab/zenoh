use async_std::sync::Mutex;
use std::sync::atomic::{
    AtomicIsize,
    Ordering
};

use crate::zasynclock;
use crate::collections::CircularBuffer;
use crate::sync::Condition;


/// Credit-based queue
/// 
/// This queue is meant to be used in scenario where a credit-based fair queueing (a.k.a. scheduling) is desired.
/// The [`CreditQueue`][CreditQueue] implementation leverages the [`async-std`](https://docs.rs/async-std) library, 
/// making it a good choice for all the applications using the [`async-std`](https://docs.rs/async-std) library.
/// 
/// Multiple priority queues can be configured with different queue capacity (i.e. number of elements
/// that can be stored) and initial credit amount.
/// 
pub struct CreditQueue<T> {
    state: Mutex<Vec<CircularBuffer<T>>>,
    credit: Vec<AtomicIsize>,
    not_full: Condition,
    not_empty: Condition
}

impl<T> CreditQueue<T> {
    /// Create a new credit-based queue.
    /// 
    /// # Arguments
    /// * `queue` - A vector containing the parameters for the queues in the form of tuples: (capacity, credits)      
    ///
    /// * `concurrency_level` - The desired concurrency_level when accessing a single priority queue.
    /// 
    pub fn new(queues: Vec<(usize, isize)>, concurrency_level: usize) -> CreditQueue<T> {
        let mut state = Vec::with_capacity(queues.len());
        let mut credit = Vec::with_capacity(queues.len());
        for (cap, cre) in queues.iter() {
            state.push(CircularBuffer::new(*cap));
            credit.push(AtomicIsize::new(*cre));
        }
         
        CreditQueue { 
            state: Mutex::new(state),
            credit,
            not_full: Condition::new(concurrency_level),
            not_empty: Condition::new(concurrency_level),
        }
    }

    #[inline]
    pub fn get_credit(&self, priority: usize) -> isize {
        self.credit[priority].load(Ordering::Acquire)
    }

    #[inline]
    pub async fn spend(&self, priority: usize, amount: isize) {
        self.credit[priority].fetch_sub(amount, Ordering::AcqRel);
    }

    pub async fn recharge(&self, priority: usize, amount: isize) {
        // Recharge the credit for a given priority queue
        let old = self.credit[priority].fetch_add(amount, Ordering::AcqRel);
        // We had a negative credit, now it is recharged and it might be above zero
        // If the credit is positive, we might be able to pull from the queue
        if old <= 0 && self.get_credit(priority) > 0 {
            let q = zasynclock!(self.state);
            if self.not_empty.has_waiting_list() {
                self.not_empty.notify(q).await;
            }
        }
    }

    pub async fn try_push(&self, t: T, priority: usize) -> Option<T> {
        let mut q = zasynclock!(self.state);
        if !q[priority].is_full() {
            q[priority].push(t);
            if self.not_empty.has_waiting_list() {
                self.not_empty.notify(q).await;
            } 
            return None;
        }
        Some(t)          
    }

    pub async fn push(&self, t: T, priority: usize) {
        loop {
            let mut q = zasynclock!(self.state);
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

    pub async fn try_pull(&self) -> Option<T> {
        let mut q = zasynclock!(self.state);
        for priority in 0usize..q.len() {
            if self.credit[priority].load(Ordering::Acquire) > 0 {
                if let Some(e) = q[priority].pull() {
                    if self.not_full.has_waiting_list() {
                        self.not_full.notify(q).await;
                    }
                    return Some(e);
                }
            }
        }
        None
    }

    pub async fn pull(&self) -> T {
        loop {
            let mut q = zasynclock!(self.state);
            for priority in 0usize..q.len() {
                if self.credit[priority].load(Ordering::Acquire) > 0 {
                    if let Some(e) = q[priority].pull() {
                        if self.not_full.has_waiting_list() {
                            self.not_full.notify(q).await;
                        }
                        return e;
                    }
                }
            }
            self.not_empty.wait(q).await;
        }
    }
}