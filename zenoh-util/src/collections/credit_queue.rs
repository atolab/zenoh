use async_std::sync::{
    Mutex,
    MutexGuard
};
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
    pub async fn get_credit(&self, priority: usize) -> isize {
        self.credit[priority].load(Ordering::Acquire)
    }

    #[inline]
    pub async fn spend(&self, priority: usize, amount: isize) {
        self.credit[priority].fetch_sub(amount, Ordering::AcqRel);
    }

    #[inline]
    pub async fn recharge(&self, priority: usize, amount: isize) {
        // Recharge the credit for a given priority queue
        let old = self.credit[priority].fetch_add(amount, Ordering::AcqRel);
        // We had a negative credit, now it is recharged and it might be above zero
        // If the credit is positive, we might be able to pull from the queue
        if old <= 0 && self.get_credit(priority).await > 0 {
            let q = zasynclock!(self.state);
            if self.not_empty.has_waiting_list() {
                self.not_empty.notify(q).await;
            }
        }
    }

    // Lock and return a guard on the queue
    // This allows to perform as many try_pull/try_push as desired
    // IMPORTANT: remember to call the .unlock().await on the guard
    // after having finalized the try_pull/try_push operations
    pub async fn lock(&self) -> CreditQueueGuard<'_, T> {
        let guard = zasynclock!(self.state);
        CreditQueueGuard::new(guard, self)
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


pub struct CreditQueueGuard<'a, T> {
    state: MutexGuard<'a, Vec<CircularBuffer<T>>>,
    queue: &'a CreditQueue<T>,
    pushed: bool,
    pulled: bool,
    recharged: bool
}

impl<'a, T> CreditQueueGuard<'a, T> {
    fn new(state: MutexGuard<'a, Vec<CircularBuffer<T>>>, queue: &'a CreditQueue<T>) -> CreditQueueGuard<'a, T> {
        CreditQueueGuard {
            state,
            queue,
            pushed: false,
            pulled: false,
            recharged: false
        }
    }

    pub fn peek(&self) -> Option<&T> {
        for priority in 0usize..self.state.len() {
            if self.queue.credit[priority].load(Ordering::Acquire) > 0 {
                if let Some(e) = self.state[priority].peek() {
                    return Some(e);
                }
            }
        }
        None
    }

    #[inline]
    pub fn get_credit(&self, priority: usize) -> isize {
        // Return the amount of credit
        self.queue.credit[priority].load(Ordering::Acquire)
    }

    #[inline]
    pub fn spend(&mut self, priority: usize, amount: isize) {
        // Spend the credit for a given priority queue
        self.queue.credit[priority].fetch_sub(amount, Ordering::AcqRel);
    }

    #[inline]
    pub fn recharge(&mut self, priority: usize, amount: isize) {
        // Recharge the credit for a given priority queue
        let old = self.queue.credit[priority].fetch_add(amount, Ordering::AcqRel);
        // We had a negative credit, now it is recharged and it might be above zero
        // If the credit is positive, we might be able to pull from the queue
        if old <= 0 && self.get_credit(priority) > 0 {
            self.recharged = true
        }
    }

    pub fn try_pull(&mut self) -> Option<T> {
        for priority in 0usize..self.state.len() {
            if self.queue.credit[priority].load(Ordering::Acquire) > 0 {
                if let Some(e) = self.state[priority].pull() {
                    self.pulled = true;
                    return Some(e);
                }
            }
        }
        None
    }

    pub fn try_push(&mut self, t: T, priority: usize) -> Option<T> {
        if !self.state[priority].is_full() {
            self.state[priority].push(t);
            self.pushed = true;
            return None;
        }
        Some(t)          
    }

    // This function is necessary to notify any pending push or pull
    // Ideally, this function should be implemented in the Drop destructor in such a 
    // a way it is automatically called when the CreditQueueGuard goes out of scope.
    // However, no async Drop exsits as of today and this needs to be manually called.
    // More information on the possibility of having an async destructor can be found
    // here: https://internals.rust-lang.org/t/asynchronous-destructors/11127
    // The unlock function consumes the interal MutexGuard
    pub async fn unlock(self) {
        // We give priority at emptying the queue, so if a new element
        // was pushed on the queue and a not_empty condition is pending,
        // we first notify the pending not_empty
        if (self.pushed || self.recharged) && self.queue.not_empty.has_waiting_list() {
            self.queue.not_empty.notify(self.state).await;
        } else if self.pulled && self.queue.not_full.has_waiting_list() {
            self.queue.not_full.notify(self.state).await;
        }
    }
}