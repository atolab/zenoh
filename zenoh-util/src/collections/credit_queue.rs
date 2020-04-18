use async_std::sync::Mutex;
use std::sync::atomic::{
    AtomicIsize,
    Ordering
};

use crate::zasynclock;
use crate::collections::CircularBuffer;
use crate::sync::Condition;


type SpendingClosure<T> = Box<dyn Fn(&T) -> isize + Send + Sync>;

pub struct CreditBuffer<T> {
    capacity: usize,
    credit: isize,
    spending: SpendingClosure<T>
}

impl<T> CreditBuffer<T> {
    pub fn new(capacity: usize, credit: isize, spending: SpendingClosure<T>) -> CreditBuffer<T> {
        CreditBuffer {
            capacity,
            credit,
            spending
        }
    }

    pub fn spending_policy<F>(f: F) -> SpendingClosure<T> 
    where F: Fn(&T) -> isize + Send + Sync + 'static
    {
        Box::new(f)
    }
}

/// Credit-based queue
/// 
/// This queue is meant to be used in scenario where a credit-based fair queueing (a.k.a. scheduling) is desired.
/// The [`CreditQueue`][CreditQueue] implementation leverages the [`async-std`](https://docs.rs/async-std) library, 
/// making it a good choice for all the applications using the [`async-std`](https://docs.rs/async-std) library.
/// 
/// Multiple priority queues can be configured with different queue capacity (i.e. number of elements
/// that can be stored), initial credit amount and spending policy.
/// 
pub struct CreditQueue<T> {
    state: Mutex<Vec<CircularBuffer<T>>>,
    credit: Vec<AtomicIsize>,
    spending: Vec<SpendingClosure<T>>,
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
    pub fn new(mut queues: Vec<CreditBuffer<T>>, concurrency_level: usize) -> CreditQueue<T> {
        let mut state = Vec::with_capacity(queues.len());
        let mut credit = Vec::with_capacity(queues.len());
        let mut spending = Vec::with_capacity(queues.len());
        for buffer in queues.drain(..) {
            state.push(CircularBuffer::new(buffer.capacity));
            credit.push(AtomicIsize::new(buffer.credit));
            spending.push(buffer.spending);
        }
         
        CreditQueue { 
            state: Mutex::new(state),
            credit,
            spending,
            not_full: Condition::new(concurrency_level),
            not_empty: Condition::new(concurrency_level),
        }
    }

    #[inline]
    pub fn get_credit(&self, priority: usize) -> isize {
        self.credit[priority].load(Ordering::Acquire)
    }

    #[inline]
    pub fn spend(&self, priority: usize, amount: isize) {
        self.credit[priority].fetch_sub(amount, Ordering::AcqRel);
    }

    #[inline]
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

    pub async fn push_batch(&self, mut v: Vec<T>, priority: usize) {
        while !v.is_empty() {
            loop {
                let mut q = zasynclock!(self.state);
                let tot = (q[priority].capacity() - q[priority].len()).min(v.len());
                // Start draining the elements
                for t in v.drain(0..tot) {
                    q[priority].push(t);
                }
                // If some element has been pushed and there is a waiting list,
                // notify the pullers and return the messages not pushed on the queue
                if tot > 0 {
                    if self.not_empty.has_waiting_list() {
                        self.not_empty.notify(q).await;
                    }
                    break;
                }
                self.not_full.wait(q).await;  
            }
        }         
    }

    pub async fn pull(&self) -> T {
        loop {
            let mut q = zasynclock!(self.state);
            for priority in 0usize..q.len() {
                if self.get_credit(priority) > 0 {
                    if let Some(e) = q[priority].pull() {
                        self.spend(priority, (self.spending[priority])(&e));
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

    pub async fn drain(&self) -> Vec<T> {
        // The vector to return
        let mut v = Vec::<T>::new();

        loop {
            let mut q = zasynclock!(self.state);

            // Compute the total number of messages in the queue
            let mut len = 0;
            for queue in q.iter() {
                len += queue.len();
            }

            // Reserve enough space on the vector
            v.reserve(len);

            for priority in 0usize..q.len() {
                // Drain a single priority queue while there is enough credit
                while self.get_credit(priority) > 0 {
                    if let Some(e) = q[priority].pull() {
                        self.spend(priority, (self.spending[priority])(&e));
                        v.push(e);
                    } else {
                        break
                    }
                }
            }

            if !v.is_empty() {
                if self.not_full.has_waiting_list() {
                    self.not_full.notify(q).await;
                }
                return v
            }

            self.not_empty.wait(q).await;
        }
    }

    pub async fn try_drain(&self) -> Vec<T> {
        // The vector to return
        let mut v = Vec::<T>::new();

        let mut q = zasynclock!(self.state);

        // Compute the total number of messages in the queue
        let mut len = 0;
        for queue in q.iter() {
            len += queue.len();
        }

        // Reserve enough space on the vector
        v.reserve(len);

        for priority in 0usize..q.len() {
            // Drain a single priority queue while there is enough credit
            while self.get_credit(priority) > 0 {
                if let Some(e) = q[priority].pull() {
                    self.spend(priority, (self.spending[priority])(&e));
                    v.push(e);
                } else {
                    break
                }
            }
        }

        if !v.is_empty() && self.not_full.has_waiting_list() {
            self.not_full.notify(q).await;
        }
        
        v
    }

    pub async fn drain_into(&self, v: &mut Vec<T>) {
        loop {
            let mut q = zasynclock!(self.state);
            for priority in 0usize..q.len() {
                // Drain a single priority queue while there is enough credit
                while self.get_credit(priority) > 0 {
                    if let Some(e) = q[priority].pull() {
                        self.spend(priority, (self.spending[priority])(&e));
                        v.push(e);
                    } else {
                        break
                    }
                }
            }

            if !v.is_empty() {
                if self.not_full.has_waiting_list() {
                    self.not_full.notify(q).await;
                }
                return;
            }

            self.not_empty.wait(q).await;
        }
    }

    pub async fn try_drain_into(&self, v: &mut Vec<T>) {
        let mut q = zasynclock!(self.state);

        for priority in 0usize..q.len() {
            // Drain a single priority queue while there is enough credit
            while self.get_credit(priority) > 0 {
                if let Some(e) = q[priority].pull() {
                    self.spend(priority, (self.spending[priority])(&e));
                    v.push(e);
                } else {
                    break
                }
            }
        }

        if !v.is_empty() && self.not_full.has_waiting_list() {
            self.not_full.notify(q).await;
        }
    }
}
