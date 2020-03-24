use async_std::sync::Mutex;
use std::collections::VecDeque;
use crate::sync::Condition;

struct CQueue<T> {
    buffer: VecDeque<T>,
    capacity: usize,
    n: usize
}

impl<T> CQueue<T> {
    fn new(capacity: usize) -> CQueue<T> {
        let buffer = VecDeque::<T>::with_capacity(capacity);        
        CQueue {buffer, capacity, n: 0}
    }

    fn push(&mut self, elem: T) -> bool {
        if self.n < self.capacity {
            self.buffer.push_back(elem);
            self.n += 1;
            true             
        } else { false }
    }

    #[inline]
    fn pull(&mut self) -> Option<T> {
        let x = self.buffer.pop_front();
        if x.is_some() {
            self.n -= 1;
        }
        x
    }
    #[allow(dead_code)]
    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    fn is_full(&self) -> bool{
        self.n == self.capacity
    }

    fn len(&self) -> usize {
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
            {
                let mut q = self.state.lock().await;
                if !q.is_full() {
                    q.push(x);
                    if self.not_empty.has_waiting_list() {
                      self.not_empty.notify().await;
                    }                    
                    return;
                }
            }
            self.not_full.wait().await;            
        }            
    }

    pub async fn pull(&self) -> T {
        loop {
            {
                let mut q = self.state.lock().await;
                if let Some(e) = q.pull() {
                  if self.not_full.has_waiting_list() {
                    self.not_full.notify().await;
                  }                   
                  return e;
                }                                
            }
            self.not_empty.wait().await;
        }
    }

    pub async fn drain(&self) -> Vec<T> {
        let mut q = self.state.lock().await;
        let mut xs = Vec::with_capacity(q.len());        
        while let Some(x) = q.pull() {
            xs.push(x);
        }         
        xs
    }

    pub async fn drain_into(&self, xs: &mut Vec<T>){
        let mut q = self.state.lock().await;        
        while let Some(x) = q.pull() {
            xs.push(x);
        }                 
    }
}
