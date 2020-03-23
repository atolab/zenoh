use async_std::sync::Mutex;
use async_std::sync::{Receiver, Sender};
use async_std::sync::channel;
use std::collections::VecDeque;

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
    
    notify_push_rx: Receiver<bool>,
    notify_push_tx: Sender<bool>,
    
    wait_push_rx: Receiver<bool>,
    wait_push_tx: Sender<bool>,
    
    notify_pull_rx: Receiver<bool>,
    notify_pull_tx: Sender<bool>,
    
    wait_pull_rx: Receiver<bool>,
    wait_pull_tx: Sender<bool>
}


impl<T:Copy> CircularQueue<T> {
    pub fn new(capacity: usize) -> CircularQueue<T> {
        let (notify_push_tx, notify_push_rx) = channel(16);
        let (wait_push_tx, wait_push_rx) = channel(16);
        let (notify_pull_tx, notify_pull_rx) = channel(16);
        let (wait_pull_tx, wait_pull_rx) = channel(16);
        CircularQueue { 
            state: Mutex::new(CQueue::new(capacity)),
            notify_push_rx, notify_push_tx,  
            wait_push_rx, wait_push_tx,
            notify_pull_rx, notify_pull_tx,
            wait_pull_rx, wait_pull_tx            
        }
    }
    
    pub async fn push(&self, x: T) {
        loop {
            {
                let mut q = self.state.lock().await;
                if !q.is_full() {
                    q.push(x);
                    if !self.wait_pull_rx.is_empty() {              
                        self.wait_pull_rx.recv().await;                        
                        self.notify_pull_tx.send(true).await;
                    }
                    return;
                }
            } 
            self.wait_push_tx.send(true).await;
            self.notify_push_rx.recv().await;

        }            
    }

    pub async fn pull(&self) -> T {
        loop {
            {
                let mut q = self.state.lock().await;
                if let Some(e) = q.pull() {
                    if !self.wait_push_rx.is_empty() {
                        self.wait_push_rx.recv().await;
                        self.notify_push_tx.send(true).await;
                    }                        
                    return e;
                }                
                
            }
            self.wait_pull_tx.send(true).await;
            self.notify_pull_rx.recv().await;
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
