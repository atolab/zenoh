use async_std::sync::{
    Arc,
    Barrier
};
use crossbeam::queue::ArrayQueue;

use crate::proto::Message;
use crate::link::Locator;


pub struct QueueMessage {
    pub message: Arc<Message>, 
    pub priority: Option<usize>, 
    pub link: Option<(Locator, Locator)>,
    pub barrier: Option<Arc<Barrier>>
}

pub struct Queue {
    high: ArrayQueue<Arc<QueueMessage>>,
    low: ArrayQueue<Arc<QueueMessage>>
}

impl Queue {
    pub fn new(capacity: usize) -> Self {
        Self {
            high: ArrayQueue::new(capacity),
            low: ArrayQueue::new(capacity)
        }
    }

    pub fn pop(&self) -> Option<Arc<QueueMessage>> {
        if let Ok(msg) = self.high.pop() {
            return Some(msg)
        }
        if let Ok(msg) = self.low.pop() {
            return Some(msg)
        }
        None
    }

    pub fn push(&self, message: Arc<QueueMessage>) -> bool {
        let queue = match message.priority {
            Some(0) => &self.high,
            _ => &self.low
        };
        match queue.push(message) {
            Ok(_) => true,
            Err(_) => false
        }
    }
}
