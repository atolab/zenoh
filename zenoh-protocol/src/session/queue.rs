use async_std::sync::Arc;

use crossbeam::queue::ArrayQueue;

use crate::proto::Message;


// Struct ZQueue
#[derive(Debug)]
pub enum QueueError {
    IsEmpty,
    IsFull
}

pub struct Queue {
    high: ArrayQueue<Arc<Message>>,
    low: ArrayQueue<Arc<Message>>
}

impl Queue {
    pub fn new(capacity: usize) -> Self {
        Self {
            high: ArrayQueue::new(capacity),
            low: ArrayQueue::new(capacity)
        }
    }

    pub fn pop(&self) -> Result<Arc<Message>, QueueError> {
        if let Ok(msg) = self.high.pop() {
            return Ok(msg)
        }
        if let Ok(msg) = self.low.pop() {
            return Ok(msg)
        }
        return Err(QueueError::IsEmpty)
    }

    pub fn push(&self, message: Arc<Message>) -> Result<(), QueueError> {
        let queue = match message.is_reliable() {
            true => &self.high,
            false => &self.low
        };
        match queue.push(message) {
            Ok(_) => return Ok(()),
            Err(_) => return Err(QueueError::IsFull)
        }
    }
}
