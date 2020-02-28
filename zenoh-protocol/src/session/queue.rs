use async_std::sync::Arc;
use crossbeam::queue::ArrayQueue;

use crate::proto::Message;
use crate::proto::Locator;


// Struct ZQueue
#[derive(Debug)]
pub enum QueueError {
    IsEmpty,
    IsFull
}

pub type QueueMessage = (Arc<Message>, Option<usize>, Option<(Locator, Locator)>);
// type QueueMessage = Arc<Message>;

pub struct Queue {
    high: ArrayQueue<QueueMessage>,
    low: ArrayQueue<QueueMessage>
}

impl Queue {
    pub fn new(capacity: usize) -> Self {
        Self {
            high: ArrayQueue::new(capacity),
            low: ArrayQueue::new(capacity)
        }
    }

    pub fn pop(&self) -> Result<QueueMessage, QueueError> {
        if let Ok(msg) = self.high.pop() {
            return Ok(msg)
        }
        if let Ok(msg) = self.low.pop() {
            return Ok(msg)
        }
        Err(QueueError::IsEmpty)
    }

    pub fn push(&self, message: QueueMessage) -> Result<(), QueueError> {
        let queue = match message.1 {
            Some(0) => &self.high,
            _ => &self.low
        };
        match queue.push(message) {
            Ok(_) => Ok(()),
            Err(_) => Err(QueueError::IsFull)
        }
    }
}
