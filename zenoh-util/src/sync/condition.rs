use async_std::sync::{Receiver, Sender};
use async_std::sync::channel;

/// This is a Condition Variable similar to that provided by POSIX. 
/// As for POSIX condition variables, this assumes that a mutex is 
/// properly used to coordinate behaviour. In other terms there should
/// not be race condition on [notify].
/// 
pub struct Condition {
  wait_rx: Receiver<()>,
  wait_tx: Sender<()>,
  notify_rx: Receiver<()>,
  notify_tx: Sender<()>,
}

impl Condition {
  /// Creates a new condition variable with a given capacity. 
  /// The capacity indicates the maximum number of tasks that 
  /// may be waiting on the condition.
  pub fn new(capacity: usize) -> Condition {
    let (wait_tx, wait_rx) = channel(capacity);
    let (notify_tx, notify_rx) = channel(capacity);
    Condition {wait_tx, wait_rx, notify_tx, notify_rx}
  }

  /// Waits for the condition to be notified
  pub async fn wait(&self) {
    // Indicate you are waiting
    self.wait_tx.send(()).await;
    // Wait for notification
    let _ = self.notify_rx.recv().await;    
  }

  #[inline]
  pub fn has_waiting_list(&self) -> bool {
    !self.wait_rx.is_empty()
  }
  
  /// Notify one task on the waiting list. The waiting list is 
  /// managed as a FIFO queue.
  pub async fn notify(&self) {
    if !self.wait_rx.is_empty() {
      let _ = self.wait_rx.recv().await;
      self.notify_tx.send(()).await;
    }
  }
}