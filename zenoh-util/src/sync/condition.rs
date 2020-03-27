use async_std::sync::{Receiver, Sender};
use async_std::sync::channel;
use std::sync::atomic::{AtomicUsize, Ordering};

/// This is a Condition Variable similar to that provided by POSIX. 
/// As for POSIX condition variables, this assumes that a mutex is 
/// properly used to coordinate behaviour. In other terms there should
/// not be race condition on [notify].
/// 
pub struct Condition {
  wait_rx: Receiver<bool>,
  wait_tx: Sender<bool>,  
  waiters: AtomicUsize

}

impl Condition {
  /// Creates a new condition variable with a given capacity. 
  /// The capacity indicates the maximum number of tasks that 
  /// may be waiting on the condition.
  pub fn new(capacity: usize) -> Condition {
    let (wait_tx, wait_rx) = channel(capacity);    
    Condition {wait_tx, wait_rx, waiters: AtomicUsize::new(0)}
  }

  /// Waits for the condition to be notified
  #[inline]
  pub async fn wait(&self) {    
    // self.waiters.fetch_add(1, Ordering::Release);
    let _ = self.wait_rx.recv().await;    
  }

  #[inline]
  pub fn going_to_waiting_list(&self) {
    self.waiters.fetch_add(1, Ordering::Release);
  }

  #[inline]
  pub fn has_waiting_list(&self) -> bool {
    self.waiters.load(Ordering::Acquire) > 0
  }

  /// Notify one task on the waiting list. The waiting list is 
  /// managed as a FIFO queue.
  #[inline]
  pub async fn notify(&self) {
      if self.has_waiting_list() {
        self.wait_tx.send(true).await;
        self.waiters.fetch_sub(1, Ordering::Release);
      }
    }
}