use async_std::task;
use std::fmt;
use std::sync::atomic::spin_loop_hint;

const SPIN_LIMIT: usize = 6;
const YIELD_LIMIT: usize = 10;


/// Performs exponential backoff in spin loops.
///
/// This is the async version of the Backoff implementation provided by
/// [`Crossebam utils`]: https://docs.rs/crossbeam/0.7.3/crossbeam/utils/struct.Backoff.html

pub struct Backoff {
    step: usize,
}

impl Backoff {
    #[inline]
    pub fn new() -> Self {
        Backoff { step: 0 }
    }

    #[inline]
    pub fn reset(&mut self) {
        self.step = 0;
    }

    #[inline]
    pub fn spin(&mut self) {
        for _ in 0..1 << self.step.min(SPIN_LIMIT) {
            spin_loop_hint();
        }

        if self.step <= SPIN_LIMIT {
            self.step += 1;
        }
    }

    #[inline]
    pub async fn snooze(&mut self) {
        if self.step <= SPIN_LIMIT {
            for _ in 0..1 << self.step {
                spin_loop_hint();
            }
        } else {
            task::yield_now().await;
        }

        if self.step <= YIELD_LIMIT {
            self.step += 1;
        }
    }

    #[inline]
    pub fn is_completed(&self) -> bool {
        self.step > YIELD_LIMIT
    }
}

impl fmt::Debug for Backoff {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Backoff")
            .field("step", &self.step)
            .field("is_completed", &self.is_completed())
            .finish()
    }
}

impl Default for Backoff {
    fn default() -> Backoff {
        Backoff::new()
    }
}