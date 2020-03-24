use async_std::sync::{
    Sender
};
use async_std::task::{
    Context,
    Poll
};
use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::future::Future;
use std::ops::{
    Deref, 
    DerefMut
};
use std::pin::Pin;
use std::sync::atomic::{
    AtomicBool,
    AtomicUsize,
    Ordering
};

use crate::core::ZResult;
use crate::link::Locator;
use crate::proto::Message;
use crate::session::WakerSet;


const H_BIT: usize = (usize::max_value() >> 1) + 1;
const L_BIT: usize = 1;


// Struct to add additional fields to the message
pub struct MessageTx {
    // The inner message to transmit
    pub inner: Message, 
    // The preferred link to transmit the Message on
    pub link: Option<(Locator, Locator)>,
    pub notify: Option<Sender<ZResult<()>>>
}


pub struct QueueInner<T> {
    inner: Vec<VecDeque<T>>,
    mask: usize
}

impl<T> QueueInner<T> {
    pub fn new(queues: usize, capacity: usize) -> Self {
        let mut inner = Vec::with_capacity(queues);
        for _ in 0..queues {
            inner.push(VecDeque::with_capacity(capacity))
        }
        Self {
            inner,
            mask: 0,
        }
    }

    pub fn capacity(&self, priority: usize) -> usize {
        self.inner[priority].capacity()
    }

    pub fn len(&self, priority: usize) -> usize {
        self.inner[priority].len()
    }

    pub fn is_empty(&self, priority: usize) -> bool {
        self.len(priority) == 0
    }

    pub fn is_full(&self, priority: usize) -> bool {
        self.len(priority) == self.capacity(priority)
    }

    pub fn get_mask(&self) -> usize {
        self.mask
    }

    pub fn push(&mut self, t: T, priority: usize) -> Option<T> {
        if !self.is_full(priority) {
            self.inner[priority].push_back(t);
            self.mask |= L_BIT << priority;
            if self.is_full(priority) {
                self.mask |= H_BIT >> priority;
            }
            return None
        }
        Some(t)
    }

    pub fn pop(&mut self) -> Option<(usize, T)> {
        for i in 0..self.inner.len() {
            if let Some(msg) = self.inner[i].pop_front() {
                if self.is_empty(i) {
                    self.mask &= !(L_BIT << i);
                }
                if !self.is_full(i) {
                    self.mask &= !(H_BIT >> i);
                }
                return Some((i, msg))
            }
        }
        None
    }
}


// pub struct QueuePrio<T>(MutexQueuePrio<QueueInner<T>>);

// impl<T> QueuePrio<T> {
//     pub fn new(queues: usize, capacity: usize) -> QueuePrio<T> {
//         Self(MutexQueuePrio::new(QueueInner::new(queues, capacity), queues))
//     }

//     #[inline(always)]
//     pub async fn push(&self, t: T, priority: usize) {
//         let mut guard = self.0.lock_push(priority).await;
//         guard.push(t, priority);
//         self.0.update_mask(guard.get_mask());
//     }

//     #[inline(always)]
//     pub async fn pop(&self) -> (usize, T) {
//         let mut guard = self.0.lock_pop().await;
//         let t = guard.pop().unwrap();
//         self.0.update_mask(guard.get_mask());
//         t
//     }
// }




pub struct MutexQueuePrio<T: ?Sized> {
    locked: AtomicBool,
    wakers_push: Vec<WakerSet>,
    wakers_pop: WakerSet,
    l_filter: usize,
    mask: AtomicUsize,
    value: UnsafeCell<T>
}

unsafe impl<T: ?Sized + Send> Send for MutexQueuePrio<T> {}
unsafe impl<T: ?Sized + Send> Sync for MutexQueuePrio<T> {}

impl<T> MutexQueuePrio<T> {
    pub fn new(t: T, queues: usize) -> MutexQueuePrio<T> {
        let mut wakers_push = Vec::with_capacity(queues);
        for _ in 0..queues {
            wakers_push.push(WakerSet::new());
        }
        MutexQueuePrio {
            locked: AtomicBool::new(false),
            wakers_push,
            wakers_pop: WakerSet::new(),
            l_filter: !(usize::max_value() << queues),
            mask: AtomicUsize::new(0),
            value: UnsafeCell::new(t)
        }
    }
}

impl<T: ?Sized> MutexQueuePrio<T> {
    pub fn update_mask(&self, mask: usize) {
        self.mask.store(mask, Ordering::Release);
    }

    pub async fn lock_push(&self, priority: usize) -> MutexQueuePrioPushGuard<'_, T> {
        pub struct LockPushFuture<'a, T: ?Sized> {
            mutex: &'a MutexQueuePrio<T>,
            priority: usize,
            opt_key: Option<usize>,
        }

        impl<'a, T: ?Sized> Future for LockPushFuture<'a, T> {
            type Output = MutexQueuePrioPushGuard<'a, T>;

            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                loop {
                    // If the current task is in the set, remove it.
                    if let Some(key) = self.opt_key.take() {
                        self.mutex.wakers_push[self.priority].remove(key);
                    }

                    // Try acquiring the lock.
                    match self.mutex.try_lock_push() {
                        Some(guard) => {
                            // Check if the queue we are trying to push on is full or not
                            if self.mutex.mask.load(Ordering::Acquire) & (H_BIT >> self.priority) != 0 {
                                // Insert this lock operation.
                                self.opt_key = Some(self.mutex.wakers_push[self.priority].insert(cx));

                                return Poll::Pending;
                            }
                            return Poll::Ready(guard)
                        },
                        None => {
                            // Insert this lock operation.
                            self.opt_key = Some(self.mutex.wakers_push[self.priority].insert(cx));

                            // If the mutex is still locked, return.
                            if self.mutex.locked.load(Ordering::Acquire) {
                                return Poll::Pending;
                            }
                        }
                    }
                }
            }
        }

        impl<T: ?Sized> Drop for LockPushFuture<'_, T> {
            fn drop(&mut self) {
                // If the current task is still in the set, that means it is being cancelled now.
                if let Some(key) = self.opt_key {
                    self.mutex.wakers_push[self.priority].cancel(key);
                }
            }
        }

        LockPushFuture {
            mutex: self,
            priority,
            opt_key: None,
        }.await
    }


    pub async fn lock_pop(&self) -> MutexQueuePrioPopGuard<'_, T> {
        pub struct LockPopFuture<'a, T: ?Sized> {
            mutex: &'a MutexQueuePrio<T>,
            opt_key: Option<usize>
        }

        impl<'a, T: ?Sized> Future for LockPopFuture<'a, T> {
            type Output = MutexQueuePrioPopGuard<'a, T>;

            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                loop {
                    // If the current task is in the set, remove it.
                    if let Some(key) = self.opt_key.take() {
                        self.mutex.wakers_pop.remove(key);
                    }

                    // Try acquiring the lock.
                    match self.mutex.try_lock_pop() {
                        Some(guard) => {
                            // Check if there is at least one message to pop
                            if self.mutex.mask.load(Ordering::Acquire) & self.mutex.l_filter == 0 {
                                // Insert this lock operation.
                                self.opt_key = Some(self.mutex.wakers_pop.insert(cx));

                                return Poll::Pending;
                            }
                            return Poll::Ready(guard)
                        },
                        None => {
                            // Insert this lock operation.
                            self.opt_key = Some(self.mutex.wakers_pop.insert(cx));

                            // If the mutex is still locked, return.
                            if self.mutex.locked.load(Ordering::SeqCst) {
                                return Poll::Pending;
                            }
                        }
                    }
                }
            }
        }

        impl<T: ?Sized> Drop for LockPopFuture<'_, T> {
            fn drop(&mut self) {
                // If the current task is still in the set, that means it is being cancelled now.
                if let Some(key) = self.opt_key {
                    self.mutex.wakers_pop.cancel(key);
                }
            }
        }

        LockPopFuture {
            mutex: self,
            opt_key: None,
        }.await
    }

    #[inline]
    pub fn try_lock_push(&self) -> Option<MutexQueuePrioPushGuard<'_, T>> {
        if !self.locked.swap(true, Ordering::SeqCst) {
            Some(MutexQueuePrioPushGuard(self))
        } else {
            None
        }
    }

    #[inline]
    pub fn try_lock_pop(&self) -> Option<MutexQueuePrioPopGuard<'_, T>> {
        if !self.locked.swap(true, Ordering::SeqCst) {
            Some(MutexQueuePrioPopGuard(self))
        } else {
            None
        }
    }
}


/// A push guard that releases the lock when dropped.
pub struct MutexQueuePrioPushGuard<'a, T: ?Sized>(&'a MutexQueuePrio<T>);

unsafe impl<T: ?Sized + Send> Send for MutexQueuePrioPushGuard<'_, T> {}
unsafe impl<T: ?Sized + Sync> Sync for MutexQueuePrioPushGuard<'_, T> {}

impl<T: ?Sized> Drop for MutexQueuePrioPushGuard<'_, T> {
    fn drop(&mut self) {
        // Load the mask
        let mask = self.0.mask.load(Ordering::Acquire);

        // Use `SeqCst` ordering to synchronize with `WakerSet::insert()` and `WakerSet::update()`.
        self.0.locked.store(false, Ordering::SeqCst);

        // Notify a blocked `lock()` operation if none were notified already.
        if mask & self.0.l_filter != 0 {
            // If a pop is notified, return
            if self.0.wakers_pop.notify_any() {
                return
            }
        }

        // Wake up a push if there are no messages in the queue o no pop exists
        for priority in 0..self.0.wakers_push.len() {
            // Wake up a pending push if the queue is not full
            if mask & (H_BIT >> priority) == 0 {
                if self.0.wakers_push[priority].notify_any() {
                    break
                }
            }
        }
    }
}

impl<T: ?Sized> Deref for MutexQueuePrioPushGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &*self.0.value.get() }
    }
}

impl<T: ?Sized> DerefMut for MutexQueuePrioPushGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.0.value.get() }
    }
}


/// A pop guard that releases the lock when dropped.
pub struct MutexQueuePrioPopGuard<'a, T: ?Sized>(&'a MutexQueuePrio<T>);

unsafe impl<T: ?Sized + Send> Send for MutexQueuePrioPopGuard<'_, T> {}
unsafe impl<T: ?Sized + Sync> Sync for MutexQueuePrioPopGuard<'_, T> {}

impl<T: ?Sized> Drop for MutexQueuePrioPopGuard<'_, T> {
    fn drop(&mut self) {
        // Load the mask
        let mask = self.0.mask.load(Ordering::Acquire);

        // Use `SeqCst` ordering to synchronize with `WakerSet::insert()` and `WakerSet::update()`.
        self.0.locked.store(false, Ordering::SeqCst);

        // Notify a blocked `lock()` operation if none were notified already.
        for priority in 0..self.0.wakers_push.len() {
            // Wake up a pending push if the queue is not full
            if mask & (H_BIT >> priority) == 0 {
                if self.0.wakers_push[priority].notify_any() {
                    return
                }
            }
        }

        // Notify a pop operation if no push operations were pending
        self.0.wakers_pop.notify_any();
    }
}

impl<T: ?Sized> Deref for MutexQueuePrioPopGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &*self.0.value.get() }
    }
}

impl<T: ?Sized> DerefMut for MutexQueuePrioPopGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.0.value.get() }
    }
}