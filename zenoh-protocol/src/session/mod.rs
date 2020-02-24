mod link;
mod manager;
mod queue;
mod session;

pub use session::Session;
pub use manager::SessionManager;
pub(crate) use queue::Queue;
pub(crate) use queue::QueueError;

use async_std::sync::{
    Arc,
    Weak
};
use async_trait::async_trait;
use std::cell::UnsafeCell;

use crate::core::ZError;
use crate::proto::Locator;
use crate::proto::Message;

/*************************************/
/*              LINK                 */
/*************************************/
#[async_trait]
pub trait Link {
    async fn close(&self, reason: Option<ZError>) -> Result<(), ZError>;

    fn get_mtu(&self) -> usize;

    fn get_src(&self) -> Locator;

    fn get_dst(&self) -> Locator;

    fn is_ordered(&self) -> bool;

    fn is_reliable(&self) -> bool;

    async fn send(&self, msg: Arc<Message>) -> Result<(), ZError>;

    async fn set_session(&self, session: Arc<Session>) -> Result<(), ZError>;
}

/*************************************/
/*          LINK MANAGER             */
/*************************************/
#[async_trait]
pub trait LinkManager {
    async fn new_link(&self, dst: &Locator, session: Arc<Session>) -> Result<Arc<dyn Link + Send + Sync>, ZError>;

    async fn del_link(&self, src: &Locator, dst: &Locator, reason: Option<ZError>) -> Result<Arc<dyn Link + Send + Sync>, ZError>;

    async fn new_listener(&self, locator: &Locator, limit: Option<usize>) -> Result<(), ZError>;

    async fn del_listener(&self, locator: &Locator) -> Result<(), ZError>;

    async fn get_listeners(&self) -> Vec<Locator>;
}

/*********************************************************/
/* Session Callback to be implemented by the Upper Layer */
/*********************************************************/
#[async_trait]
pub trait SessionCallback {
    async fn receive_message(&self, session: Arc<Session>, msg: Message) -> Result<(), ZError>;

    async fn new_session(&self, session: Arc<Session>);
}

// Define an empty SessionCallback for the listener session
pub struct EmptyCallback {}

impl EmptyCallback {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl SessionCallback for EmptyCallback {
    async fn receive_message(&self, _session: Arc<Session>, _message: Message) -> Result<(), ZError> {
        Ok(())
    }

    async fn new_session(&self, _session: Arc<Session>) {
    }
}


/*********************************************************/
/*                     ArcSelf                           */
/*********************************************************/
pub struct ArcSelf<T> {
    inner: UnsafeCell<Weak<T>>
}

impl<T> ArcSelf<T> {
    fn new() -> Self {
        Self {
            inner: UnsafeCell::new(Weak::new())
        }
    }

    fn set(&self, t: &Arc<T>) {
        unsafe {
            *self.inner.get() = Arc::downgrade(t)
        }
    }

    fn get(&self) -> Arc<T> {
        if let Some(a_self) = unsafe { &*self.inner.get() }.upgrade() {
            return a_self.clone()
        }
        panic!("Object not intiliazed!!!");
    }
}

unsafe impl<T> Send for ArcSelf<T> {}
unsafe impl<T> Sync for ArcSelf<T> {}