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

use crate::core::{
    PeerId,
    ZError,
    ZResult
};
use crate::proto::Locator;
use crate::proto::Message;

/*************************************/
/*              LINK                 */
/*************************************/
#[async_trait]
pub trait Link {
    async fn close(&self, reason: Option<ZError>) -> ZResult<()>;

    fn get_mtu(&self) -> usize;

    fn get_src(&self) -> Locator;

    fn get_dst(&self) -> Locator;

    fn is_ordered(&self) -> bool;

    fn is_reliable(&self) -> bool;

    async fn send(&self, msg: Arc<Message>) -> ZResult<()>;

    async fn start(&self) -> ZResult<()>;

    async fn stop(&self) -> ZResult<()>;
}

/*************************************/
/*          LINK MANAGER             */
/*************************************/
#[async_trait]
pub trait LinkManager {
    async fn new_link(&self, dst: &Locator, session: Arc<Session>) -> ZResult<Arc<dyn Link + Send + Sync>>;

    async fn del_link(&self, src: &Locator, dst: &Locator, reason: Option<ZError>) -> ZResult<Arc<dyn Link + Send + Sync>>;

    async fn move_link(&self, src: &Locator, dst: &Locator, session: Arc<Session>) -> ZResult<()>;

    async fn new_listener(&self, locator: &Locator) -> ZResult<()>;

    async fn del_listener(&self, locator: &Locator) -> ZResult<()>;

    async fn get_listeners(&self) -> Vec<Locator>;
}

/*********************************************************/
/* Session Callback to be implemented by the Upper Layer */
/*********************************************************/
#[async_trait]
pub trait SessionCallback {
    async fn receive_message(&self, msg: Message) -> Result<(), ZError>;
}

#[async_trait]
pub trait SessionHandler {
    async fn new_session(&self, peer: &PeerId) -> Arc<dyn SessionCallback + Send + Sync>;

    async fn del_session(&self, session: &Arc<Session>);
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
    async fn receive_message(&self, _message: Message) -> Result<(), ZError> {
        Ok(())
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