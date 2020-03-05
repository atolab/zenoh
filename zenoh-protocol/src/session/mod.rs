mod manager;
mod queue;
mod transport;

pub use transport::Transport;
pub use manager::{
    SessionManager,
    SessionManagerInner,
    Session
};

use async_std::sync::Arc;
use async_trait::async_trait;

use crate::core::ZResult;
use crate::proto::Message;


/*********************************************************/
/* Session Callback to be implemented by the Upper Layer */
/*********************************************************/
#[async_trait]
pub trait MsgHandler {
    // async fn id(&self) -> usize;
    async fn handle_message(&self, msg: Message) -> ZResult<()>;
}

#[async_trait]
pub trait SessionHandler {
    async fn new_session(&self, session: Arc<dyn MsgHandler + Send + Sync>) -> Arc<dyn MsgHandler + Send + Sync>;
    async fn del_session(&self, session: &(dyn MsgHandler + Send + Sync));
}

// Define an empty SessionCallback for the listener session
pub struct DummyHandler {}

impl DummyHandler {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl MsgHandler for DummyHandler {
    async fn handle_message(&self, _message: Message) -> ZResult<()> {
        Ok(())
    }
}

#[macro_export]
macro_rules! zrwopt {
    ($var:expr) => ($var.try_read().unwrap().as_ref().unwrap());
}