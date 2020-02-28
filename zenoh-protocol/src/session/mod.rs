mod link;
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

use crate::core::{
    PeerId,
    ZResult
};
use crate::proto::Message;


/*********************************************************/
/* Session Callback to be implemented by the Upper Layer */
/*********************************************************/
#[async_trait]
pub trait SessionCallback {
    async fn receive_message(&self, msg: Message) -> ZResult<()>;
}

#[async_trait]
pub trait SessionHandler {
    async fn get_callback(&self, peer: &PeerId) -> Arc<dyn SessionCallback + Send + Sync>;

    async fn new_session(&self, session: Arc<Session>);

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
    async fn receive_message(&self, _message: Message) -> ZResult<()> {
        Ok(())
    }
}

#[macro_export]
macro_rules! zrwopt {
    ($kind:expr) => ($kind.try_read().unwrap().as_ref().unwrap());
}