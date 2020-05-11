mod conduit;
mod defaults;
mod manager;
mod transport;

pub use manager::*;

pub(crate) use conduit::*;
pub(crate) use transport::*;

use async_std::sync::Arc;
use async_trait::async_trait;

use crate::core::ZResult;
use crate::proto::{ZenohMessage, WhatAmI};

/*********************************************************/
/* Session Callback to be implemented by the Upper Layer */
/*********************************************************/
#[async_trait]
pub trait MsgHandler {
    async fn handle_message(&self, msg: ZenohMessage) -> ZResult<()>;
    async fn close(&self);
}

#[async_trait]
pub trait SessionHandler {
    async fn new_session(
        &self,
        whatami: WhatAmI,
        session: Arc<dyn MsgHandler + Send + Sync>,
    ) -> Arc<dyn MsgHandler + Send + Sync>;
}

// Define an empty SessionCallback for the listener session
#[derive(Default)]
pub struct DummyHandler;

impl DummyHandler {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl MsgHandler for DummyHandler {
    async fn handle_message(&self, _message: ZenohMessage) -> ZResult<()> {
        Ok(())
    }
    async fn close(&self) {}
}
