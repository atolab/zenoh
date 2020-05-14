mod channel;
mod defaults;
mod initial;
mod manager;

pub use manager::*;

use channel::*;
use initial::*;

use async_std::sync::Arc;
use async_trait::async_trait;

use crate::core::ZResult;
use crate::link::Link;
use crate::proto::{SessionMessage, WhatAmI, ZenohMessage};

/*********************************************************/
/*           Trait for implementing a transport          */
/*********************************************************/
pub type Transport = Arc<dyn TransportTrait + Send + Sync>;

#[async_trait]
pub trait TransportTrait {
    async fn receive_message(&self, link: &Link, msg: SessionMessage) -> Action;
    async fn link_err(&self, link: &Link);
}

pub enum Action {
    ChangeTransport(Transport),
    Close,
    Read
}

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
