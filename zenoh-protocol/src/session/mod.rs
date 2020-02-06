mod link;
mod manager;
mod queue;
mod session;

pub use session::Session;
pub use manager::SessionManager;
pub(crate) use queue::Queue;
pub(crate) use queue::QueueError;

use async_std::sync::Arc;
use async_trait::async_trait;

use crate::proto::Locator;
use crate::proto::Message;

/*************************************/
/*              LINK                 */
/*************************************/
#[async_trait]
pub trait Link {
    async fn close(&self) -> async_std::io::Result<()>;

    fn get_mtu(&self) -> usize;

    fn get_locator(&self) -> Locator;

    fn is_ordered(&self) -> bool;

    fn is_reliable(&self) -> bool;

    async fn send(&self, msg: Arc<Message>) -> async_std::io::Result<()>;

    async fn set_session(&self, session: Arc<Session>);
}

/*************************************/
/*          LINK MANAGER             */
/*************************************/
#[async_trait]
pub trait LinkManager {
    async fn new_link(&self, locator: &Locator) -> async_std::io::Result<()>;

    async fn del_link(&self, locator: &Locator) -> Option<Arc<dyn Link + Send + Sync>>;

    async fn new_listener(&self, locator: &Locator) -> async_std::io::Result<()>;

    async fn del_listener(&self, locator: &Locator) -> async_std::io::Result<()>;
}

/*********************************************************/
/* Session Callback to be implemented by the Upper Layer */
/*********************************************************/
#[async_trait]
pub trait SessionCallback {
    async fn receive_message(&self, msg: Message) -> async_std::io::Result<()>;

    async fn new_session(&self, session: Arc<Session>);
}