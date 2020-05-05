mod locator;
pub use locator::*;

mod manager;
pub use manager::*;

/* Import of Link modules */
mod tcp;

/* General imports */
use async_std::sync::Arc;
use async_trait::async_trait;

use crate::core::ZResult;
use crate::io::RBuf;
use crate::session::Transport;

use std::cmp::PartialEq;
use std::fmt;
use std::hash::{Hash, Hasher};


/*************************************/
/*              LINK                 */
/*************************************/
// pub enum Link {
//     Tcp(Arc<LinkTcp>),
// }

pub type Link = Arc<dyn LinkTrait + Send + Sync>;

#[async_trait]
pub trait LinkTrait {
    async fn close(&self) -> ZResult<()>;

    fn get_mtu(&self) -> usize;

    fn get_src(&self) -> Locator;

    fn get_dst(&self) -> Locator;

    fn is_ordered(&self) -> bool;

    fn is_reliable(&self) -> bool;

    async fn send(&self, buffer: RBuf) -> ZResult<()>;

    async fn start(&self) -> ZResult<()>;

    async fn stop(&self) -> ZResult<()>;
}

impl Eq for dyn LinkTrait + Send + Sync {}

impl PartialEq for dyn LinkTrait + Send + Sync {
    fn eq(&self, other: &Self) -> bool {
        self.get_src() == other.get_src() && self.get_dst() == other.get_dst()
    }
}

impl Hash for dyn LinkTrait + Send + Sync {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.get_src().hash(state);
        self.get_dst().hash(state);
    }
}

impl fmt::Display for dyn LinkTrait + Send + Sync {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} => {}", self.get_src(), self.get_dst())?;
        Ok(())
    }
}

/*************************************/
/*           LINK MANAGER            */
/*************************************/
pub type LinkManager = Arc<dyn ManagerTrait + Send + Sync>;

#[async_trait]
pub trait ManagerTrait {
    async fn new_link(&self, dst: &Locator, transport: Arc<Transport>) -> ZResult<Link>;

    async fn del_link(&self, src: &Locator, dst: &Locator) -> ZResult<Link>;

    async fn get_link(&self, src: &Locator, dst: &Locator) -> ZResult<Link>;

    async fn new_listener(&self, locator: &Locator) -> ZResult<()>;

    async fn del_listener(&self, locator: &Locator) -> ZResult<()>;

    async fn get_listeners(&self) -> Vec<Locator>;
}
