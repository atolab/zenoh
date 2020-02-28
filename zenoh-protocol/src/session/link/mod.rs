mod tcp;
use tcp::{
    LinkTcp,
    ManagerTcp
};

// mod udp;
// pub(in super) use udp::ManagerUdp;

use async_std::sync::Arc;

use crate::core::{
    ZError,
    ZResult
};
use crate::proto::{
    Locator,
    LocatorProtocol,
    Message
};
use crate::session::{
    SessionManagerInner,
    Transport
};


/*************************************/
/*              LINK                 */
/*************************************/
pub enum Link {
    Tcp(Arc<LinkTcp>)
}

impl Link {
    pub async fn close(&self, reason: Option<ZError>) -> ZResult<()> {
        match self {
            Self::Tcp(link) => link.close(reason).await
        }
    }

    pub fn get_mtu(&self) -> usize {
        match self {
            Self::Tcp(link) => link.get_mtu()
        }
    }

    pub fn get_src(&self) -> Locator {
        match self {
            Self::Tcp(link) => link.get_src()
        }
    }

    pub fn get_dst(&self) -> Locator {
        match self {
            Self::Tcp(link) => link.get_dst()
        }
    }

    pub fn is_ordered(&self) -> bool {
        match self {
            Self::Tcp(link) => link.is_ordered()
        }
    }

    pub fn is_reliable(&self) -> bool {
        match self {
            Self::Tcp(link) => link.is_reliable()
        }
    }

    pub async fn send(&self, msg: &Arc<Message>) -> ZResult<()> {
        match self {
            Self::Tcp(link) => link.send(msg).await
        }
    }

    pub async fn start(&self) -> ZResult<()> {
        // match self {
        //     Self::Tcp(link) => link.start().await
        // }
        Ok(())
    }

    pub async fn stop(&self) -> ZResult<()> {
        match self {
            Self::Tcp(link) => link.stop().await
        }
    }
}

/*************************************/
/*           LINK MANAGER            */
/*************************************/
pub enum LinkManager {
    Tcp(ManagerTcp)
}

impl LinkManager {
    pub fn new(manager: Arc<SessionManagerInner>, protocol: &LocatorProtocol) -> Self {
        match protocol {
            LocatorProtocol::Tcp => LinkManager::Tcp(ManagerTcp::new(manager)),
        }
    }

    pub async fn new_link(&self, dst: &Locator, transport: Arc<Transport>) -> ZResult<Link> {
        match self {
            Self::Tcp(manager) => manager.new_link(dst, transport).await
        }
    }

    pub async fn del_link(&self, src: &Locator, dst: &Locator, reason: Option<ZError>) -> ZResult<Link> {
        match self {
            Self::Tcp(manager) => manager.del_link(src, dst, reason).await
        }
    }

    pub async fn move_link(&self, src: &Locator, dst: &Locator, transport: Arc<Transport>) -> ZResult<()> {
        match self {
            Self::Tcp(manager) => manager.move_link(src, dst, transport).await
        }
    }

    pub async fn new_listener(&self, locator: &Locator) -> ZResult<()> {
        match self {
            Self::Tcp(manager) => manager.new_listener(locator).await
        } 
    }

    pub async fn del_listener(&self, locator: &Locator) -> ZResult<()> {
        match self {
            Self::Tcp(manager) => manager.del_listener(locator).await
        } 
    }
    pub async fn get_listeners(&self) -> Vec<Locator> {
        match self {
            Self::Tcp(manager) => manager.get_listeners().await
        } 
    }
}