mod dummy;
pub use dummy::{
    LinkDummy,
    ManagerDummy
};

mod tcp;
pub use tcp::{
    LinkTcp,
    ManagerTcp
};

// mod udp;
// pub(in super) use udp::ManagerUdp;

use async_std::sync::Arc;

use crate::zerror;
use crate::core::{
    ZError,
    ZErrorKind,
    ZResult
};
use crate::proto::Message;
use crate::session::{
    SessionManagerInner,
    Transport
};

use async_std::net::SocketAddr;
use std::fmt;
use std::hash::Hash;
use std::str::FromStr;


/*************************************/
/*          LOCATOR                  */
/*************************************/
const STR_DUMMY: &str = "dmy";
const STR_TCP: &str = "tcp";
// const STR_UDP: &str = "udp";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LocatorProtocol {
    Dummy,
    Tcp,
    // Udp
}

impl fmt::Display for LocatorProtocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LocatorProtocol::Dummy => write!(f, "{}", STR_DUMMY)?,
            LocatorProtocol::Tcp => write!(f, "{}", STR_TCP)?,
            // LocatorProtocol::Udp => write!(f, "{}", STR_UDP)?,
        };
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Locator {
    Dummy(String),
    Tcp(SocketAddr),
    // Udp(SocketAddr)
}

impl FromStr for Locator {
    type Err = ZError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut iter = s.split("/");
        let proto = iter.next().unwrap();
        let addr = iter.next().unwrap();
        match proto {
            STR_DUMMY => {
                Ok(Locator::Dummy(addr.to_string()))
            },
            STR_TCP => {
                let addr: SocketAddr = match addr.parse() {
                    Ok(addr) => addr,
                    Err(_) => return Err(zerror!(ZErrorKind::InvalidLocator{ 
                        descr: format!("Invalid TCP Socket Address format: {}", addr) 
                    }))
                };
                Ok(Locator::Tcp(addr))
            },
            // STR_UDP => {
            //     let addr: SocketAddr = match addr.parse() {
            //         Ok(addr) => addr,
            //         Err(_) => return Err(zerror!(ZErrorKind::InvalidLocator{ 
            //             descr: format!("Invalid UDP Socket Address format: {}", addr) 
            //         }))
            //     };
            //     return Ok(Locator::Udp { addr })
            // },
            _ => Err(zerror!(ZErrorKind::InvalidLocator{ 
                descr: format!("Invalid protocol: {}", proto) 
            }))
        }
    }
}

impl fmt::Display for Locator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Ok(match self {
            Locator::Dummy(addr) => write!(f, "{}/{}", STR_DUMMY, addr)?,
            Locator::Tcp(addr) => write!(f, "{}/{}", STR_TCP, addr)?,
            // Locator::Udp(addr) => write!(f, "{}/{}", STR_UDP, addr)?,
        })
    }
}

impl Locator {
    pub fn get_proto(&self) -> LocatorProtocol {
        match self {
            Locator::Dummy(..) => LocatorProtocol::Dummy,
            Locator::Tcp(..) => LocatorProtocol::Tcp,
            // Locator::Udp(..) => LocatorProtocol::Udp
        }
    }
}


/*************************************/
/*              LINK                 */
/*************************************/
pub enum Link {
    Dummy(Arc<LinkDummy>),
    Tcp(Arc<LinkTcp>)
}

impl Link {
    pub async fn close(&self, reason: Option<ZError>) -> ZResult<()> {
        match self {
            Self::Dummy(link) => link.close(reason).await,
            Self::Tcp(link) => link.close(reason).await
        }
    }

    pub fn get_mtu(&self) -> usize {
        match self {
            Self::Dummy(link) => link.get_mtu(),
            Self::Tcp(link) => link.get_mtu()
        }
    }

    pub fn get_src(&self) -> Locator {
        match self {
            Self::Dummy(link) => link.get_src(),
            Self::Tcp(link) => link.get_src()
        }
    }

    pub fn get_dst(&self) -> Locator {
        match self {
            Self::Dummy(link) => link.get_dst(),
            Self::Tcp(link) => link.get_dst()
        }
    }

    pub fn is_ordered(&self) -> bool {
        match self {
            Self::Dummy(link) => link.is_ordered(),
            Self::Tcp(link) => link.is_ordered()
        }
    }

    pub fn is_reliable(&self) -> bool {
        match self {
            Self::Dummy(link) => link.is_reliable(),
            Self::Tcp(link) => link.is_reliable()
        }
    }

    pub async fn send(&self, msg: &Message) -> ZResult<()> {
        match self {
            Self::Dummy(link) => link.send(msg).await,
            Self::Tcp(link) => link.send(msg).await
        }
    }

    pub fn start(&self) {
        match self {
            Self::Dummy(link) => LinkDummy::start(link.clone()),
            Self::Tcp(link) => LinkTcp::start(link.clone())
        }
    } 

    pub async fn stop(&self) -> ZResult<()> {
        match self {
            Self::Dummy(link) => link.stop().await,
            Self::Tcp(link) => link.stop().await
        }
    }
}

impl Clone for Link {
    fn clone(&self) -> Self {
        match self {
            Self::Dummy(link) => Link::Dummy(link.clone()),
            Self::Tcp(link) => Link::Tcp(link.clone())
        }
    }
}

/*************************************/
/*           LINK MANAGER            */
/*************************************/
pub enum LinkManager {
    Dummy(ManagerDummy),
    Tcp(ManagerTcp)
}

impl LinkManager {
    pub fn new(manager: Arc<SessionManagerInner>, protocol: &LocatorProtocol) -> Self {
        match protocol {
            LocatorProtocol::Dummy => LinkManager::Dummy(ManagerDummy::new(manager)),
            LocatorProtocol::Tcp => LinkManager::Tcp(ManagerTcp::new(manager)),
        }
    }

    pub async fn new_link(&self, dst: &Locator, transport: Arc<Transport>) -> ZResult<Link> {
        match self {
            Self::Dummy(manager) => manager.new_link(dst, transport).await,
            Self::Tcp(manager) => manager.new_link(dst, transport).await
        }
    }

    pub async fn del_link(&self, src: &Locator, dst: &Locator, reason: Option<ZError>) -> ZResult<Link> {
        match self {
            Self::Dummy(manager) => manager.del_link(src, dst, reason).await,
            Self::Tcp(manager) => manager.del_link(src, dst, reason).await
        }
    }

    pub async fn move_link(&self, src: &Locator, dst: &Locator, transport: Arc<Transport>) -> ZResult<()> {
        match self {
            Self::Dummy(manager) => manager.move_link(src, dst, transport).await,
            Self::Tcp(manager) => manager.move_link(src, dst, transport).await
        }
    }

    pub async fn new_listener(&self, locator: &Locator) -> ZResult<()> {
        match self {
            Self::Dummy(manager) => manager.new_listener(locator).await,
            Self::Tcp(manager) => manager.new_listener(locator).await
        } 
    }

    pub async fn del_listener(&self, locator: &Locator) -> ZResult<()> {
        match self {
            Self::Dummy(manager) => manager.del_listener(locator).await,
            Self::Tcp(manager) => manager.del_listener(locator).await
        } 
    }
    pub async fn get_listeners(&self) -> Vec<Locator> {
        match self {
            Self::Dummy(manager) => manager.get_listeners().await,
            Self::Tcp(manager) => manager.get_listeners().await
        } 
    }
}