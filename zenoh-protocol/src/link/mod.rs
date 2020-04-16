mod tcp;
pub use tcp::{
    LinkTcp,
    ManagerTcp
};

// mod udp;
// pub(in super) use udp::ManagerUdp;

use async_std::net::SocketAddr;
use async_std::sync::Arc;

use crate::zerror;
use crate::core::{
    ZError,
    ZErrorKind,
    ZResult
};
use crate::io::RBuf;
use crate::session::{
    SessionManagerInner,
    Transport
};

use std::cmp::PartialEq;
use std::fmt;
use std::hash::Hash;
use std::str::FromStr;


/*************************************/
/*          LOCATOR                  */
/*************************************/
const STR_TCP: &str = "tcp";
// const STR_UDP: &str = "udp";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LocatorProtocol {
    Tcp,
    // Udp
}

impl fmt::Display for LocatorProtocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LocatorProtocol::Tcp => write!(f, "{}", STR_TCP)?,
            // LocatorProtocol::Udp => write!(f, "{}", STR_UDP)?,
        };
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Locator {
    Tcp(SocketAddr),
    // Udp(SocketAddr)
}

impl FromStr for Locator {
    type Err = ZError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut iter = s.split('/');
        let proto = iter.next().unwrap();
        let addr = iter.next().unwrap();
        match proto {
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
        match self {
            Locator::Tcp(addr) => write!(f, "{}/{}", STR_TCP, addr)?,
            // Locator::Udp(addr) => write!(f, "{}/{}", STR_UDP, addr)?,
        }
        Ok(())
    }
}

impl Locator {
    pub fn get_proto(&self) -> LocatorProtocol {
        match self {
            Locator::Tcp(..) => LocatorProtocol::Tcp,
            // Locator::Udp(..) => LocatorProtocol::Udp
        }
    }
}


/*************************************/
/*              LINK                 */
/*************************************/
pub enum Link {
    Tcp(Arc<LinkTcp>)
}

impl Link {
    pub async fn close(&self) -> ZResult<()> {
        match self {
            Self::Tcp(link) => link.close().await
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

    pub async fn send(&self, buffer: RBuf) -> ZResult<()> {
        match self {
            Self::Tcp(link) => link.send(buffer).await
        }
    }

    pub fn start(&self) {
        match self {
            Self::Tcp(link) => LinkTcp::start(link.clone())
        }
    } 

    pub async fn stop(&self) -> ZResult<()> {
        match self {
            Self::Tcp(link) => link.stop().await
        }
    }
}

impl Clone for Link {
    fn clone(&self) -> Self {
        match self {
            Self::Tcp(link) => Link::Tcp(link.clone())
        }
    }
}

impl PartialEq for Link {
    fn eq(&self, other: &Self) -> bool {
        self.get_src() ==  other.get_src() && self.get_dst() == self.get_dst()
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

    pub async fn del_link(&self, src: &Locator, dst: &Locator) -> ZResult<Link> {
        match self {
            Self::Tcp(manager) => manager.del_link(src, dst).await
        }
    }

    pub async fn get_link(&self, src: &Locator, dst: &Locator) -> ZResult<Link> {
        match self {
            Self::Tcp(manager) => manager.get_link(src, dst).await
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