use async_std::net::SocketAddr;
use std::fmt;
use std::hash::Hash;
use std::str::FromStr;

use crate::core::{
    ZError,
    ZErrorKind
};
use crate::zerror;

/*************************************/
/*          LOCATOR                  */
/*************************************/
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LocatorProtocol {
    Tcp,
    // Udp
}

impl fmt::Display for LocatorProtocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LocatorProtocol::Tcp => write!(f, "TCP")?,
            // LocatorProtocol::Udp => write!(f, "UDP")?,
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
        let mut iter = s.split("/");
        let proto = iter.next().unwrap();
        let addr = iter.next().unwrap();
        match proto {
            "tcp" => {
                let addr: SocketAddr = match addr.parse() {
                    Ok(addr) => addr,
                    Err(_) => return Err(zerror!(ZErrorKind::InvalidLocator{ 
                        reason: format!("Invalid TCP Socket Address format: {}", addr) 
                    }))
                };
                Ok(Locator::Tcp(addr))
            },
            // "udp" => {
            //     let addr: SocketAddr = match addr.parse() {
            //         Ok(addr) => addr,
            //         Err(_) => return Err(zerror!(ZErrorKind::InvalidLocator{ 
            //             reason: format!("Invalid UDP Socket Address format: {}", addr) 
            //         }))
            //     };
            //     return Ok(Locator::Udp { addr })
            // },
            _ => Err(zerror!(ZErrorKind::InvalidLocator{ 
                reason: format!("Invalid protocol: {}", proto) 
            }))
        }
    }
}

impl fmt::Display for Locator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Locator::Tcp(addr) => write!(f, "tcp/{}", addr)?,
            // Locator::Udp(addr) => write!(f, "udp/{}", addr)?,
        };
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