use async_std::net::SocketAddr;
use std::fmt;
use std::hash::{
    Hash,
    Hasher
};
use std::str::FromStr;

use crate::core::{
    ZError,
    ZErrorKind
};
use crate::zerror;

/*************************************/
/*          LOCATOR                  */
/*************************************/
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Locator {
    Tcp { addr: SocketAddr },
    Udp { addr: SocketAddr }
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
                return Ok(Locator::Tcp { addr })
            },
            "udp" => {
                let addr: SocketAddr = match addr.parse() {
                    Ok(addr) => addr,
                    Err(_) => return Err(zerror!(ZErrorKind::InvalidLocator{ 
                        reason: format!("Invalid UDP Socket Address format: {}", addr) 
                    }))
                };
                return Ok(Locator::Udp { addr })
            },
            _ => return Err(zerror!(ZErrorKind::InvalidLocator{ 
                reason: format!("Invalid protocol: {}", proto) 
            }))
        }
    }
}

impl fmt::Display for Locator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Locator::Tcp{ addr } => write!(f, "tcp/{}", addr)?,
            Locator::Udp{ addr } => write!(f, "udp/{}", addr)?,
        };
        Ok(())
    }
}

impl Hash for Locator {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Locator::Tcp { addr } => ("tcp", addr).hash(state),
            Locator::Udp { addr } => ("udp", addr).hash(state)
        }
    }
}