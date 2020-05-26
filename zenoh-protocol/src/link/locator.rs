use async_std::net::SocketAddr;

use crate::core::{ZError, ZErrorKind};
use crate::zerror;

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
                    Err(_) => {
                        return Err(zerror!(ZErrorKind::InvalidLocator {
                            descr: format!("Invalid TCP Socket Address format: {}", addr)
                        }))
                    }
                };
                Ok(Locator::Tcp(addr))
            }
            // STR_UDP => {
            //     let addr: SocketAddr = match addr.parse() {
            //         Ok(addr) => addr,
            //         Err(_) => return Err(zerror!(ZErrorKind::InvalidLocator{
            //             descr: format!("Invalid UDP Socket Address format: {}", addr)
            //         }))
            //     };
            //     return Ok(Locator::Udp { addr })
            // },
            _ => Err(zerror!(ZErrorKind::InvalidLocator {
                descr: format!("Invalid protocol: {}", proto)
            })),
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