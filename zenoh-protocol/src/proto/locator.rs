use async_std::net::SocketAddr;
use std::hash::{
    Hash,
    Hasher
};
use std::str::FromStr;

/*************************************/
/*          LOCATOR                  */
/*************************************/
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Locator {
    Tcp(SocketAddr),
    Udp(SocketAddr)
}

impl FromStr for Locator {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut iter = s.split("/");
        let proto = iter.next().unwrap();
        let addr = iter.next().unwrap();
        match proto {
            "tcp" => return Ok(Locator::Tcp(addr.parse().unwrap())),
            "udp" => return Ok(Locator::Udp(addr.parse().unwrap())),
            _ => return Err(())
        }
    }
}

// SocketAddr does not distinguish between TCP and UDP
// We need to differentiate the Locator Hash for TCP and UDP for the same pair of
// IP address and port. The current solution works but it may be slow.
impl Hash for Locator {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Locator::Tcp(addr) => ["tcp/", &addr.to_string()].concat().hash(state),
            Locator::Udp(addr) => ["udp/", &addr.to_string()].concat().hash(state)
        }
    }
}