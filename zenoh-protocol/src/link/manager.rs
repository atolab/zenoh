use async_std::sync::Arc;

use crate::link::{LinkManager, LocatorProtocol};
use crate::link::tcp::ManagerTcp;
use crate::session::SessionManagerInner;


pub struct LinkManagerBuilder;

impl LinkManagerBuilder {
    pub(crate) fn make(manager: Arc<SessionManagerInner>, protocol: &LocatorProtocol) -> LinkManager {
        match protocol {
            LocatorProtocol::Tcp => Arc::new(ManagerTcp::new(manager)),
            // LocatorProtocol::Udp => write!(f, "{}", STR_UDP)?,
        }
    }
}