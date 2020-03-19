use std::fmt;
use std::collections::HashMap;
use async_std::sync::Arc;
use spin::RwLock;
use super::InnerSession;

pub type ZInt = zenoh_protocol::core::ZInt;
pub type ZError = zenoh_protocol::core::ZError;
pub type ZResult<T> = zenoh_protocol::core::ZResult<T>;

pub type Reliability = zenoh_protocol::proto::Reliability;
pub type SubMode     = zenoh_protocol::proto::SubMode;
pub type Period      = zenoh_protocol::proto::Period;
pub type SubInfo     = zenoh_protocol::proto::SubInfo;

pub type Properties = HashMap<ZInt, Vec<u8>>;

pub type QueryHandle = ZInt;

pub type DataHandler = dyn FnMut(/*res_name:*/ &str, /*payload:*/ &[u8], /*data_info:*/ &[u8]) + Send + Sync + 'static;

pub type QueryHandler = dyn FnMut(/*res_name:*/ &str, /*predicate:*/ &str, /*replies_sender:*/ &RepliesSender, /*query_handle:*/ QueryHandle) + Send + Sync + 'static;

pub type RepliesSender = dyn Fn(/*query_handle:*/ QueryHandle, /*replies:*/ Vec<(&str, Vec<u8>)>) + Send + Sync + 'static;

pub type RepliesHandler = dyn FnMut(/*res_name:*/ &str, /*payload:*/ &[u8], /*data_info:*/ &[u8]) + Send + Sync + 'static;


pub(crate) type Id = usize;

#[derive(Clone)]
pub struct Publisher {
    pub(crate) id: Id,
}

impl PartialEq for Publisher {
    fn eq(&self, other: &Publisher) -> bool {
        self.id == other.id
    }
}

impl fmt::Debug for Publisher {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Publisher{{ id:{} }}", self.id)
    }
}


#[derive(Clone)]
pub struct Subscriber {
    pub(crate) id: Id,
    pub(crate) resname: String,
    pub(crate) dhandler: Arc<RwLock<DataHandler>>,
    pub(crate) session: Arc<RwLock<InnerSession>>
}

impl Subscriber {
    pub fn pull(&self) -> ZResult<()> {
        // @TODO: implement
        println!("---- PULL on {:?}", self);
        Ok(())
    }
}

impl PartialEq for Subscriber {
    fn eq(&self, other: &Subscriber) -> bool {
        self.id == other.id
    }
}

impl fmt::Debug for Subscriber {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Subscriber{{ id:{}, resname:{} }}", self.id, self.resname)
    }
}

#[derive(Clone)]
pub struct Storage {
    pub(crate) id: Id,
    pub(crate) dhandler: Arc<RwLock<DataHandler>>,
    pub(crate) qhandler: Arc<RwLock<QueryHandler>>,
}

impl PartialEq for Storage {
    fn eq(&self, other: &Storage) -> bool {
        self.id == other.id
    }
}

impl fmt::Debug for Storage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Storage{{ id:{} }}", self.id)
    }
}


#[derive(Clone)]
pub struct Eval {
    pub(crate) id: Id,
    pub(crate) qhandler: Arc<RwLock<QueryHandler>>,
}

impl PartialEq for Eval {
    fn eq(&self, other: &Eval) -> bool {
        self.id == other.id
    }
}

impl fmt::Debug for Eval {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Eval{{ id:{} }}", self.id)
    }
}
