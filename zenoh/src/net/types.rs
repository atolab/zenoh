use std::fmt;
use std::collections::HashMap;

pub type ZInt = zenoh_protocol::core::ZInt;
pub type ZError = zenoh_protocol::core::ZError;
pub type ZResult<T> = zenoh_protocol::core::ZResult<T>;
pub type SubMode = zenoh_protocol::proto::SubMode;

pub type Properties = HashMap<ZInt, Vec<u8>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceId {
    pub(crate) id: ZInt
}

impl fmt::Display for ResourceId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.id)
    }
}

pub const NO_RESOURCE_ID: ResourceId = ResourceId { id: 0 };


#[derive(Debug)]
pub enum ResourceKey<'a> {
    RName(&'a str),
    RId(&'a ResourceId),
    RIdWithSuffix(&'a ResourceId, &'a str),
}
use ResourceKey::*;

impl<'a> ResourceKey<'a> {
    pub fn rid(&self) -> &ResourceId {
        match self {
            RName(_) => &NO_RESOURCE_ID,
            RId(rid) | RIdWithSuffix(rid, _) => &rid,
        }
    }
}

impl<'a> fmt::Display for ResourceKey<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RName(name) => write!(f, "{}", name),
            RId(rid) => write!(f, "{}", rid),
            RIdWithSuffix(rid, suffix) => write!(f, "{}+{}", rid, suffix),
        }
    }
}

impl<'a> From<&'a ResourceId> for ResourceKey<'a> {
    fn from(rid: &'a ResourceId) -> ResourceKey<'a> {
        RId(rid)
    }
}

impl<'a> From<&'a str> for ResourceKey<'a> {
    fn from(name: &'a str) -> ResourceKey<'a> {
        RName(name)
    }
}

impl<'a> From<(&'a ResourceId, &'a str)> for ResourceKey<'a> {
    fn from(tuple: (&'a ResourceId, &'a str)) -> ResourceKey<'a> {
        RIdWithSuffix(tuple.0, tuple.1)
    }
}

pub type QueryHandle = ZInt;

pub type DataHandler = dyn FnMut(/*res_name:*/ &str, /*payload:*/ &[u8], /*data_info:*/ &[u8]) + 'static;

pub type QueryHandler = dyn FnMut(/*res_name:*/ &str, /*predicate:*/ &str, /*replies_sender:*/ &RepliesSender, /*query_handle:*/ QueryHandle) + 'static;

pub type RepliesSender = dyn Fn(/*query_handle:*/ QueryHandle, /*replies:*/ Vec<(&str, Vec<u8>)>) + 'static;

pub type RepliesHandler = dyn FnMut(/*res_name:*/ &str, /*payload:*/ &[u8], /*data_info:*/ &[u8]) + 'static;
