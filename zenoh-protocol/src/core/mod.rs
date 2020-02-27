use std::convert::From;
use uuid::Uuid;
use std::fmt;

mod zerror;
pub use zerror::*;

pub mod rname;


pub type ZResult<T> = Result<T, ZError>;

pub type ZInt = u64;
pub const ZINT_MAX_BYTES : usize = 10;

///  7 6 5 4 3 2 1 0
/// +-+-+-+-+-+-+-+-+
/// ~      id       — if ResName{name} : id=0
/// +-+-+-+-+-+-+-+-+
/// ~  name/suffix  ~ if flag C!=1 in Message's header
/// +---------------+
///
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum ResKey {
  ResId { id: ZInt},
  ResName { name: String},
  ResGenId{ id: ZInt, suffix: String},  // id cannot be 0 in this case
}

impl From<ZInt> for ResKey {
  fn from(id: ZInt) -> ResKey {
    ResKey::ResId { id }
  }
}

impl From<String> for ResKey {
  fn from(name: String) -> ResKey {
    ResKey::ResName { name }
  }
}

impl From<(ZInt, String)> for ResKey {
  fn from((id, suffix): (ZInt, String)) -> ResKey {
    if id == 0 { panic!("For a ResKey::ResGenId id cannot be 0") }
    ResKey::ResGenId { id, suffix }
  }
}

impl ResKey {
  pub fn is_numerical(&self) -> bool {
    match self {
      ResKey::ResId {id: _} => true,
      _ => false
    }
  }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Property {
    pub key:   ZInt,
    pub value: Vec<u8>
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct PeerId {
    pub id: Vec<u8>
}

#[derive(Debug, Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TimeStamp {
  pub time: u64,
  pub id: Uuid
}

impl fmt::Display for TimeStamp {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
      write!(f, "{}/{}", self.time, self.id)
  }
}
