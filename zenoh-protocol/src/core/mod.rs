use std::convert::From;
use uuid::Uuid;
use std::fmt;

pub mod rname;

pub type ZInt = u64;
pub const ZINT_MAX_BYTES : usize = 10;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum ResKey {
  ResId { id: ZInt},
  ResName { name: String},
  ResGenId{ id: ZInt, suffix: String},  // Generalised I
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

#[derive(Debug, Clone)]
pub struct Property {
    pub key:   ZInt,
    pub value: Vec<u8>
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
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
