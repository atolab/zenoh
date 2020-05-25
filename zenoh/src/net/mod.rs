use zenoh_protocol::core;

mod types;
pub use types::*;

mod consts;
pub use consts::*;

#[macro_use]
mod session;
pub use session::*;

pub const LOCATOR_AUTO: &str = "auto";

pub fn rname_intersect(s1: &str, s2: &str) -> bool {
    core::rname::intersect(s1, s2)
}

pub async fn scout(_iface: &str, _tries: usize, _period: usize) -> Vec<String> {
    // @TODO: implement
    apitrace!(">>>> scout({:?}, {:?}, {:?})", _iface, _tries, _period);
    vec![]
}

pub async fn open(locator: &str, ps: Option<Properties>) -> ZResult<Session> {
    // @TODO: implement
    apitrace!(">>>> open({:?}, {:?})", locator, ps);
    Ok(Session::new(locator, ps).await)
}

