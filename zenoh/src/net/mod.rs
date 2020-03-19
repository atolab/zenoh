use zenoh_protocol::core;

mod types;
pub use types::*;

mod consts;
pub use consts::*;

mod session;
pub use session::*;

pub use zenoh_protocol::core::ResourceId;
pub use zenoh_protocol::core::ResKey;

pub const LOCATOR_AUTO: &str = "auto";

pub fn rname_intersect(s1: &str, s2: &str) -> bool {
    core::rname::intersect(s1, s2)
}

pub fn scout(iface: &str, tries: usize, period: usize) -> Vec<String> {
    // @TODO: implement
    println!("---- SCOUT via {} : {} tries each {} second", iface, tries, period);
    vec![]
}

pub fn open(locator: &str, ps: Option<Properties>) -> ZResult<Session> {
    // @TODO: implement
    println!("---- OPEN to \"{}\"", locator);
    Ok(Session::new(locator, ps))
}

