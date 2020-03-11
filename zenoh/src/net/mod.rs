use zenoh_protocol::core;

mod types;
pub use types::*;

mod consts;
pub use consts::*;

mod session;
pub use session::*;


pub const LOCATOR_AUTO: &'static str = "auto";

pub fn rname_intersect(s1: &str, s2: &str) -> bool {
    core::rname::intersect(s1, s2)
}

pub fn scout(iface: &str, tries: usize, period: usize) -> Vec<String> {
    // @TODO: implement
    println!("---- SCOUT via {} : {} tries each {} second", iface, tries, period);
    vec![]
}

pub fn open(locator: &str, _ps: Option<Properties>) -> ZResult<Session> {
    // @TODO: implement
    println!("---- OPEN to \"{}\"", locator);

    let mut info = Properties::new();
    info.insert(ZN_INFO_PEER_KEY, locator.as_bytes().to_vec());
    info.insert(ZN_INFO_PID_KEY, vec![1u8, 2, 3]);
    info.insert(ZN_INFO_PEER_PID_KEY, vec![4u8, 5, 6]);

    Ok(Session::new(info))
}

