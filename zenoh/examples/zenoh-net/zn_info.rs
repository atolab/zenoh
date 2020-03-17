use std::env;
use zenoh::net::*;

fn main() {
    let mut args: Vec<String> = env::args().collect();

    args.pop(); // ignore arg[0] (exe name)
    let locator = args.pop().unwrap_or("".to_string());
    
    let mut ps = Properties::new();
    ps.insert(ZN_USER_KEY, "user".as_bytes().to_vec());
    ps.insert(ZN_PASSWD_KEY, "password".as_bytes().to_vec());

    println!("Openning session...");
    let session = open(&locator, Some(ps)).unwrap();

    let info = session.info();
    println!("LOCATOR :  \"{}\"", String::from_utf8_lossy(info.get(&ZN_INFO_PEER_KEY).unwrap()));
    println!("PID :      {:02x?}", info.get(&ZN_INFO_PID_KEY).unwrap());
    println!("PEER PID : {:02x?}", info.get(&ZN_INFO_PEER_PID_KEY).unwrap());
}
