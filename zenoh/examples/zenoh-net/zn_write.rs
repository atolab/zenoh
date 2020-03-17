use std::env;
use zenoh::net::*;
use zenoh::net::ResKey::*;

fn main() {
    let mut args: Vec<String> = env::args().collect();

    args.pop(); // ignore arg[0] (exe name)
    let uri     = args.pop().unwrap_or("/demo/example/zenoh-rs-write".to_string());
    let value   = args.pop().unwrap_or("Write from Rust!".to_string());
    let locator = args.pop().unwrap_or("".to_string());

    println!("Openning session...");
    let mut session = open(&locator, None).unwrap();

    println!("Writing Data ('{}': '{}')...\n", uri, value);
    session.write(&RName(uri), value.as_bytes()).unwrap();

    session.close().unwrap();
}
