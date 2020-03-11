use std::env;
use zenoh::net::*;
use zenoh::net::ResourceKey::*;

fn main() {
    let args: Vec<String> = env::args().collect();

    let uri     = args.get(1).map_or("/demo/example/zenoh-rs-write", |s| &s);
    let value   = args.get(2).map_or("Write from Rust!", |s| &s);
    let locator = args.get(3).map_or("", |s| &s);

    println!("Openning session...");
    let mut session = open(locator, None).unwrap();

    println!("Writing Data ('{}': '{}')...\n", uri, value);
    session.write(&RName(uri), value.as_bytes()).unwrap();

    session.close().unwrap();
}
