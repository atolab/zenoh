use std::env;
use std::io::Read;
use zenoh::net::*;
use zenoh::net::ResourceKey::*;

fn data_handler(res_name: &str, payload: &[u8], data_info: &[u8]) {
    println!("FUNCTION >> [Subscription listener] Received ('{}': '{:02x?}')", res_name, payload);
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let uri     = args.get(1).map_or("/demo/example/**", |s| &s);
    let locator = args.get(2).map_or("", |s| &s);

    println!("Openning session...");
    let session = open(locator, None).unwrap();

    println!("Declaring Subscriber on {}", uri);

    let sub = session.declare_subscriber(&RName(uri), SubMode::Push, data_handler).unwrap();

    let sub2 = session.declare_subscriber(&RName(uri), SubMode::Push,
        |res_name: &str, payload: &[u8], data_info: &[u8]| {
            println!("CLOSURE >> [Subscription listener] Received ('{}': '{:02x?}')", res_name, payload);
        }
    ).unwrap();

    let mut reader = std::io::stdin();
    let mut input = [0u8];
    while input[0] != 'q' as u8 {
        reader.read_exact(&mut input).unwrap();
    }

    session.undeclare_subscriber(sub).unwrap();
    session.undeclare_subscriber(sub2).unwrap();
    session.close().unwrap();
}
