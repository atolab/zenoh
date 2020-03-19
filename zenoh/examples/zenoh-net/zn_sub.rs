use std::env;
use std::io::Read;
use zenoh::net::*;
use zenoh::net::ResKey::*;

fn data_handler(res_name: &str, payload: &[u8], _data_info: &[u8]) {
    println!("FUNCTION >> [Subscription listener] Received ('{}': '{:02x?}')", res_name, payload);
}

fn main() {
    let mut args: Vec<String> = env::args().collect();

    let mut options = args.drain(1..);
    let uri     = options.next().unwrap_or("/demo/example/**".to_string());
    let locator = options.next().unwrap_or("".to_string());

    println!("Openning session...");
    let session = open(&locator, None).unwrap();

    println!("Declaring Subscriber on {}", uri);

    let sub = session.declare_subscriber(&RName(uri.clone()), &SubMode::Push, data_handler).unwrap();

    let sub2 = session.declare_subscriber(&RName(uri), &SubMode::Push,
        move |res_name: &str, payload: &[u8], _data_info: &[u8]| {
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
