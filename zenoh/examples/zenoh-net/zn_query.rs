use std::env;
use zenoh::net::*;
use zenoh::net::ResKey::*;


fn main() {
    let mut args: Vec<String> = env::args().collect();

    let mut options = args.drain(1..);
    let uri     = options.next().unwrap_or("/demo/example/**".to_string());
    let locator = options.next().unwrap_or("".to_string());

    println!("Openning session...");
    let session = open(&locator, None).unwrap();

    let replies_handler = move |res_name: &str, payload: &[u8], _data_info: &[u8]| {
        println!(">> [Reply handler] received something... {} : {:02x?}'", res_name, payload);
    };

    println!("Sending Query '{}'...", uri);
    let _eval = session.query(&RName(uri), "", replies_handler).unwrap();

    session.close().unwrap();
}
