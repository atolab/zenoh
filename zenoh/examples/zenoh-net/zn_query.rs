use std::env;
use std::io::Read;
use zenoh::net::*;
use zenoh::net::ResourceKey::*;


fn main() {
    let args: Vec<String> = env::args().collect();

    let uri     = args.get(1).map_or("/demo/example/**", |s| &s);
    let locator = args.get(2).map_or("", |s| &s);

    println!("Openning session...");
    let session = open(locator, None).unwrap();

    let replies_handler = move |res_name: &str, payload: &[u8], data_info: &[u8]| {
        println!(">> [Reply handler] received something... {} : {:02x?}'", res_name, payload);
    };

    println!("Sending Query '{}'...", uri);
    let eval = session.query(&RName(uri), "", replies_handler).unwrap();

    session.close().unwrap();
}
