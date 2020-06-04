use std::env;
use async_std::task;
use zenoh::net::*;

fn main() {
    // for logging
    env_logger::init();

    task::block_on( async {
        let mut args: Vec<String> = env::args().collect();

        let mut options = args.drain(1..);
        let uri     = options.next().unwrap_or("/demo/example/zenoh-rs-pub".to_string());
        let value   = options.next().unwrap_or("Write from Rust!".to_string());
        let locator = options.next().unwrap_or("".to_string());

        println!("Openning session...");
        let session = open(&locator, None).await.unwrap();
        
        print!("Declaring Resource {} ", uri);
        let rid = session.declare_resource(&uri.into()).await.unwrap();
        println!("=> RId {}", rid);
        
        println!("Declaring Publisher on {}", rid);
        let publ = session.declare_publisher(&rid.into()).await.unwrap();
        
        println!("Writing Data ('{}': '{}')...\n", rid, value);
        session.write(&rid.into(), value.as_bytes().into()).await.unwrap();

        session.undeclare_publisher(publ).await.unwrap();
        session.close().await.unwrap();
    })
}
