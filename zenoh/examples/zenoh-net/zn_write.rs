use std::env;
use async_std::task;
use zenoh::net::*;
use zenoh::net::ResKey::*;

fn main() {
    task::block_on( async {
        let mut args: Vec<String> = env::args().collect();

        let mut options = args.drain(1..);
        let uri     = options.next().unwrap_or("/demo/example/zenoh-rs-write".to_string());
        let value   = options.next().unwrap_or("Write from Rust!".to_string());
        let locator = options.next().unwrap_or("".to_string());

        println!("Openning session...");
        let session = open(&locator, None).await.unwrap();

        println!("Writing Data ('{}': '{}')...\n", uri, value);
        session.write(&RName(uri), value.as_bytes().to_vec()).await.unwrap();

        session.close().await.unwrap();
    })
}
