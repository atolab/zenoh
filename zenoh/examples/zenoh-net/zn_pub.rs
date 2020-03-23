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

        // Split the uri on the last '/'. 
        // The first part will be declared as resource,
        // and the second as suffix for writing using the resource.
        let mut resource_name = uri.to_string();
        let suffix = resource_name.split_off(resource_name.rfind('/').unwrap());

        println!("Declaring Resource {} ", resource_name);
        let rid = session.declare_resource(&RName(resource_name.clone())).await.unwrap();

        println!("Declaring Publisher on {}/**", resource_name);
        let publ = session.declare_publisher(&RIdWithSuffix(rid.clone(), "/**".to_string())).await.unwrap();

        println!("Writing Data ('{}': '{}')...\n", uri, value);
        session.write(&RIdWithSuffix(rid, suffix), value.as_bytes()).await.unwrap();

        session.undeclare_publisher(publ).await.unwrap();
        session.close().await.unwrap();
    })
}
