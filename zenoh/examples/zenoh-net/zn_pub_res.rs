use std::env;
use async_std::task;
use zenoh::net::*;
// use zenoh::net::ResKey::*;

fn main() {
    task::block_on( async {
        let mut args: Vec<String> = env::args().collect();

        let mut options = args.drain(1..);
        let uri     = options.next().unwrap_or("/zenoh/demo/quote".to_string());
        let value   = options.next().unwrap_or("Simplicity is the ultimate sophistication".to_string());
        let locator = options.next().unwrap_or("".to_string());

        println!("Openning session...");
        let session = open(&locator, None).await.unwrap();

        // Split the uri on the last '/'. 
        // The first part will be declared as resource,
        // and the second as suffix for writing using the resource.
        // let mut resource_name = uri.to_string();
        // let suffix = resource_name.split_off(resource_name.rfind('/').unwrap());

        println!("Declaring Resource {} ", uri);

        let rid = session.declare_resource(&uri.into()).await.unwrap();
        println!("Declaring Publisher on {}", rid);

        let res_key = rid.into();
        let publ = session.declare_publisher(&res_key).await.unwrap();
        
        println!("Writing Data ('{}': '{}')...\n", rid, value);
        session.write(&res_key, value.as_bytes().to_vec()).await.unwrap();

        session.undeclare_publisher(publ).await.unwrap();
        session.close().await.unwrap();
    })
}
