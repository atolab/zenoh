use clap::App;
use async_std::task;
use zenoh::net::*;

fn main() {
    task::block_on( async {
        // initiate logging
        env_logger::init();

        let args = App::new("zenoh-net pub example")
            .arg("-l, --locator=[LOCATOR] 'Sets the locator used to initiate the zenoh session'")
            .arg("-p, --path=[PATH]       'Sets the name of the resource to publish'")
            .arg("-v, --value=[VALUE]     'Sets the value of the resource to publish'")
            .get_matches();

        let locator = args.value_of("locator").unwrap_or("").to_string();
        let path    = args.value_of("path").unwrap_or("/demo/example/zenoh-rs-pub").to_string();
        let value   = args.value_of("value").unwrap_or("Pub from Rust!").to_string();

        println!("Openning session...");
        let session = open(&locator, None).await.unwrap();
        
        print!("Declaring Resource {}", path);
        let rid = session.declare_resource(&path.into()).await.unwrap();
        println!(" => RId {}", rid);
        
        println!("Declaring Publisher on {}", rid);
        let publ = session.declare_publisher(&rid.into()).await.unwrap();
        
        println!("Writing Data ('{}': '{}')...\n", rid, value);
        session.write(&rid.into(), value.as_bytes().into()).await.unwrap();

        session.undeclare_publisher(publ).await.unwrap();
        session.close().await.unwrap();
    })
}
