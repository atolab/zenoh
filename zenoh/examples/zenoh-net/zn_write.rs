use clap::App;
use async_std::task;
use zenoh::net::*;

fn main() {
    task::block_on( async {
        // initiate logging
        env_logger::init();

        let args = App::new("zenoh-net write example")
            .arg("-l, --locator=[LOCATOR] 'Sets the locator used to initiate the zenoh session'")
            .arg("-p, --path=[PATH]       'Sets the name of the resource to write'")
            .arg("-v, --value=[VALUE]     'Sets the value of the resource to write'")
            .get_matches();

        let locator = args.value_of("locator").unwrap_or("").to_string();
        let path    = args.value_of("path").unwrap_or("/demo/example/zenoh-rs-write").to_string();
        let value   = args.value_of("value").unwrap_or("Write from Rust!").to_string();

        println!("Openning session...");
        let session = open(&locator, None).await.unwrap();

        println!("Writing Data ('{}': '{}')...\n", path, value);
        session.write(&path.into(), value.as_bytes().into()).await.unwrap();

        session.close().await.unwrap();
    })
}
