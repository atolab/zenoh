use clap::App;
use async_std::task;
use zenoh::net::*;
use zenoh::net::ResKey::*;

fn main() {
    task::block_on( async {
        // initiate logging
        env_logger::init();

        let args = App::new("zenoh-net throughput pub example")
            .arg("-l, --locator=[LOCATOR] 'Sets the locator used to initiate the zenoh session'")
            .arg("<PAYLOAD_SIZE>          'Sets the size of the payload to publish'")
            .get_matches();

        let locator = args.value_of("locator").unwrap_or("").to_string();
        let size    = args.value_of("size").unwrap().parse::<usize>().unwrap();

        let data: RBuf = (0usize..size).map(|i| (i%10) as u8).collect::<Vec<u8>>().into();

        println!("Openning session...");
        let session = open(&locator, None).await.unwrap();

        let reskey = RId(session.declare_resource(&RName("/test/thr".to_string())).await.unwrap());
        let _publ = session.declare_publisher(&reskey).await.unwrap();

        loop {
            session.write(&reskey, data.clone()).await.unwrap();
        }
    })
}
