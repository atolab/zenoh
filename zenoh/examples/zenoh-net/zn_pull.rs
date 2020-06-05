use clap::App;
use async_std::prelude::*;
use async_std::task;
use zenoh::net::*;

fn main() {
    task::block_on( async {
        // initiate logging
        env_logger::init();

        let args = App::new("zenoh-net pull example")
            .arg("-l, --locator=[LOCATOR]   'Sets the locator used to initiate the zenoh session'")
            .arg("-s, --selector=[SELECTOR] 'Sets the selection of resources to pull'")
            .get_matches();

        let locator  = args.value_of("locator").unwrap_or("").to_string();
        let selector = args.value_of("selector").unwrap_or("/demo/example/**").to_string();

        println!("Openning session...");
        let session = open(&locator, None).await.unwrap();

        println!("Declaring Subscriber on {}", selector);
        let sub_info = SubInfo {
            reliability: Reliability::Reliable,
            mode: SubMode::Pull,
            period: None
        };

        let sub = session.declare_subscriber(&selector.into(), &sub_info,
            move |res_name: &str, payload: Vec<u8>, _data_info: DataInfo| {
                println!(">> [Subscription listener] Received ('{}': '{}')", 
                    res_name, std::str::from_utf8(&payload).unwrap());
            }
        ).await.unwrap();

        println!("Press <enter> to pull data...");
        let mut stdin = async_std::io::stdin();
        let mut input = [0u8];
        while input[0] != 'q' as u8 {
            stdin.read_exact(&mut input).await.unwrap();
            sub.pull().await.unwrap();
        }

        session.undeclare_subscriber(sub).await.unwrap();
        session.close().await.unwrap();
    })
}
