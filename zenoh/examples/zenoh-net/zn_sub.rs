use std::env;
use async_std::prelude::*;
use async_std::task;
use zenoh::net::*;
use zenoh::net::ResKey::*;

fn data_handler(res_name: &str, payload: RBuf, _data_info: DataInfo) {
    println!("FUNCTION >> [Subscription listener] Received ('{}': '{:02x?}')", res_name, payload);
}

fn main() {
    // for logging
    env_logger::init();

    task::block_on( async {
        let mut args: Vec<String> = env::args().collect();

        let mut options = args.drain(1..);
        let uri     = options.next().unwrap_or("/demo/example/**".to_string());
        let locator = options.next().unwrap_or("".to_string());

        println!("Openning session...");
        let session = open(&locator, None).await.unwrap();

        println!("Declaring Subscriber on {}", uri);

        let sub_info = SubInfo {
            reliability: Reliability::Reliable,
            mode: SubMode::Push,
            period: None
        };

        let sub = session.declare_subscriber(&RName(uri.clone()), &sub_info, data_handler).await.unwrap();

        let sub2 = session.declare_subscriber(&RName(uri), &sub_info,
            move |res_name: &str, payload: RBuf, _data_info: DataInfo| {
                println!("CLOSURE >> [Subscription listener] Received ('{}': '{:02x?}')", res_name, payload);
            }
        ).await.unwrap();

        let mut stdin = async_std::io::stdin();
        let mut input = [0u8];
        while input[0] != 'q' as u8 {
            stdin.read_exact(&mut input).await.unwrap();
        }

        session.undeclare_subscriber(sub).await.unwrap();
        session.undeclare_subscriber(sub2).await.unwrap();
        session.close().await.unwrap();
    })
}
