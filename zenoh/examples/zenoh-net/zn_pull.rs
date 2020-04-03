use std::env;
use std::io::Read;
use async_std::task;
use zenoh::net::*;
use zenoh::net::ResKey::*;

fn data_handler(res_name: &str, payload: RBuf, _data_info: DataInfo) {
    println!(">> [Subscription listener] Received ('{}': '{:02x?}')", res_name, payload);
}

fn main() {
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
            mode: SubMode::Pull,
            period: None
        };
        let sub = session.declare_subscriber(RName(uri), sub_info, &data_handler).await.unwrap();

        println!("Press <enter> to pull data...");
        let mut reader = std::io::stdin();
        let mut input = [0u8];
        while input[0] != 'q' as u8 {
            reader.read_exact(&mut input).unwrap();
            sub.pull().await.unwrap();
        }

        session.undeclare_subscriber(sub).await.unwrap();
        session.close().await.unwrap();
    })
}
