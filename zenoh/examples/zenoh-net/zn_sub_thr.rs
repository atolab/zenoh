use async_std::future;
use async_std::task;
use std::env;
use std::time::Instant;
use zenoh::net::*;
use zenoh::net::ResKey::*;

const N: u128 = 100000;

fn print_stats(start: Instant) {
    let elapsed = start.elapsed().as_secs_f64();
    let thpt = (N as f64) / elapsed;
    println!("{} msg/s", thpt);
}


fn main() {
    // for logging
    env_logger::init();

    task::block_on( async {
        let mut args: Vec<String> = env::args().collect();

        args.pop(); // ignore arg[0] (exe name)
        let locator = args.pop().unwrap_or("".to_string());

        let session = open(&locator, None).await.unwrap();

        let reskey = RId(session.declare_resource(&RName("/test/thr".to_string())).await.unwrap());

        let mut count = 0u128;
        let mut start = Instant::now();

        let sub_info = SubInfo {
            reliability: Reliability::Reliable,
            mode: SubMode::Push,
            period: None
        };
        let _ = session.declare_subscriber(&reskey, &sub_info,
            move |_res_name: &str, _payload: RBuf, _data_info: DataInfo| {
                if count == 0 {
                    start = Instant::now();
                    count = count + 1;
                } else if count < N {
                    count = count + 1;
                } else {
                    print_stats(start);
                    count = 0;
                }
            }
        ).await.unwrap();

        // Stop forever
        future::pending::<()>().await;

        // @TODO: Uncomment these once the writer starvation has been solved on the RwLock      
        // session.undeclare_subscriber(sub).await.unwrap();
        // session.close().await.unwrap();
    });
}
