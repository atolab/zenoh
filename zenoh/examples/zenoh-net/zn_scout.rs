use async_std::task;
use zenoh::net::*;

fn main() {
    // for logging
    env_logger::init();

    task::block_on( async {
        println!("Scouting...");
        let locs = scout("auto", 10, 500000).await;
        if !locs.is_empty() {
            for l in locs {
                println!("Locator: {}", l);
            }
        } else {
            println!("Did not find any zenoh router.");
        }
    })
}
