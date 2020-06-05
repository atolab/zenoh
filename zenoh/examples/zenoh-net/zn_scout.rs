use async_std::task;
use zenoh::net::*;

fn main() {
    task::block_on( async {
        // initiate logging
        env_logger::init();
    
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
