use zenoh::net::*;

fn main() {
    println!("Scouting...");
    let locs = scout("auto", 10, 500000);
    if !locs.is_empty() {
        for l in locs {
            println!("Locator: {}", l);
        }
    } else {
        println!("Did not find any zenoh router.");
    }
}
