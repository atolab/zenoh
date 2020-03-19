use std::env;
use zenoh::net::*;
use zenoh::net::ResKey::*;

fn main() {
    let mut args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        println!("USAGE:\n\tzn_pub_thr <payload-size> [<zenoh-locator>]\n\n");
        std::process::exit(-1);
    }

    let mut options = args.drain(1..);
    let len     = options.next().unwrap().parse::<usize>().unwrap();
    let locator = options.next().unwrap_or("".to_string());

    let data = (0usize..len).map(|i| (i%10) as u8).collect::<Vec<u8>>();

    println!("Openning session...");
    let session = open(&locator, None).unwrap();

    let reskey = RId(session.declare_resource(&RName("/test/thr".to_string())).unwrap());
    let _publ = session.declare_publisher(&reskey).unwrap();

    loop {
        session.write(&reskey, &data).unwrap();
    }
}
