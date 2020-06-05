use clap::App;
use async_std::task;
use zenoh::net::*;

fn main() {
    task::block_on( async {
        // initiate logging
        env_logger::init();

        let args = App::new("zenoh-net info example")
            .arg("-l, --locator=[LOCATOR] 'Sets the locator used to initiate the zenoh session'")
            .get_matches();

        let locator = args.value_of("locator").unwrap_or("").to_string();
        
        let mut ps = Properties::new();
        ps.insert(ZN_USER_KEY, "user".as_bytes().to_vec());
        ps.insert(ZN_PASSWD_KEY, "password".as_bytes().to_vec());

        println!("Openning session...");
        let session = open(&locator, Some(ps)).await.unwrap();

        let info = session.info();
        println!("LOCATOR :  \"{}\"", String::from_utf8_lossy(info.get(&ZN_INFO_PEER_KEY).unwrap()));
        println!("PID :      {:02x?}", info.get(&ZN_INFO_PID_KEY).unwrap());
        println!("PEER PID : {:02x?}", info.get(&ZN_INFO_PEER_PID_KEY).unwrap());
    })
}
