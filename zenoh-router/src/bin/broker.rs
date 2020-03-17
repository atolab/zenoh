use async_std::task;
use async_std::sync::Arc;
use rand::RngCore;
use zenoh_protocol::core::PeerId;
use zenoh_protocol::proto::WhatAmI;
use zenoh_protocol::session::SessionManager;
use zenoh_router::routing::tables::TablesHdl;

fn main() {
    task::block_on(async{
        let mut args = std::env::args();
        args.next(); // skip exe name
    
        let tables = Arc::new(TablesHdl::new());

        let mut pid = vec![0, 0, 0, 0];
        rand::thread_rng().fill_bytes(&mut pid);
    
        let manager = SessionManager::new(0, WhatAmI::Broker, PeerId{id: pid}, 0, tables.clone());
        let port = match args.next() { Some(port) => {port} None => {"7447".to_string()}};
        let locator = ["tcp/127.0.0.1:", &port].concat().parse().unwrap();
        if let Err(_err) = manager.add_locator(&locator, None).await {
            println!("Unable to open listening port {}!", port);
            std::process::exit(-1);
        }

        while let Some(locator) = args.next() {
            if let Err(_err) =  manager.open_session(&locator.parse().unwrap()).await {
                println!("Unable to connect to {}!", locator);
                std::process::exit(-1);
            }
        }
    
        loop {
            std::thread::sleep(std::time::Duration::from_millis(10000));
        }
    });
}