use async_std::task;
use async_std::sync::Arc;
use rand::RngCore;
use zenoh_protocol::core::{PeerId, ResKey};
use zenoh_protocol::io::ArcSlice;
use zenoh_protocol::proto::{WhatAmI, Mux};
use zenoh_protocol::session::{SessionManager, DummyHandler};
use zenoh_router::routing::tables::TablesHdl;


fn main() {
    task::block_on(async{
        let mut args = std::env::args();
        args.next(); // skip exe name

        let my_primitives = Arc::new(Mux::new(Arc::new(DummyHandler::new())));
    
        let tables = Arc::new(TablesHdl::new());

        let mut pid = vec![0, 0, 0, 0];
        rand::thread_rng().fill_bytes(&mut pid);

        let pl_size = match args.next() { Some(size) => {size.parse().unwrap()} None => {8}};
    
        let manager = SessionManager::new(0, WhatAmI::Peer, PeerId{id: pid}, 0, tables.clone());
        let port = match args.next() { Some(port) => {port} None => {"7447".to_string()}};
        let locator = ["tcp/127.0.0.1:", &port].concat().parse().unwrap();
        if let Err(_err) = manager.add_locator(&locator, None).await {
            println!("Unable to open listening port {}!", port);
            std::process::exit(-1);
        }

        for locator in args {
            if let Err(_err) =  manager.open_session(&locator.parse().unwrap()).await {
                println!("Unable to connect to {}!", locator);
                std::process::exit(-1);
            }
        }
    
        let primitives = tables.new_primitives(my_primitives).await;

        primitives.resource(1, &"/tp".to_string().into()).await;
        let rid = ResKey::RId(1);
        primitives.publisher(&rid).await;

        
        loop {
            let payload = ArcSlice::from(vec![0u8; pl_size]);
            primitives.data(&rid, &None, &payload).await;
        }
    });
}