use async_std::task;
use async_std::sync::Arc;
use rand::RngCore;
use zenoh_protocol::core::{PeerId, ResKey};
use zenoh_protocol::io::RBuf;
use zenoh_protocol::proto::{WhatAmI, Mux};
use zenoh_protocol::session::{SessionManager, SessionManagerConfig, DummyHandler};
use zenoh_router::routing::tables::TablesHdl;


fn main() {
    task::block_on(async{
        let mut args = std::env::args();
        args.next(); // skip exe name

        let my_primitives = Arc::new(Mux::new(Arc::new(DummyHandler::new())));
    
        let tables = Arc::new(TablesHdl::new());

        let mut pid = vec![0, 0, 0, 0];
        rand::thread_rng().fill_bytes(&mut pid);

        let pl_size = match args.next() { 
            Some(size) => size.parse().unwrap(),
            None => 8
        };

        let batch_size: Option<usize> = match args.next() { 
            Some(size) => Some(size.parse().unwrap()),
            None => None
        };
    
        let config = SessionManagerConfig {
            version: 0,
            whatami: WhatAmI::Client,
            id: PeerId{id: pid},
            handler: tables.clone(),
            lease: None,
            resolution: None,
            batchsize: batch_size,
            timeout: None
        };
        let manager = SessionManager::new(config);

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

        
        let payload = RBuf::from(vec![0u8; pl_size]);
        loop {
            primitives.data(&rid, true, &None, payload.clone()).await;
        }
    });
}