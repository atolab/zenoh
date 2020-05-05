use async_std::task;
use async_std::sync::Arc;
use rand::RngCore;
use zenoh_protocol::core::{PeerId, ResKey};
use zenoh_protocol::io::RBuf;
use zenoh_protocol::link::Locator;
use zenoh_protocol::proto::{WhatAmI, Mux};
use zenoh_protocol::session::{SessionManager, SessionManagerConfig, SessionManagerOptionalConfig, DummyHandler};
use zenoh_router::routing::tables::TablesHdl;


fn main() {
    task::block_on(async{
        let mut args = std::env::args();
        args.next(); // skip exe name

        let my_primitives = Arc::new(Mux::new(Arc::new(DummyHandler::new())));
    
        let tables = Arc::new(TablesHdl::new());

        let mut pid = vec![0, 0, 0, 0];
        rand::thread_rng().fill_bytes(&mut pid);

        let pl_size: usize = match args.next() { 
            Some(size) => size.parse().unwrap(),
            None => 8
        };
        let batch_size: Option<usize> = match args.next() { 
            Some(size) => Some(size.parse().unwrap()),
            None => None
        };
        let self_locator: Locator = match args.next() { 
            Some(port) => {
                let mut s = "tcp/127.0.0.1:".to_string();
                s.push_str(&port);
                s.parse().unwrap()
            },
            None => "tcp/127.0.0.1:7447".parse().unwrap()
        };
    
        let config = SessionManagerConfig {
            version: 0,
            whatami: WhatAmI::Peer,
            id: PeerId{id: pid},
            handler: tables.clone()
        };
        let opt_config = SessionManagerOptionalConfig {
            lease: None,
            resolution: None,
            batchsize: batch_size,
            timeout: None,
            retries: None,
            max_sessions: None,
            max_links: None 
        };
        let manager = SessionManager::new(config, Some(opt_config));

        if let Err(_err) = manager.add_locator(&self_locator).await {
            println!("Unable to listen on {}!", self_locator);
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

        
        let payload = RBuf::from(vec![0u8; pl_size]);
        loop {
            primitives.data(&rid, true, &None, payload.clone()).await;
        }
    });
}