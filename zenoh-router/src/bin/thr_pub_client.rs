use async_std::task;
use async_std::sync::Arc;

use rand::RngCore;
use std::time::Duration;

use zenoh_protocol::core::{PeerId, ResKey};
use zenoh_protocol::io::RBuf;
use zenoh_protocol::proto::{Mux, whatami};
use zenoh_protocol::session::{SessionManager, SessionManagerConfig, SessionManagerOptionalConfig, DummyHandler};
use zenoh_router::routing::broker::Broker;


fn main() {
    task::block_on(async{
        let mut args = std::env::args();
        args.next(); // skip exe name

        let my_primitives = Arc::new(Mux::new(Arc::new(DummyHandler::new())));
    
        let broker = Arc::new(Broker::new());

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
            whatami: whatami::CLIENT,
            id: PeerId{id: pid},
            handler: broker.clone()
        };
        let opt_config = SessionManagerOptionalConfig {
            lease: None,
            sn_resolution: None,
            batchsize: batch_size,
            timeout: None,
            retries: None,
            max_sessions: None,
            max_links: None 
        };
        let manager = SessionManager::new(config, Some(opt_config));

        let attachment = None;
        for locator in args {
            if let Err(_err) =  manager.open_session(&locator.parse().unwrap(), &attachment).await {
                println!("Unable to connect to {}!", locator);
                std::process::exit(-1);
            }
        }
    
        let primitives = broker.new_primitives(my_primitives).await;

        primitives.resource(1, &"/tp".to_string().into()).await;
        let rid = ResKey::RId(1);
        primitives.publisher(&rid).await;

        // @TODO: Fix writer starvation in the RwLock and remove this sleep
        // Wait for the declare to arrive
        task::sleep(Duration::from_millis(1_000)).await;
        
        let payload = RBuf::from(vec![0u8; pl_size]);
        loop {
            primitives.data(&rid, true, &None, payload.clone()).await;
        }
    });
}