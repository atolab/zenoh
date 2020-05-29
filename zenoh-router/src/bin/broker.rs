use async_std::future;
use async_std::sync::Arc;
use async_std::task;
use rand::RngCore;
use zenoh_protocol::core::PeerId;
use zenoh_protocol::link::Locator;
use zenoh_protocol::proto::whatami;
use zenoh_protocol::session::{SessionManager, SessionManagerConfig, SessionManagerOptionalConfig};
use zenoh_router::routing::broker::Broker;

fn main() {
    task::block_on(async {
        let mut args = std::env::args();
        args.next(); // skip exe name
    
        let broker = Arc::new(Broker::new());

        let mut pid = vec![0, 0, 0, 0];
        rand::thread_rng().fill_bytes(&mut pid);

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
            whatami: whatami::BROKER,
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

        if let Err(_err) = manager.add_locator(&self_locator).await {
            println!("Unable to open listening {}!", self_locator);
            std::process::exit(-1);
        }

        let attachment = None;
        for locator in args {
            if let Err(_err) =  manager.open_session(&locator.parse().unwrap(), &attachment).await {
                println!("Unable to connect to {}!", locator);
                std::process::exit(-1);
            }
        }
        
        future::pending::<()>().await;
    });
}