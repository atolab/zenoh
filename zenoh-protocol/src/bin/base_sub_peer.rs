use async_std::future;
use async_std::sync::Arc;
use async_std::task;
use async_trait::async_trait;
use rand::RngCore;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use zenoh_protocol::core::{PeerId, ZResult};
use zenoh_protocol::proto::{Message, WhatAmI};
use zenoh_protocol::link::Locator;
use zenoh_protocol::session::{MsgHandler, SessionHandler, SessionManager, SessionManagerConfig};

// Session Handler for the peer
struct MySH {
    counter: Arc<AtomicUsize>
}

impl MySH {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        Self { counter }
    }
}

#[async_trait]
impl SessionHandler for MySH {
    async fn new_session(&self, 
        _whatami: WhatAmI, 
        _session: Arc<dyn MsgHandler + Send + Sync>
    ) -> Arc<dyn MsgHandler + Send + Sync> {
        Arc::new(MyMH::new(self.counter.clone()))
    }
}

// Message Handler for the peer
struct MyMH {
    counter: Arc<AtomicUsize>
}

impl MyMH {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        Self { counter }
    }
}

#[async_trait]
impl MsgHandler for MyMH {
    async fn handle_message(&self, _message: Message) -> ZResult<()> {
        self.counter.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn close(&self) {}
}


fn main() {
    let mut pid = vec![0, 0, 0, 0];
    rand::thread_rng().fill_bytes(&mut pid);

    let count = Arc::new(AtomicUsize::new(0));

    let config = SessionManagerConfig {
        version: 0,
        whatami: WhatAmI::Peer,
        id: PeerId{id: pid},
        handler: Arc::new(MySH::new(count.clone())),
        lease: None,
        resolution: None,
        batchsize: None,
        timeout: None
    };
    let manager = SessionManager::new(config);

    let mut args = std::env::args();
    // Skip exe name
    args.next();

    if args.len() == 0 {
        println!("Provide a locator for the base_pub_peer to connect on!");
        return
    }

    task::spawn(async move {
        loop {
            task::sleep(Duration::from_secs(1)).await;
            let c = count.swap(0, Ordering::Relaxed);
            println!("{} msg/s", c);
        }
    });

    // Connect to publisher
    task::block_on(async {
        // Listen on each locator
        let args: Vec<String> = args.collect();
        for l in args.iter() {
            let locator: Locator = l.parse().unwrap();
            manager.add_locator(&locator, None).await.unwrap();
            println!("Listening on {}", locator);
        }
        // Stop forever
        future::pending::<()>().await;
    });
}