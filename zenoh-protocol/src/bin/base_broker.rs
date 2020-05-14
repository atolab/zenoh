use async_std::future;
use async_std::sync::{Arc, Mutex};
use async_std::task;
use async_trait::async_trait;
use rand::RngCore;
use slab::Slab;

use zenoh_protocol::core::{PeerId, ZResult};
use zenoh_protocol::proto::{ZenohMessage, WhatAmI, whatami};
use zenoh_protocol::link::Locator;
use zenoh_protocol::session::{MsgHandler, SessionHandler, SessionManager, SessionManagerConfig, SessionManagerOptionalConfig};


type Table = Arc<Mutex<Slab<Arc<dyn MsgHandler + Send + Sync>>>>;

// Session Handler for the peer
struct MySH {
    table: Table
}

impl MySH {
    fn new() -> Self {
        Self { table: Arc::new(Mutex::new(Slab::new())) }
    }
}

#[async_trait]
impl SessionHandler for MySH {
    async fn new_session(&self, 
        _whatami: WhatAmI, 
        session: Arc<dyn MsgHandler + Send + Sync>
    ) -> Arc<dyn MsgHandler + Send + Sync> {
        println!("New session opened!");
        let index = self.table.lock().await.insert(session);
        Arc::new(MyMH::new(self.table.clone(), index))
    }
}

// Message Handler for the peer
struct MyMH {
    table: Table,
    index: usize
}

impl MyMH {
    fn new(table: Table, index: usize) -> Self {
        Self { table, index }
    }
}

#[async_trait]
impl MsgHandler for MyMH {
    async fn handle_message(&self, message: ZenohMessage) -> ZResult<()> {
        for (i, e) in self.table.lock().await.iter() {
            if i != self.index {
                let _ = e.handle_message(message.clone()).await;
            }
        }
        Ok(())
    }

    async fn close(&self) {
        self.table.lock().await.remove(self.index);
    }
}

fn print_usage(bin: String) {
    println!(
"Usage:
    cargo run --release --bin {} <batch size in bytes> <locator to listen on>
Example: 
    cargo run --release --bin {} 8192 tcp/127.0.0.1:7447",
        bin, bin
    );
}

fn main() {
    let mut pid = vec![0, 0, 0, 0];
    rand::thread_rng().fill_bytes(&mut pid);

    let mut args = std::env::args();
    // Get exe name
    let bin = args.next().unwrap();
    
    // Get next arg
    let value = if let Some(value) = args.next() {
        value
    } else {
        return print_usage(bin);
    };
    let batchsize: Option<usize> = if let Ok(v) = value.parse() {
        Some(v)
    } else {
        None
    };

    // Get next arg
    let value = if let Some(value) = args.next() {
        value
    } else {
        return print_usage(bin);
    };
    let listen_on: Locator = if let Ok(v) = value.parse() {
        v
    } else {
        return print_usage(bin);
    };
    
    // Create the session manager
    let config = SessionManagerConfig {
        version: 0,
        whatami: whatami::PEER,
        id: PeerId{id: pid},
        handler: Arc::new(MySH::new())
    };
    let opt_config = SessionManagerOptionalConfig {
        lease: None,
        sn_resolution: None,
        batchsize,
        timeout: None,
        retries: None,
        max_sessions: None,
        max_links: None 
    };
    let manager = SessionManager::new(config, Some(opt_config));

    // Connect to publisher
    task::block_on(async {
        if manager.add_locator(&listen_on).await.is_ok() {
            println!("Listening on {}", listen_on);
        } else {
            println!("Failed to listen on {}", listen_on);
            return;
        };
        // Stop forever
        future::pending::<()>().await;
    });
}