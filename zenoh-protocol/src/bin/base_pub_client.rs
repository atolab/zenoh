use async_std::sync::Arc;
use async_std::task;
use async_trait::async_trait;
use rand::RngCore;

use zenoh_protocol::core::{PeerId, ResKey};
use zenoh_protocol::io::RBuf;
use zenoh_protocol::proto::{Message, MessageKind, WhatAmI};
use zenoh_protocol::link::Locator;
use zenoh_protocol::session::{DummyHandler, MsgHandler, SessionHandler, SessionManager, SessionManagerConfig, SessionManagerOptionalConfig};


struct MySH {}

impl MySH {
    fn new() -> Self {
        Self { }
    }
}

#[async_trait]
impl SessionHandler for MySH {
    async fn new_session(&self, 
        _whatami: WhatAmI, 
        _session: Arc<dyn MsgHandler + Send + Sync>
    ) -> Arc<dyn MsgHandler + Send + Sync> {
        Arc::new(DummyHandler::new())
    }
}

fn print_usage(bin: String) {
    println!(
"Usage:
    cargo run --release --bin {} <payload size in bytes> <batch size in bytes> <messages to push on the queue at the same time> <locator to connect to>
Example: 
    cargo run --release --bin {} 8000 16384 16 tcp/127.0.0.1:7447",
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
    let payload: usize = if let Ok(v) = value.parse() {
        v
    } else {
        return print_usage(bin);
    };

    // Get next arg
    let value = if let Some(value) = args.next() {
        value
    } else {
        return print_usage(bin);
    };
    let batchsize: usize = if let Ok(v) = value.parse() {
        v
    } else {
        return print_usage(bin);
    };
    
    // Get next arg
    let value = if let Some(value) = args.next() {
        value
    } else {
        return print_usage(bin);
    };
    let msg_batch: usize = if let Ok(v) = value.parse() {
        v
    } else {
        return print_usage(bin);
    };

    // Get next arg
    let value = if let Some(value) = args.next() {
        value
    } else {
        return print_usage(bin);
    };
    let connect_to: Locator = if let Ok(v) = value.parse() {
        v
    } else {
        return print_usage(bin);
    };

    let config = SessionManagerConfig {
        version: 0,
        whatami: WhatAmI::Peer,
        id: PeerId{id: pid},
        handler: Arc::new(MySH::new())
    };
    let opt_config = SessionManagerOptionalConfig {
        lease: None,
        resolution: None,
        batchsize: Some(batchsize),
        timeout: None,
        retries: None,
        max_sessions: None,
        max_links: None 
    };
    let manager = SessionManager::new(config, Some(opt_config));

    // Connect to publisher
    task::block_on(async {
        let session = if let Ok(s) = manager.open_session(&connect_to).await {
            println!("Opened session on {}", connect_to);
            s
        } else {
            println!("Failed to open session on {}", connect_to);
            return;
        };

        // Send reliable messages
        let kind = MessageKind::FullMessage;
        let reliable = true;
        let sn = 0;
        let key = ResKey::RName("test".to_string());
        let info = None;
        let payload = RBuf::from(vec![0u8; payload]);
        let reply_context = None;
        let cid = None;
        let properties = None;

        let message = Message::make_data(
            kind, reliable, sn, key, info, payload, reply_context, cid, properties
        );

        loop {
            let v = vec![message.clone(); msg_batch];
            session.schedule_batch(v, None, None).await;
        }
    });
}