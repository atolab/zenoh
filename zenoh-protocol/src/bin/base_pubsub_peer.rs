use async_std::sync::Arc;
use async_std::task;
use async_trait::async_trait;
use rand::RngCore;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use zenoh_protocol::core::{PeerId, ResKey, ZResult};
use zenoh_protocol::io::RBuf;
use zenoh_protocol::proto::{Message, MessageKind, WhatAmI};
use zenoh_protocol::link::Locator;
use zenoh_protocol::session::{MsgHandler, SessionHandler, SessionManager, SessionManagerConfig};

// Session Handler for the peer
struct MySH {
    counter: Arc<AtomicUsize>,
    active: AtomicBool
}

impl MySH {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        Self { counter, active: AtomicBool::new(false) }
    }
}

#[async_trait]
impl SessionHandler for MySH {
    async fn new_session(&self, 
        _whatami: WhatAmI, 
        _session: Arc<dyn MsgHandler + Send + Sync>
    ) -> Arc<dyn MsgHandler + Send + Sync> {
        if !self.active.swap(true, Ordering::Acquire) {
            let count = self.counter.clone();
            task::spawn(async move {
                loop {
                    task::sleep(Duration::from_secs(1)).await;
                    let c = count.swap(0, Ordering::Relaxed);
                    println!("{} msg/s", c);
                }
            });
        }
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

fn print_usage(bin: String) {
    println!(
"Usage:
    cargo run --release --bin {} <payload size in bytes> <batch size in bytes> <messages to push on the queue at the same time> <locator to listen on> <locator to connect to>
Example: 
    cargo run --release --bin {} 8000 16384 16 tcp/127.0.0.1:7447 tcp/127.0.0.1:7448",
        bin, bin
    );
}

fn main() {
    let mut pid = vec![0, 0, 0, 0];
    rand::thread_rng().fill_bytes(&mut pid);

    let count = Arc::new(AtomicUsize::new(0));

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
    let listen_on: Locator = if let Ok(v) = value.parse() {
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
        handler: Arc::new(MySH::new(count)),
        lease: None,
        resolution: None,
        batchsize: Some(batchsize),
        timeout: None,
        max_sessions: None,
        max_links: None 
    };
    let manager = SessionManager::new(config);

    // Connect to publisher
    task::block_on(async {
        if manager.add_locator(&listen_on).await.is_ok() {
            println!("Listening on {}", listen_on);
        } else {
            println!("Failed to listen on {}", listen_on);
            return;
        };

        let session = loop {
            if let Ok(s) = manager.open_session(&connect_to).await {
                println!("Opened session with {}", connect_to);
                break s;
            } else {
                task::sleep(Duration::from_secs(1)).await;
            }
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