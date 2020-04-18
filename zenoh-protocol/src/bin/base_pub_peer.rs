use async_std::future;
use async_std::sync::Arc;
use async_std::task;
use async_trait::async_trait;
use rand::RngCore;

use zenoh_protocol::core::{PeerId, ResKey};
use zenoh_protocol::io::RBuf;
use zenoh_protocol::proto::{Message, MessageKind, WhatAmI};
use zenoh_protocol::link::Locator;
use zenoh_protocol::session::{DummyHandler, MsgHandler, SessionHandler, SessionManager, SessionManagerConfig};

// Session Handler for the peer
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


fn main() {
    let mut pid = vec![0, 0, 0, 0];
    rand::thread_rng().fill_bytes(&mut pid);

    let mut args = std::env::args();
    // Skip exe name
    args.next();

    let payload: usize = args.next().unwrap().parse().unwrap();
    let batchsize: usize = args.next().unwrap().parse().unwrap(); 
    let msg_batch: usize = args.next().unwrap().parse().unwrap(); 

    let config = SessionManagerConfig {
        version: 0,
        whatami: WhatAmI::Peer,
        id: PeerId{id: pid},
        handler: Arc::new(MySH::new()),
        lease: None,
        resolution: None,
        batchsize: Some(batchsize),
        timeout: None
    };
    let manager = SessionManager::new(config);

    // Connect to publisher
    task::block_on(async {
        let args: Vec<String> = args.collect();
        for locator in args.iter() {
            // Connect to each locator
            let l: Locator = locator.parse().unwrap();
            let session = manager.open_session(&l).await.unwrap();
            println!("Opened session on {}", l);

            // Start sending data
            task::spawn(async move  {
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
        // Wait forever
        future::pending::<()>().await;
    });
}