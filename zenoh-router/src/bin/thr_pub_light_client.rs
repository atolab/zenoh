use async_std::task;
use async_std::sync::{Arc, Mutex};
use async_trait::async_trait;
use rand::RngCore;
use zenoh_protocol::core::{PeerId, ResKey};
use zenoh_protocol::io::RBuf;
use zenoh_protocol::proto::{Primitives, WhatAmI, Mux};
use zenoh_protocol::session::{SessionManager, SessionManagerConfig, SessionHandler, MsgHandler, DummyHandler};

struct LightSessionHandler {
    pub handler: Mutex<Option<Arc<dyn MsgHandler + Send + Sync>>>,
}

impl LightSessionHandler {
    pub fn new() -> LightSessionHandler {
        LightSessionHandler { handler: Mutex::new(None),}
    }
}

#[async_trait]
impl SessionHandler for LightSessionHandler {
    async fn new_session(&self, _whatami: WhatAmI, session: Arc<dyn MsgHandler + Send + Sync>) -> Arc<dyn MsgHandler + Send + Sync> {
        *self.handler.lock().await = Some(session);
        Arc::new(DummyHandler::new())
    }
}

fn main() {
    task::block_on(async{
        let mut args = std::env::args();
        args.next(); // skip exe name

        let mut pid = vec![0, 0, 0, 0];
        rand::thread_rng().fill_bytes(&mut pid);

        let pl_size = match args.next() { Some(size) => {size.parse().unwrap()} None => {8}};

        let session_handler = Arc::new(LightSessionHandler::new());
        let config = SessionManagerConfig {
            version: 0,
            whatami: WhatAmI::Client,
            id: PeerId{id: pid.clone()},
            handler: session_handler.clone(),
            lease: None,
            resolution: None,
            batchsize: None,
            timeout: None
        };
        let manager = SessionManager::new(config);

        if let Some(locator) = args.next() {
            if let Err(_err) =  manager.open_session(&locator.parse().unwrap()).await {
                println!("Unable to connect to {}!", locator);
                std::process::exit(-1);
            }
        }
    
        let primitives = Mux::new(session_handler.handler.lock().await.as_ref().unwrap().clone());

        primitives.resource(1, "/tp".to_string().into()).await;
        primitives.publisher(ResKey::RId(1)).await;
        
        let payload = RBuf::from(vec![0u8; pl_size]);
        loop {
            primitives.data(ResKey::RId(1), true, None, payload.clone()).await;
        }
    });
}