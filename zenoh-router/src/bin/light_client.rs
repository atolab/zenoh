use async_std::task;
use async_std::sync::{Arc, Mutex};
use async_trait::async_trait;
use rand::RngCore;
use zenoh_protocol::core::{PeerId, ResKey, ZInt};
use zenoh_protocol::io::ArcSlice;
use zenoh_protocol::proto::{Primitives, SubMode, QueryConsolidation, QueryTarget, ReplySource, WhatAmI, Mux, DeMux};
use zenoh_protocol::session::{SessionManager, SessionHandler, MsgHandler};

pub struct PrintPrimitives {
}

#[async_trait]
impl Primitives for PrintPrimitives {

    async fn resource(&self, rid: ZInt, reskey: &ResKey) {
        println!("  [RECV] RESOURCE ({:?}) ({:?})", rid, reskey);
    }
    async fn forget_resource(&self, rid: ZInt) {
        println!("  [RECV] FORGET RESOURCE ({:?})", rid);
    }
    
    async fn publisher(&self, reskey: &ResKey) {
        println!("  [RECV] PUBLISHER ({:?})", reskey);
    }
    async fn forget_publisher(&self, reskey: &ResKey) {
        println!("  [RECV] FORGET PUBLISHER ({:?})", reskey);
    }
    
    async fn subscriber(&self, reskey: &ResKey, mode: &SubMode) {
        println!("  [RECV] SUBSCRIBER ({:?}) ({:?})", reskey, mode);
    }
    async fn forget_subscriber(&self, reskey: &ResKey) {
        println!("  [RECV] FORGET SUBSCRIBER ({:?})", reskey);
    }
    
    async fn storage(&self, reskey: &ResKey) {
        println!("  [RECV] STORAGE ({:?})", reskey);
    }
    async fn forget_storage(&self, reskey: &ResKey) {
        println!("  [RECV] FORGET STORAGE ({:?})", reskey);
    }
    
    async fn eval(&self, reskey: &ResKey) {
        println!("  [RECV] EVAL ({:?})", reskey);
    }
    async fn forget_eval(&self, reskey: &ResKey) {
        println!("  [RECV] FORGET EVAL ({:?})", reskey);
    }

    async fn data(&self, reskey: &ResKey, _info: &Option<ArcSlice>, _payload: &ArcSlice) {
        println!("  [RECV] DATA ({:?})", reskey);
    }
    async fn query(&self, reskey: &ResKey, predicate: &String, qid: &ZInt, target: &Option<QueryTarget>, consolidation: &QueryConsolidation) {
        println!("  [RECV] QUERY ({:?}) ({:?}) ({:?}) ({:?}) ({:?})", reskey, predicate, qid, target, consolidation);
    }
    async fn reply(&self, qid: &ZInt, source: &ReplySource, replierid: &Option<PeerId>, reskey: &ResKey, _info: &Option<ArcSlice>, _payload: &ArcSlice) {
        println!("  [RECV] REPLY ({:?}) ({:?}) ({:?}) ({:?})", qid, source, replierid, reskey);
    }
    async fn pull(&self, is_final: bool, reskey: &ResKey, pull_id: &ZInt, max_samples: &Option<ZInt>) {
        println!("  [RECV] PULL ({:?}) ({:?}) ({:?}) ({:?})", is_final, reskey, pull_id, max_samples);
    }

    async fn close(&self) {
        println!("  CLOSE");
    }
}

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
        Arc::new(DeMux::new(PrintPrimitives {}))
    }
}

fn main() {
    task::block_on(async{
        let mut args = std::env::args();
        args.next(); // skip exe name

        let mut pid = vec![0, 0, 0, 0];
        rand::thread_rng().fill_bytes(&mut pid);

        let session_handler = Arc::new(LightSessionHandler::new());
        let manager = SessionManager::new(0, WhatAmI::Client, PeerId{id: pid.clone()}, 0, session_handler.clone());

        if let Some(locator) = args.next() {
            if let Err(_err) =  manager.open_session(&locator.parse().unwrap()).await {
                println!("Unable to connect to {}!", locator);
                std::process::exit(-1);
            }
        }
    
        let primitives = Mux::new(session_handler.handler.lock().await.as_ref().unwrap().clone());

        primitives.subscriber(&"/demo/**".to_string().into(), &SubMode::Push).await;

        let res: ResKey = ["/demo/client/", &pid[0].to_string(), &pid[1].to_string(), &pid[2].to_string(), &pid[3].to_string()]
            .concat().into();
        loop {
            println!("[SEND] DATA ({:?})", &res);
            primitives.data(&res, &None, &ArcSlice::from(vec![1])).await;
            std::thread::sleep(std::time::Duration::from_millis(1000));
        }
    });
}