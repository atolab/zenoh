use async_std::task;
use async_std::sync::{Arc, Mutex};
use async_trait::async_trait;
use rand::RngCore;
use zenoh_protocol::core::{PeerId, ResKey, ZInt};
use zenoh_protocol::io::RBuf;
use zenoh_protocol::proto::{Primitives, SubInfo, Reliability, SubMode, QueryConsolidation, QueryTarget, Reply, Mux, DeMux, WhatAmI, whatami};
use zenoh_protocol::session::{SessionManager, SessionManagerConfig, SessionHandler, MsgHandler};

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
    
    async fn subscriber(&self, reskey: &ResKey, sub_info: &SubInfo) {
        println!("  [RECV] SUBSCRIBER ({:?}) ({:?})", reskey, sub_info);
    }
    async fn forget_subscriber(&self, reskey: &ResKey) {
        println!("  [RECV] FORGET SUBSCRIBER ({:?})", reskey);
    }
    
    async fn queryable(&self, reskey: &ResKey) {
        println!("  [RECV] QUERYABLE ({:?})", reskey);
    }
    async fn forget_queryable(&self, reskey: &ResKey) {
        println!("  [RECV] FORGET QUERYABLE ({:?})", reskey);
    }

    async fn data(&self, reskey: &ResKey, _reliable: bool, _info: &Option<RBuf>, _payload: RBuf) {
        println!("  [RECV] DATA ({:?})", reskey);
    }
    async fn query(&self, reskey: &ResKey, predicate: &str, qid: ZInt, target: QueryTarget, consolidation: QueryConsolidation) {
        println!("  [RECV] QUERY ({:?}) ({:?}) ({:?}) ({:?}) ({:?})", reskey, predicate, qid, target, consolidation);
    }
    async fn reply(&self, qid: ZInt, reply: &Reply) {
        println!("  [RECV] REPLY ({:?}) ({:?})", qid, reply);
    }
    async fn pull(&self, is_final: bool, reskey: &ResKey, pull_id: ZInt, max_samples: &Option<ZInt>) {
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
        let config = SessionManagerConfig {
            version: 0,
            whatami: whatami::CLIENT,
            id: PeerId{id: pid.clone()},
            handler: session_handler.clone()
        };
        let manager = SessionManager::new(config, None);

        let attachment = None;
        if let Some(locator) = args.next() {
            if let Err(_err) =  manager.open_session(&locator.parse().unwrap(), &attachment).await {
                println!("Unable to connect to {}!", locator);
                std::process::exit(-1);
            }
        }
    
        let primitives = Mux::new(session_handler.handler.lock().await.as_ref().unwrap().clone());

        let sub_info = SubInfo {
            reliability: Reliability::Reliable,
            mode: SubMode::Push,
            period: None
        };
        primitives.subscriber(&"/demo/**".to_string().into(), &sub_info).await;

        let res: ResKey = ["/demo/client/", &pid[0].to_string(), &pid[1].to_string(), &pid[2].to_string(), &pid[3].to_string()]
            .concat().into();
        loop {
            println!("[SEND] DATA ({:?})", &res);
            primitives.data(&res, true, &None, RBuf::from(vec![1])).await;
            task::sleep(std::time::Duration::from_millis(1000)).await;
        }
    });
}