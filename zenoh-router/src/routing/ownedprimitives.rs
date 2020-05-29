use async_std::sync::Arc;

use zenoh_protocol::core::{ZInt, ResKey};
use zenoh_protocol::io::RBuf;
use zenoh_protocol::proto::{SubInfo, QueryTarget, QueryConsolidation, Primitives, Reply};


#[derive(Clone)]
pub struct OwnedPrimitives {
    primitives: Arc<dyn Primitives + Send + Sync>,
}

impl OwnedPrimitives {
    pub fn new(primitives: Arc<dyn Primitives + Send + Sync>) -> OwnedPrimitives {OwnedPrimitives {primitives}}

    pub async fn resource(self, rid: ZInt, reskey: ResKey) {
        self.primitives.resource(rid, &reskey).await
    }
    pub async fn forget_resource(self, rid: ZInt) {
        self.primitives.forget_resource(rid).await
    }
    
    pub async fn publisher(self, reskey: ResKey) {
        self.primitives.publisher(&reskey).await
    }
    pub async fn forget_publisher(self, reskey: ResKey) {
        self.primitives.forget_publisher(&reskey).await
    }
    
    pub async fn subscriber(self, reskey: ResKey, sub_info: SubInfo) {
        self.primitives.subscriber(&reskey, &sub_info).await
    }
    pub async fn forget_subscriber(self, reskey: ResKey) {
        self.primitives.forget_subscriber(&reskey).await
    }
    
    pub async fn queryable(self, reskey: ResKey) {
        self.primitives.queryable(&reskey).await
    }
    pub async fn forget_queryable(self, reskey: ResKey) {
        self.primitives.forget_queryable(&reskey).await
    }

    pub async fn data(self, reskey: ResKey, reliable: bool, info: Option<RBuf>, payload: RBuf) {
        self.primitives.data(&reskey, reliable, &info, payload).await
    }
    pub async fn query(self, reskey: ResKey, predicate: String, qid: ZInt, target: QueryTarget, consolidation: QueryConsolidation) {
        self.primitives.query(&reskey, &predicate, qid, target, consolidation).await
    }
    pub async fn reply(self, qid: ZInt, reply: Reply) {
        self.primitives.reply(qid, &reply).await
    }
    pub async fn pull(self, is_final: bool, reskey: ResKey, pull_id: ZInt, max_samples: Option<ZInt>){
        self.primitives.pull(is_final, &reskey, pull_id, &max_samples).await
    }

    pub async fn close(self) {
        self.primitives.close().await
    }
}