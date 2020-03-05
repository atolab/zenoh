use async_trait::async_trait;
use crate::core::{ZInt, PeerId, ResKey};
use crate::io::ArcSlice;
use crate::proto::{SubMode, QueryTarget, QueryConsolidation, ReplySource};

#[async_trait]
pub trait Primitives {
    async fn resource(&self, rid: &ZInt, reskey: &ResKey);
    async fn forget_resource(&self, rid: &ZInt);
    
    async fn publisher(&self, reskey: &ResKey);
    async fn forget_publisher(&self, reskey: &ResKey);
    
    async fn subscriber(&self, reskey: &ResKey, mode: &SubMode);
    async fn forget_subscriber(&self, reskey: &ResKey);
    
    async fn storage(&self, reskey: &ResKey);
    async fn forget_storage(&self, reskey: &ResKey);
    
    async fn eval(&self, reskey: &ResKey);
    async fn forget_eval(&self, reskey: &ResKey);

    async fn data(&self, reskey: &ResKey, info: &Option<ArcSlice>, payload: &ArcSlice);
    async fn query(&self, reskey: &ResKey, predicate: &String, qid: &ZInt, target: &Option<QueryTarget>, consolidation: &QueryConsolidation);
    async fn reply(&self, qid: &ZInt, source: &ReplySource, replierid: &Option<PeerId>, reskey: &ResKey, info: &Option<ArcSlice>, payload: &ArcSlice);
    async fn pull(&self, is_final: bool, reskey: &ResKey, pull_id: &ZInt, max_samples: &Option<ZInt>);
}