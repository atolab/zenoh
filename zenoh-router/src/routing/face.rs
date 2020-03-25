use async_trait::async_trait;
use std::sync::Arc;
use spin::RwLock;
use std::collections::HashMap;
use zenoh_protocol::core::{ZInt, PeerId, ResKey};
use zenoh_protocol::io::ArcSlice;
use zenoh_protocol::proto::{Primitives, SubInfo, QueryTarget, QueryConsolidation, ReplySource, WhatAmI};
use crate::routing::resource::Resource;
use crate::routing::tables::Tables;

pub struct Face {
    pub(super) id: usize,
    pub(super) whatami: WhatAmI,
    pub(super) primitives: Arc<dyn Primitives + Send + Sync>,
    pub(super) local_mappings: HashMap<u64, Arc<RwLock<Resource>>>,
    pub(super) remote_mappings: HashMap<u64, Arc<RwLock<Resource>>>,
    pub(super) subs: Vec<Arc<RwLock<Resource>>>,
    pub(super) stos: Vec<Arc<RwLock<Resource>>>,
}

impl Face {
    pub(super) fn new(id: usize, whatami: WhatAmI, primitives: Arc<dyn Primitives + Send + Sync>) -> Arc<RwLock<Face>> {
        Arc::new(RwLock::new(Face {
            id,
            whatami,
            primitives,
            local_mappings: HashMap::new(),
            remote_mappings: HashMap::new(),
            subs: Vec::new(),
            stos: Vec::new(),
        }))
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub(super) fn get_mapping(&self, prefixid: &ZInt) -> Option<&std::sync::Arc<RwLock<Resource>>> {
        match self.remote_mappings.get(prefixid) {
            Some(prefix) => {Some(prefix)}
            None => {
                match self.local_mappings.get(prefixid) {
                    Some(prefix) => {Some(prefix)}
                    None => {None}
                }
            }
        }
    }

    pub(super) fn get_next_local_id(&self) -> ZInt {
        let mut id = 1;
        while self.local_mappings.get(&id).is_some() {
            id += 1;
        }
        id
    }
}

pub struct FaceHdl {
    pub(super) tables: Arc<RwLock<Tables>>,
    pub(super) face: Arc<RwLock<Face>>,
}

#[async_trait]
impl Primitives for FaceHdl {
    async fn resource(&self, rid: u64, reskey: &ResKey) {
        let (prefixid, suffix) = reskey.into();
        Tables::declare_resource(&self.tables, &Arc::downgrade(&self.face), rid, prefixid, suffix).await;
    }

    async fn forget_resource(&self, rid: u64) {
        Tables::undeclare_resource(&self.tables, &Arc::downgrade(&self.face), rid).await;
    }
    
    async fn subscriber(&self, reskey: &ResKey, sub_info: &SubInfo) {
        let (prefixid, suffix) = reskey.into();
        Tables::declare_subscription(&self.tables, &Arc::downgrade(&self.face), prefixid, suffix, sub_info).await;
    }

    async fn forget_subscriber(&self, reskey: &ResKey) {
        let (prefixid, suffix) = reskey.into();
        Tables::undeclare_subscription(&self.tables, &Arc::downgrade(&self.face), prefixid, suffix).await;
    }
    
    async fn publisher(&self, _reskey: &ResKey) {}

    async fn forget_publisher(&self, _reskey: &ResKey) {}
    
    async fn storage(&self, reskey: &ResKey) {
        let (prefixid, suffix) = reskey.into();
        Tables::declare_storage(&self.tables, &Arc::downgrade(&self.face), prefixid, suffix).await;
    }

    async fn forget_storage(&self, reskey: &ResKey) {
        let (prefixid, suffix) = reskey.into();
        Tables::undeclare_storage(&self.tables, &Arc::downgrade(&self.face), prefixid, suffix).await;
    }
    
    async fn eval(&self, _reskey: &ResKey) {}

    async fn forget_eval(&self, _reskey: &ResKey) {}

    async fn data(&self, reskey: &ResKey, reliable: bool, info: &Option<ArcSlice>, payload: &ArcSlice) {
        let (prefixid, suffix) = reskey.into();
        Tables::route_data(&self.tables, &Arc::downgrade(&self.face), prefixid, suffix, reliable, info, payload).await;
    }

    async fn query(&self, _reskey: &ResKey, _predicate: &str, _qid: ZInt, _target: QueryTarget, _consolidation: QueryConsolidation) {}

    async fn reply(&self, _qid: ZInt, _source: &ReplySource, _replierid: &Option<PeerId>, _reskey: &ResKey, _info: &Option<ArcSlice>, _payload: &ArcSlice) {}

    async fn pull(&self, _is_final: bool, _reskey: &ResKey, _pull_id: ZInt, _max_samples: &Option<ZInt>) {}

    async fn close(&self) {
        Tables::undeclare_session(&self.tables, &Arc::downgrade(&self.face)).await;
    }
}