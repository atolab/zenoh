use async_trait::async_trait;
use async_std::sync::RwLock;
use std::sync::Arc;
use std::collections::HashMap;
use zenoh_protocol::core::{ZInt, ResKey};
use zenoh_protocol::io::RBuf;
use zenoh_protocol::proto::{Primitives, SubInfo, QueryTarget, QueryConsolidation, Reply, WhatAmI};
use crate::routing::resource::Resource;
use crate::routing::tables::Tables;
use crate::routing::ownedprimitives::OwnedPrimitives;

pub struct Face {
    pub(super) id: usize,
    pub(super) whatami: WhatAmI,
    pub(super) primitives: OwnedPrimitives,
    pub(super) local_mappings: HashMap<u64, Arc<Resource>>,
    pub(super) remote_mappings: HashMap<u64, Arc<Resource>>,
    pub(super) subs: Vec<Arc<Resource>>,
}

impl Face {
    pub(super) fn new(id: usize, whatami: WhatAmI, primitives: Arc<dyn Primitives + Send + Sync>) -> Arc<Face> {
        Arc::new(Face {
            id,
            whatami,
            primitives: OwnedPrimitives::new(primitives),
            local_mappings: HashMap::new(),
            remote_mappings: HashMap::new(),
            subs: Vec::new(),
        })
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub(super) fn get_mapping(&self, prefixid: &ZInt) -> Option<&std::sync::Arc<Resource>> {
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
        while self.local_mappings.get(&id).is_some() || 
              self.remote_mappings.get(&id).is_some() {
            id += 1;
        }
        id
    }
}

pub struct FaceHdl {
    pub(super) tables: Arc<RwLock<Tables>>,
    pub(super) face: Arc<Face>,
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
    
    async fn queryable(&self, _reskey: &ResKey) {
        // let (prefixid, suffix) = reskey.into();
        // Tables::declare_queryable(&self.tables, &Arc::downgrade(&self.face), prefixid, suffix).await;
    }

    async fn forget_queryable(&self, _reskey: &ResKey) {
        // let (prefixid, suffix) = reskey.into();
        // Tables::undeclare_queryable(&self.tables, &Arc::downgrade(&self.face), prefixid, suffix).await;
    }

    async fn data(&self, reskey: &ResKey, reliable: bool, info: &Option<RBuf>, payload: RBuf) {
        let (prefixid, suffix) = reskey.into();
        Tables::route_data(&self.tables, &Arc::downgrade(&self.face), prefixid, suffix, reliable, info, payload).await;
    }

    async fn query(&self, _reskey: &ResKey, _predicate: &str, _qid: ZInt, _target: QueryTarget, _consolidation: QueryConsolidation) {}

    async fn reply(&self, _qid: ZInt, _reply: &Reply) {}

    async fn pull(&self, _is_final: bool, _reskey: &ResKey, _pull_id: ZInt, _max_samples: &Option<ZInt>) {}

    async fn close(&self) {
        Tables::undeclare_session(&self.tables, &Arc::downgrade(&self.face)).await;
    }
}