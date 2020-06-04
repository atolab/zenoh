use async_trait::async_trait;
use async_std::sync::{Arc, RwLock};
use std::collections::HashMap;

use zenoh_protocol::core::{ZInt, ResKey};
use zenoh_protocol::io::RBuf;
use zenoh_protocol::proto::{Primitives, SubInfo, QueryTarget, QueryConsolidation, Reply, WhatAmI};

use crate::routing::broker::*;
use crate::routing::ownedprimitives::OwnedPrimitives;


pub struct Face {
    pub(super) id: usize,
    pub(super) whatami: WhatAmI,
    pub(super) primitives: OwnedPrimitives,
    pub(super) local_mappings: HashMap<u64, Arc<Resource>>,
    pub(super) remote_mappings: HashMap<u64, Arc<Resource>>,
    pub(super) subs: Vec<Arc<Resource>>,
    pub(super) qabl: Vec<Arc<Resource>>,
    pub(super) next_qid: ZInt,
    pub(super) pending_queries: HashMap<u64, Arc<Query>>,
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
            qabl: Vec::new(),
            next_qid: 0,
            pending_queries: HashMap::new(),
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
        let mut tables = self.tables.write().await;
        declare_resource(&mut tables, &mut self.face.clone(), rid, prefixid, suffix).await;
    }

    async fn forget_resource(&self, rid: u64) {
        let mut tables = self.tables.write().await;
        undeclare_resource(&mut tables, &mut self.face.clone(), rid).await;
    }
    
    async fn subscriber(&self, reskey: &ResKey, sub_info: &SubInfo) {
        let (prefixid, suffix) = reskey.into();
        let mut tables = self.tables.write().await;
        declare_subscription(&mut tables, &mut self.face.clone(), prefixid, suffix, sub_info).await;
    }

    async fn forget_subscriber(&self, reskey: &ResKey) {
        let (prefixid, suffix) = reskey.into();
        let mut tables = self.tables.write().await;
        undeclare_subscription(&mut tables, &mut self.face.clone(), prefixid, suffix).await;
    }
    
    async fn publisher(&self, _reskey: &ResKey) {}

    async fn forget_publisher(&self, _reskey: &ResKey) {}
    
    async fn queryable(&self, reskey: &ResKey) {
        let (prefixid, suffix) = reskey.into();
        let mut tables = self.tables.write().await;
        declare_queryable(&mut tables, &mut self.face.clone(), prefixid, suffix).await;
    }

    async fn forget_queryable(&self, reskey: &ResKey) {
        let (prefixid, suffix) = reskey.into();
        let mut tables = self.tables.write().await;
        undeclare_queryable(&mut tables, &mut self.face.clone(), prefixid, suffix).await;
    }

    async fn data(&self, reskey: &ResKey, reliable: bool, info: &Option<RBuf>, payload: RBuf) {
        let (prefixid, suffix) = reskey.into();
        let tables = self.tables.read().await;
        route_data(&tables, &self.face, prefixid, suffix, reliable, info, payload).await;
    }

    async fn query(&self, reskey: &ResKey, predicate: &str, qid: ZInt, target: QueryTarget, consolidation: QueryConsolidation) {
        let (prefixid, suffix) = reskey.into();
        let mut tables = self.tables.write().await;
        route_query(&mut tables, &self.face, prefixid, suffix, predicate, qid, target, consolidation).await;
    }

    async fn reply(&self, qid: ZInt, reply: &Reply) {
        let mut tables = self.tables.write().await;
        route_reply(&mut tables, &mut self.face.clone(), qid, reply).await;
    }

    async fn pull(&self, _is_final: bool, _reskey: &ResKey, _pull_id: ZInt, _max_samples: &Option<ZInt>) {}

    async fn close(&self) {
        Tables::close_face(&self.tables, &Arc::downgrade(&self.face)).await;
    }
}