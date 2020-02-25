use std::sync::Arc;
use spin::RwLock;
use std::collections::HashMap;
use crate::routing::resource::Resource;

pub struct Session {
    pub(super) id: usize,
    pub(super) mappings: HashMap<u64, Arc<RwLock<Resource>>>,
    pub(super) subs: Vec<Arc<RwLock<Resource>>>,
}

impl Session {
    pub(super) fn new(id: usize) -> Arc<RwLock<Session>> {
        Arc::new(RwLock::new(Session {
            id: id,
            mappings: HashMap::new(),
            subs: Vec::new(),
        }))
    }
}