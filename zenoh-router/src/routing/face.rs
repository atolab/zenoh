use std::sync::Arc;
use spin::RwLock;
use std::collections::HashMap;
use crate::routing::resource::Resource;

pub struct Face {
    pub(super) id: usize,
    pub(super) mappings: HashMap<u64, Arc<RwLock<Resource>>>,
    pub(super) subs: Vec<Arc<RwLock<Resource>>>,
}

impl Face {
    pub(super) fn new(id: usize) -> Arc<RwLock<Face>> {
        Arc::new(RwLock::new(Face {
            id: id,
            mappings: HashMap::new(),
            subs: Vec::new(),
        }))
    }
}