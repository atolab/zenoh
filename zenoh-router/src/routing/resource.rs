use std::collections::HashMap;
use std::sync::{Arc, Weak};
use spin::RwLock;
use crate::routing::face::Face;

pub struct Resource {
    pub(super) parent: Option<Arc<RwLock<Resource>>>,
    pub(super) suffix: String,
    pub(super) nonwild_prefix: Option<(Arc<RwLock<Resource>>, String)>,
    pub(super) childs: HashMap<String, Arc<RwLock<Resource>>>,
    pub(super) contexts: HashMap<usize, Arc<RwLock<Context>>>,
    pub(super) matches: Vec<Weak<RwLock<Resource>>>,
    pub(super) route: HashMap<usize, (Weak<RwLock<Face>>, u64, String)>
}

impl Resource {

    fn new(parent: &Arc<RwLock<Resource>>, suffix: &str) -> Resource {
        let nonwild_prefix = match &parent.read().nonwild_prefix {
            None => {
                if suffix.contains('*') {
                    Some((parent.clone(), String::from(suffix)))
                } else {
                    None
                }
            }
            Some((prefix, wildsuffix)) => {Some((prefix.clone(), [wildsuffix, suffix].concat()))}
        };

        Resource {
            parent: Some(parent.clone()),
            suffix: String::from(suffix),
            nonwild_prefix,
            childs: HashMap::new(),
            contexts: HashMap::new(),
            matches: Vec::new(),
            route: HashMap::new(),
        }
    }

    pub fn name(&self) -> String {
        match &self.parent {
            Some(parent) => {[&parent.read().name() as &str, &self.suffix].concat()}
            None => {String::from("")}
        }
    }

    pub fn is_key(&self) -> bool {
        !self.contexts.is_empty()
    }

    pub fn root() -> Arc<RwLock<Resource>> {
        Arc::new(RwLock::new(Resource {
            parent: None,
            suffix: String::from(""),
            nonwild_prefix: None,
            childs: HashMap::new(),
            contexts: HashMap::new(),
            matches: Vec::new(),
            route: HashMap::new(),
        }))
    }

    pub fn clean(res: &Arc<RwLock<Resource>>) {
        let rres = res.read();
        if let Some(parent) = &rres.parent {
            if Arc::strong_count(res) <= 2 && rres.childs.is_empty() {
                for match_ in &rres.matches {
                    let match_ = &match_.upgrade().unwrap();
                    if ! Arc::ptr_eq(match_, res) {
                        let mut wmatch = match_.write();
                        wmatch.matches.retain(|x| ! Arc::ptr_eq(&x.upgrade().unwrap(), res));
                    }
                }
                {
                    let mut wparent = parent.write();
                    wparent.childs.remove(&rres.suffix);
                }
                Resource::clean(parent);
            }
        }
    }

    pub fn print_tree(from: &Arc<RwLock<Resource>>) {
        println!("{}", from.read().name());
        for match_ in &from.read().matches.clone() {
            println!("  -> {}", match_.upgrade().unwrap().read().name());
        }
        for child in from.read().childs.values() {
            Resource::print_tree(&child)
        }
    }


    pub fn make_resource(from: &Arc<RwLock<Resource>>, suffix: &str) -> Arc<RwLock<Resource>> {
        if suffix.is_empty() {
            from.clone()
        } else if suffix.starts_with('/') {
            let (chunk, rest) = match suffix[1..].find('/') {
                Some(idx) => {(&suffix[0..(idx+1)], &suffix[(idx+1)..])}
                None => (suffix, "")
            };
    
            let rfrom = from.read();
            match rfrom.childs.get(chunk) {
                Some(res) => {Resource::make_resource(res, rest)}
                None => {
                    drop(rfrom);
                    let new = Arc::new(RwLock::new(Resource::new(from, chunk)));
                    let res = Resource::make_resource(&new, rest);
                    from.write().childs.insert(String::from(chunk), new);
                    res
                }
            }
        } else {
            let rfrom = from.read();
            match &rfrom.parent {
                Some(parent) => {Resource::make_resource(&parent, &[&rfrom.suffix, suffix].concat())}
                None => {
                    let (chunk, rest) = match suffix[1..].find('/') {
                        Some(idx) => {(&suffix[0..(idx+1)], &suffix[(idx+1)..])}
                        None => (suffix, "")
                    };

                    match rfrom.childs.get(chunk) {
                        Some(res) => {Resource::make_resource(res, rest)}
                        None => {
                            drop(rfrom);
                            let new = Arc::new(RwLock::new(Resource::new(from, chunk)));
                            let res = Resource::make_resource(&new, rest);
                            from.write().childs.insert(String::from(chunk), new);
                            res
                        }
                    }
                }
            }
        }
    }

    pub fn get_resource(from: &Arc<RwLock<Resource>>, suffix: &str) -> Option<Weak<RwLock<Resource>>> {
        if suffix.is_empty() {
            Some(Arc::downgrade(from))
        } else if suffix.starts_with('/') {
            let (chunk, rest) = match suffix[1..].find('/') {
                Some(idx) => {(&suffix[0..(idx+1)], &suffix[(idx+1)..])}
                None => (suffix, "")
            };
    
            match from.read().childs.get(chunk) {
                Some(res) => {Resource::get_resource(res, rest)}
                None => {None}
            }
        } else {
            let rfrom = from.read();
            match &rfrom.parent {
                Some(parent) => {Resource::get_resource(&parent, &[&rfrom.suffix, suffix].concat())}
                None => {
                    let (chunk, rest) = match suffix[1..].find('/') {
                        Some(idx) => {(&suffix[0..(idx+1)], &suffix[(idx+1)..])}
                        None => (suffix, "")
                    };
            
                    match rfrom.childs.get(chunk) {
                        Some(res) => {Resource::get_resource(res, rest)}
                        None => {None}
                    }
                }
            }
        }
    }
}

pub(super) struct Context {
    pub(super) face: Arc<RwLock<Face>>,
    pub(super) local_rid: Option<u64>,
    pub(super) remote_rid: Option<u64>,
    pub(super) subs: Option<bool>,
}