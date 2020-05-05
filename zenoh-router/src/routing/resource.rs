use std::collections::HashMap;
use std::sync::{Arc, Weak};
use zenoh_protocol::proto::SubInfo;
use crate::routing::face::Face;

pub struct Resource {
    pub(super) parent: Option<Arc<Resource>>,
    pub(super) suffix: String,
    pub(super) nonwild_prefix: Option<(Arc<Resource>, String)>,
    pub(super) childs: HashMap<String, Arc<Resource>>,
    pub(super) contexts: HashMap<usize, Arc<Context>>,
    pub(super) matches: Vec<Weak<Resource>>,
    pub(super) route: HashMap<usize, (Arc<Face>, u64, String)>
}

impl Resource {

    fn new(parent: &Arc<Resource>, suffix: &str) -> Resource {
        let nonwild_prefix = match &parent.nonwild_prefix {
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
            Some(parent) => {[&parent.name() as &str, &self.suffix].concat()}
            None => {String::from("")}
        }
    }

    pub fn nonwild_prefix(res: &Arc<Resource>) -> (Option<Arc<Resource>>, String) {
        match &res.nonwild_prefix {
            None => {
                (Some(res.clone()), "".to_string())
            }
            Some((nonwild_prefix, wildsuffix)) => {
                if ! nonwild_prefix.name().is_empty() {
                    (Some(nonwild_prefix.clone()), wildsuffix.clone())
                }else {
                    (None, res.name())
                }
            }
        }
    }

    pub fn is_key(&self) -> bool {
        !self.contexts.is_empty()
    }

    pub fn root() -> Arc<Resource> {
        Arc::new(Resource {
            parent: None,
            suffix: String::from(""),
            nonwild_prefix: None,
            childs: HashMap::new(),
            contexts: HashMap::new(),
            matches: Vec::new(),
            route: HashMap::new(),
        })
    }

    pub fn clean(res: &mut Arc<Resource>) {
        unsafe {
            let mut resclone = res.clone();
            let mutres = Arc::get_mut_unchecked(&mut resclone);
            if let Some(ref mut parent) = mutres.parent {
                if Arc::strong_count(res) <= 3 && res.childs.is_empty() {
                        for match_ in &mut mutres.matches {
                            let mut match_ = match_.upgrade().unwrap();
                            if ! Arc::ptr_eq(&match_, res) {
                                Arc::get_mut_unchecked(&mut match_).matches.retain(
                                    |x| ! Arc::ptr_eq(&x.upgrade().unwrap(), res));
                            }
                        }
                        {
                            Arc::get_mut_unchecked(parent).childs.remove(&res.suffix);
                        }
                        Resource::clean(parent);
                }
            }
        }
    }

    pub fn print_tree(from: &Arc<Resource>) {
        println!("{}", from.name());
        for match_ in &from.matches {
            println!("  -> {}", match_.upgrade().unwrap().name());
        }
        for child in from.childs.values() {
            Resource::print_tree(&child)
        }
    }


    pub fn make_resource(from: &mut Arc<Resource>, suffix: &str) -> Arc<Resource> {
        unsafe {
            if suffix.is_empty() {
                from.clone()
            } else if suffix.starts_with('/') {
                let (chunk, rest) = match suffix[1..].find('/') {
                    Some(idx) => {(&suffix[0..(idx+1)], &suffix[(idx+1)..])}
                    None => (suffix, "")
                };
        
                match Arc::get_mut_unchecked(from).childs.get_mut(chunk) {
                    Some(mut res) => {Resource::make_resource(&mut res, rest)}
                    None => {
                        let mut new = Arc::new(Resource::new(from, chunk));
                        let res = Resource::make_resource(&mut new, rest);
                        Arc::get_mut_unchecked(from).childs.insert(String::from(chunk), new);
                        res
                    }
                }
            } else {
                match from.parent.clone() {
                    Some(mut parent) => {Resource::make_resource(&mut parent, &[&from.suffix, suffix].concat())}
                    None => {
                        let (chunk, rest) = match suffix[1..].find('/') {
                            Some(idx) => {(&suffix[0..(idx+1)], &suffix[(idx+1)..])}
                            None => (suffix, "")
                        };

                        match Arc::get_mut_unchecked(from).childs.get_mut(chunk) {
                            Some(mut res) => {Resource::make_resource(&mut res, rest)}
                            None => {
                                let mut new = Arc::new(Resource::new(from, chunk));
                                let res = Resource::make_resource(&mut new, rest);
                                Arc::get_mut_unchecked(from).childs.insert(String::from(chunk), new);
                                res
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn get_resource(from: &Arc<Resource>, suffix: &str) -> Option<Arc<Resource>> {
        if suffix.is_empty() {
            Some(from.clone())
        } else if suffix.starts_with('/') {
            let (chunk, rest) = match suffix[1..].find('/') {
                Some(idx) => {(&suffix[0..(idx+1)], &suffix[(idx+1)..])}
                None => (suffix, "")
            };
    
            match from.childs.get(chunk) {
                Some(res) => {Resource::get_resource(res, rest)}
                None => {None}
            }
        } else {
            match &from.parent {
                Some(parent) => {Resource::get_resource(&parent, &[&from.suffix, suffix].concat())}
                None => {
                    let (chunk, rest) = match suffix[1..].find('/') {
                        Some(idx) => {(&suffix[0..(idx+1)], &suffix[(idx+1)..])}
                        None => (suffix, "")
                    };
            
                    match from.childs.get(chunk) {
                        Some(res) => {Resource::get_resource(res, rest)}
                        None => {None}
                    }
                }
            }
        }
    }
}

pub(super) struct Context {
    pub(super) face: Arc<Face>,
    pub(super) local_rid: Option<u64>,
    pub(super) remote_rid: Option<u64>,
    pub(super) subs: Option<SubInfo>,
    #[allow(dead_code)]
    pub(super) qabl: bool,
}