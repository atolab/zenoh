use std::collections::HashMap;
use std::sync::Arc;
use spin::RwLock;
use crate::routing::session::Session;

macro_rules! opt_match { 
    ($expr:expr ; Some($some:ident) => $sblock:block None => $nblock:block) => {
        {let x = match $expr {
            Some($some) => {Some($sblock)}
            None => None
        };
        match x {
            Some(v) => {v}
            None => $nblock
        }}
    };
    ($expr:expr ; None => $nblock:block Some($some:ident) => $sblock:block) => {
        opt_match!($expr ; Some($some) => $sblock None => $nblock)
    };
}

pub struct Resource {
    pub(super) parent: Option<Arc<RwLock<Resource>>>,
    pub(super) suffix: String,
    pub(super) nonwild_prefix: Option<(Arc<RwLock<Resource>>, String)>,
    pub(super) childs: HashMap<String, Arc<RwLock<Resource>>>,
    pub(super) contexts: HashMap<u64, Arc<RwLock<Context>>>,
}

impl Resource {

    fn new(parent: &Arc<RwLock<Resource>>, suffix: &str) -> Resource {
        let nonwild_prefix = match &parent.read().nonwild_prefix {
            None => match suffix.contains('*') {
                true => {Some((parent.clone(), String::from(suffix)))}
                false => {None}
            }
            prefix => {prefix.clone()}
        };

        Resource {
            parent: Some(parent.clone()),
            suffix: String::from(suffix),
            nonwild_prefix: nonwild_prefix,
            childs: HashMap::new(),
            contexts: HashMap::new(),
        }
    }

    pub fn name(&self) -> String {
        match &self.parent {
            Some(parent) => {String::from([&parent.read().name() as &str, &self.suffix].concat())}
            None => {String::from("")}
        }
    }

    pub fn root() -> Arc<RwLock<Resource>> {
        Arc::new(RwLock::new(Resource {
            parent: None,
            suffix: String::from(""),
            nonwild_prefix: None,
            childs: HashMap::new(),
            contexts: HashMap::new(),
        }))
    }

    pub fn print_tree(from: &Arc<RwLock<Resource>>) {
        println!("{}", from.read().name());
        for (_, child) in &from.read().childs {
            Resource::print_tree(&child)
        }
    }

    #[inline]
    fn build_resource(from: &Arc<RwLock<Resource>>, suffix: &str) -> Arc<RwLock<Resource>> {
        let (chunk, rest) = match suffix[1..].find('/') {
            Some(idx) => {(&suffix[0..(idx+1)], &suffix[(idx+1)..])}
            None => (suffix, "")
        };

        opt_match!(from.read().childs.get(chunk) ;
            Some(res) => {Resource::make_resource(res, rest)}
            None => {
                let new = Arc::new(RwLock::new(Resource::new(from, chunk)));
                let res = Resource::make_resource(&new, rest);
                from.write().childs.insert(String::from(chunk), new);
                res
            }
        )
    }

    pub fn make_resource(from: &Arc<RwLock<Resource>>, suffix: &str) -> Arc<RwLock<Resource>> {
        if suffix.is_empty() {
            from.clone()
        } else {
            if suffix.starts_with('/') {
                Resource::build_resource(from, suffix)
            } else {
                opt_match!(&from.read().parent ;
                    Some(parent) => {Resource::make_resource(&parent, &[&from.read().suffix, suffix].concat())}
                    None => {Resource::build_resource(from, suffix)}
                )
            }
        }
    }
}

pub(super) struct Context {
    pub(super) session: Arc<RwLock<Session>>,
    pub(super) rid: Option<u64>,
    pub(super) subs: Option<bool>,
}