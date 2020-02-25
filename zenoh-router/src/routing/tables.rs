use std::sync::{Arc, Weak};
use spin::RwLock;
use std::collections::{HashMap};
use crate::routing::resource::*;
use crate::routing::face::Face;
use zenoh_protocol::core::rname::intersect;

pub struct Tables {
    root_res: Arc<RwLock<Resource>>,
    faces: HashMap<usize, Arc<RwLock<Face>>>,
}

impl Tables {

    pub fn new() -> Arc<RwLock<Tables>> {
        Arc::new(RwLock::new(Tables {
            root_res: Resource::root(),
            faces: HashMap::new(),
        }))
    }

    #[doc(hidden)]
    pub fn _get_root(&self) -> &Arc<RwLock<Resource>> {
        &self.root_res
    }

    pub fn print(tables: &Arc<RwLock<Tables>>) {
        Resource::print_tree(&tables.read().root_res)
    }

    pub fn declare_session(tables: &Arc<RwLock<Tables>>, sid: usize) -> Weak<RwLock<Face>> {
        let mut t = tables.write();
        if ! t.faces.contains_key(&sid) {
            t.faces.insert(sid, Face::new(sid));
        }
        Arc::downgrade(t.faces.get(&sid).unwrap())
    }

    pub fn undeclare_session(tables: &Arc<RwLock<Tables>>, sex: &Weak<RwLock<Face>>) {
        let mut t = tables.write();
        match sex.upgrade() {
            Some(sex) => {
                let mut wsex = sex.write();
                for (_, mapping) in &wsex.mappings {
                    Resource::clean(&mapping);
                }
                wsex.mappings.clear();
                while let Some(res) = wsex.subs.pop() {
                    Resource::clean(&res);
                }
                t.faces.remove(&wsex.id);
            }
            None => println!("Undeclare closed session!")
        }
    }

    fn build_direct_tables(res: &Arc<RwLock<Resource>>) {
        let mut dests = HashMap::new();
        for match_ in &res.read().matches {
            let match_ = &match_.upgrade().unwrap();
            let rmatch_ = match_.read();
            for (sid, context) in &rmatch_.contexts {
                let rcontext = context.read();
                if let Some(_) = rcontext.subs {
                    let (rid, suffix) = Tables::get_best_key(res, "", sid);
                    dests.insert(*sid, (Arc::downgrade(&rcontext.face), rid, suffix));
                }
            }
        }
        res.write().route = dests;
    }

    fn build_matches_direct_tables(res: &Arc<RwLock<Resource>>) {
        Tables::build_direct_tables(&res);
        for match_ in &res.read().matches {
            let match_ = &match_.upgrade().unwrap();
            if ! Arc::ptr_eq(match_, res) {
                Tables::build_direct_tables(match_);
            }
        }
    }

    fn make_and_match_resource(from: &Arc<RwLock<Resource>>, prefix: &Arc<RwLock<Resource>>, suffix: &str) -> Arc<RwLock<Resource>> {
        let res = Resource::make_resource(prefix, suffix);
        let matches = Tables::get_matches_from(suffix, from);

        fn matches_contain(matches: &Vec<Weak<RwLock<Resource>>>, res: &Arc<RwLock<Resource>>) -> bool {
            for match_ in matches {
                if Arc::ptr_eq(&match_.upgrade().unwrap(), res) {
                    return true;
                }
            }
            return false;
        }
        
        for match_ in &matches {
            let match_ = &match_.upgrade().unwrap();
            if ! matches_contain(&match_.read().matches, &res) {
                match_.write().matches.push(Arc::downgrade(&res));
            }
        }
        res.write().matches = matches;
        res
    }

    pub fn declare_resource(tables: &Arc<RwLock<Tables>>, sex: &Weak<RwLock<Face>>, rid: u64, prefixid: u64, suffix: &str) {
        let t = tables.write();
        match sex.upgrade() {
            Some(sex) => {
                let rsex = sex.read();
                match rsex.mappings.get(&rid) {
                    Some(_res) => {
                        // if _res.read().name() != rname {
                        //     // TODO : mapping change 
                        // }
                    }
                    None => {
                        let prefix = {
                            match prefixid {
                                0 => {Some(&t.root_res)}
                                prefixid => {
                                    match rsex.mappings.get(&prefixid) {
                                        Some(prefix) => {Some(prefix)}
                                        None => {None}
                                    }
                                }
                            }
                        };
                        match prefix {
                            Some(prefix) => {
                                let res = Tables::make_and_match_resource(&t.root_res, prefix, suffix);
                                {
                                    let mut wres = res.write();
                                    match wres.contexts.get(&rsex.id) {
                                        Some(_ctx) => {}
                                        None => {
                                            wres.contexts.insert(rsex.id, 
                                                Arc::new(RwLock::new(Context {
                                                    face: sex.clone(),
                                                    rid: Some(rid),
                                                    subs: None,
                                                }))
                                            );
                                        }
                                    }
                                }
                                drop(rsex);
                                Tables::build_matches_direct_tables(&res);
                                sex.write().mappings.insert(rid, res);
                            }
                            None => println!("Declare resource with unknown prefix {}!", prefixid)
                        }
                    }
                }
            }
            None => println!("Declare resource for closed session!")
        }
    }

    pub fn undeclare_resource(tables: &Arc<RwLock<Tables>>, sex: &Weak<RwLock<Face>>, rid: u64) {
        let _t = tables.write();
        match sex.upgrade() {
            Some(sex) => {
                let mut wsex = sex.write();
                match wsex.mappings.remove(&rid) {
                    Some(res) => {Resource::clean(&res)}
                    None => println!("Undeclare unknown resource!")
                }
            }
            None => println!("Undeclare resource for closed session!")
        }
    }

    pub fn declare_subscription(tables: &Arc<RwLock<Tables>>, sex: &Weak<RwLock<Face>>, prefixid: u64, suffix: &str) {
        let t = tables.write();
        match sex.upgrade() {
            Some(sex) => {
                let mut wsex = sex.write();
                let prefix = {
                    match prefixid {
                        0 => {Some(&t.root_res)}
                        prefixid => {
                            match wsex.mappings.get(&prefixid) {
                                Some(prefix) => {Some(prefix)}
                                None => {None}
                            }
                        }
                    }
                };
                match prefix {
                    Some(prefix) => {
                        let res = Tables::make_and_match_resource(&t.root_res, prefix, suffix);
                        {
                            let mut wres = res.write();
                            match wres.contexts.get(&wsex.id) {
                                Some(ctx) => {
                                    ctx.write().subs = Some(false);
                                }
                                None => {
                                    wres.contexts.insert(wsex.id, 
                                        Arc::new(RwLock::new(Context {
                                            face: sex.clone(),
                                            rid: None,
                                            subs: Some(false),
                                        }))
                                    );
                                }
                            }
                        }
                        Tables::build_matches_direct_tables(&res);
                        wsex.subs.push(res);
                    }
                    None => println!("Declare subscription for unknown rid {}!", prefixid)
                }
            }
            None => println!("Declare subscription for closed session!")
        }
    }

    pub fn undeclare_subscription(tables: &Arc<RwLock<Tables>>, sex: &Weak<RwLock<Face>>, prefixid: u64, suffix: &str) {
        let t = tables.write();
        match sex.upgrade() {
            Some(sex) => {
                let mut wsex = sex.write();
                let prefix = {
                    match prefixid {
                        0 => {Some(&t.root_res)}
                        prefixid => {
                            match wsex.mappings.get(&prefixid) {
                                Some(prefix) => {Some(prefix)}
                                None => {None}
                            }
                        }
                    }
                };
                match prefix {
                    Some(prefix) => {
                        match Resource::get_resource(prefix, suffix) {
                            Some(res) => {
                                let res = res.upgrade().unwrap();
                                {
                                    let wres = res.write();
                                    match wres.contexts.get(&wsex.id) {
                                        Some(ctx) => {
                                            ctx.write().subs = None;
                                        }
                                        None => {}
                                    }
                                }
                                wsex.subs.retain(|x| ! Arc::ptr_eq(&x, &res));
                                Resource::clean(&res)
                            }
                            None => println!("Undeclare unknown subscription!")
                        }
                    }
                    None => println!("Undeclare subscription with unknown prefix!")
                }
            }
            None => println!("Undeclare subscription for closed session!")
        }
    }

    fn fst_chunk(rname: &str) -> (&str, &str) {
        match rname.starts_with('/') {
            true => {
                match rname[1..].find('/') {
                    Some(idx) => {(&rname[0..(idx+1)], &rname[(idx+1)..])}
                    None => (rname, "")
                }
            }
            false => {
                match rname.find('/') {
                    Some(idx) => {(&rname[0..(idx)], &rname[(idx)..])}
                    None => (rname, "")
                }
            }
        }
    }

    fn get_matches_from(rname: &str, from: &Arc<RwLock<Resource>>) -> Vec<Weak<RwLock<Resource>>> {
        let mut matches = Vec::new();
        if from.read().parent.is_none() {
            for (_, child) in &from.read().childs {
                matches.append(&mut Tables::get_matches_from(rname, child));
            }
            return matches
        }
        if rname.is_empty() {
            if from.read().suffix == "/**" || from.read().suffix == "/" {
                matches.push(Arc::downgrade(from));
                for (_, child) in &from.read().childs {
                    matches.append(&mut Tables::get_matches_from(rname, child));
                }
            }
            return matches
        }
        let (chunk, rest) = Tables::fst_chunk(rname);
        if intersect(chunk, &from.read().suffix) {
            if rest.is_empty() || rest == "/" || rest == "/**" {
                matches.push(Arc::downgrade(from))
            } else if chunk == "/**" || from.read().suffix == "/**" {
                matches.append(&mut Tables::get_matches_from(rest, from));
            }
            for (_, child) in &from.read().childs {
                matches.append(&mut Tables::get_matches_from(rest, child));
                if chunk == "/**" || from.read().suffix == "/**" {
                    matches.append(&mut Tables::get_matches_from(rname, child));
                }
            }
        }
        matches
    }

    pub fn get_matches(tables: &Arc<RwLock<Tables>>, rname: &str) -> Vec<Weak<RwLock<Resource>>> {
        let t = tables.read();
        Tables::get_matches_from(rname, &t.root_res)
    }

    #[inline]
    fn get_best_key(prefix: &Arc<RwLock<Resource>>, suffix: &str, sid: &usize) -> (u64, String) {
        fn get_best_key_(prefix: &Arc<RwLock<Resource>>, suffix: &str, sid: &usize, checkchilds: bool) -> (u64, String) {
            let rprefix = prefix.read();
            if checkchilds && ! suffix.is_empty() {
                let (chunk, rest) = Tables::fst_chunk(suffix);
                if let Some(child) = rprefix.childs.get(chunk) {
                    return get_best_key_(child, rest, sid, true)
                }
            }
            if let Some(ctx) = rprefix.contexts.get(sid) {
                if let Some(rid) = ctx.read().rid {
                    return (rid, suffix.to_string())
                }
            }
            match &rprefix.parent {
                Some(parent) => {get_best_key_(&parent, &[&rprefix.suffix, suffix].concat(), sid, false)}
                None => {(0, suffix.to_string())}
            }
        }
        get_best_key_(prefix, suffix, sid, true)
    }

    pub fn route_data(tables: &Arc<RwLock<Tables>>, sex: &Weak<RwLock<Face>>, rid: &u64, suffix: &str) 
    -> Option<HashMap<usize, (Weak<RwLock<Face>>, u64, String)>> {

        let t = tables.read();

        let build_route = |prefix: &Arc<RwLock<Resource>>, suffix: &str| {
            let consolidate = |matches: &Vec<Weak<RwLock<Resource>>>| {
                let mut sexs = HashMap::new();
                for res in matches {
                    let res = res.upgrade().unwrap();
                    let rres = res.read();
                    for (sid, context) in &rres.contexts {
                        let rcontext = context.read();
                        if let Some(_) = rcontext.subs {
                            if ! sexs.contains_key(sid)
                            {
                                let (rid, suffix) = Tables::get_best_key(prefix, suffix, sid);
                                sexs.insert(*sid, (Arc::downgrade(&rcontext.face), rid, suffix));
                            }
                        }
                    }
                };
                sexs
            };
    
            Some(match Resource::get_resource(prefix, suffix) {
                Some(res) => {res.upgrade().unwrap().read().route.clone()}
                None => {consolidate(&Tables::get_matches_from(&[&prefix.read().name(), suffix].concat(), &t.root_res))}
            })
        };

        match sex.upgrade() {
            Some(sex) => {
                let rsex = sex.read();
                match rsex.mappings.get(rid) {
                    Some(res) => {
                        match suffix {
                            "" => {Some(res.read().route.clone())}
                            suffix => {
                                build_route(rsex.mappings.get(rid).unwrap(), suffix)
                            }
                        }
                    }
                    None => {
                        if *rid == 0 {
                            build_route(&t.root_res, suffix)
                        } else {
                            println!("Route data with unknown rid {}!", rid); None
                        }
                    }
                }
            }
            None => {println!("Declare subscription for closed session!"); None}
        }
    }
}
