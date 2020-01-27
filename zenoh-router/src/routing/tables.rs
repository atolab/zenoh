use std::sync::{Arc, Weak};
use spin::RwLock;
use std::collections::{HashMap, LinkedList};
use crate::routing::resource::{Resource, Context};
use crate::routing::session::Session;
use zenoh_protocol::core::rname::intersect;

pub struct Tables {
    root_res: Arc<RwLock<Resource>>,
    sessions: HashMap<u64, Arc<RwLock<Session>>>,
}

impl Tables {

    pub fn new() -> Arc<RwLock<Tables>> {
        Arc::new(RwLock::new(Tables {
            root_res: Resource::root(),
            sessions: HashMap::new(),
        }))
    }

    pub fn print(tables: &Arc<RwLock<Tables>>) {
        Resource::print_tree(&tables.read().root_res)
    }

    pub fn declare_session(tables: &Arc<RwLock<Tables>>, sid: u64) -> Weak<RwLock<Session>> {
        let mut t = tables.write();
        if ! t.sessions.contains_key(&sid) {
            t.sessions.insert(sid, Session::new(sid));
        }
        Arc::downgrade(t.sessions.get(&sid).unwrap())
    }

    pub fn declare_resource(tables: &Arc<RwLock<Tables>>, sex: &Weak<RwLock<Session>>, rid: u64, rname: &str) {
        let t = tables.write();
        match sex.upgrade() {
            Some(sex) => {
                let mut wsex = sex.write();
                match wsex.mappings.get(&rid) {
                    Some(_res) => {
                        // if _res.read().name() != rname {
                        //     // TODO : mapping change 
                        // }
                    }
                    None => {
                        let res = Resource::make_resource(&t.root_res, rname);
                        {
                            let mut wres = res.write();
                            match wres.contexts.get(&wsex.id) {
                                Some(_ctx) => {}
                                None => {
                                    wres.contexts.insert(wsex.id, 
                                        Arc::new(RwLock::new(Context {
                                            session: sex.clone(),
                                            rid: Some(rid),
                                            subs: None,
                                        }))
                                    );
                                }
                            }
                        }
                        wsex.mappings.insert(rid, res);
                    }
                }
            }
            None => println!("Declare resource for closed session!")
        }
    }

    pub fn declare_subscription(tables: &Arc<RwLock<Tables>>, sex: &Weak<RwLock<Session>>, rid: u64, suffix: &str) {
        let _t = tables.write();
        match sex.upgrade() {
            Some(sex) => {
                let rsex = sex.read();
                match rsex.mappings.get(&rid) {
                    Some(prefix) => {
                        let res = Resource::make_resource(prefix, suffix);
                        let mut wres = res.write();
                        match wres.contexts.get(&rsex.id) {
                            Some(ctx) => {
                                ctx.write().subs = Some(false);
                            }
                            None => {
                                wres.contexts.insert(rsex.id, 
                                    Arc::new(RwLock::new(Context {
                                        session: sex.clone(),
                                        rid: None,
                                        subs: Some(false),
                                    }))
                                );
                            }
                        }
                    }
                    None => println!("Declare subscription for unknown rid {}!", rid)
                }
            }
            None => println!("Declare subscription for closed session!")
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

    fn get_matches_from(rname: &str, from: &Arc<RwLock<Resource>>) -> LinkedList<Arc<RwLock<Resource>>> {
        let mut matches = LinkedList::new();
        if from.read().parent.is_none() {
            for (_, child) in &from.read().childs {
                matches.append(&mut Tables::get_matches_from(rname, child));
            }
            return matches
        }

        if rname.is_empty() {
            if from.read().suffix == "/**" || from.read().suffix == "/" {
                matches.push_back(from.clone()); // weak ?
                for (_, child) in &from.read().childs {
                    matches.append(&mut Tables::get_matches_from(rname, child));
                }
            }
            return matches
        }
        let (chunk, rest) = Tables::fst_chunk(rname);
        if intersect(chunk, &from.read().suffix) {
            if rest.is_empty() || rest == "/" || rest == "/**" {
                matches.push_back(from.clone()) // weak ?
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

    pub fn get_matches(tables: &Arc<RwLock<Tables>>, rname: &str) -> LinkedList<Arc<RwLock<Resource>>> {
        let t = tables.read();
        Tables::get_matches_from(rname, &t.root_res)
    }

    fn get_best_key_(prefix: &Arc<RwLock<Resource>>, suffix: &str, sid: &u64, checkchilds: bool) -> (u64, String) {
        let rprefix = prefix.read();
        if checkchilds && ! suffix.is_empty() {
            let (chunk, rest) = Tables::fst_chunk(suffix);
            if let Some(child) = rprefix.childs.get(chunk) {
                return Tables::get_best_key_(child, rest, sid, true)
            }
        }
        if let Some(ctx) = rprefix.contexts.get(sid) {
            if let Some(rid) = ctx.read().rid {
                return (rid, suffix.to_string())
            }
        }
        match &rprefix.parent {
            Some(parent) => {Tables::get_best_key_(&parent, &[&rprefix.suffix, suffix].concat(), sid, false)}
            None => {(0, suffix.to_string())}
        }
    }

    #[inline]
    fn get_best_key(prefix: &Arc<RwLock<Resource>>, suffix: &str, sid: &u64) -> (u64, String) {
        Tables::get_best_key_(prefix, suffix, sid, true)
    }

    pub fn route_data(tables: &Arc<RwLock<Tables>>, sex: &Weak<RwLock<Session>>, rid: &u64, suffix: &str) 
    -> Option<HashMap<u64, (Weak<RwLock<Session>>, u64, String)>> {
        let t = tables.read();
        match sex.upgrade() {
            Some(sex) => {
                match sex.read().mappings.get(rid) {
                    Some(prefix) => {
                        let mut sexs = HashMap::new();
                        for res in &Tables::get_matches_from(&[&prefix.read().name(), suffix].concat(), &t.root_res) {
                            let rres = res.read();
                            for (sid, context) in &rres.contexts {
                                let rcontext = context.read();
                                if let Some(_) = rcontext.subs {
                                    let (rid, suffix) = Tables::get_best_key(prefix, suffix, sid);
                                    sexs.insert(*sid, (Arc::downgrade(&rcontext.session), rid, suffix));
                                }
                            }
                        }
                        Some(sexs)
                    }
                    None => {
                        if *rid == 0 {
                            let prefix = &t.root_res;
                            let mut sexs = HashMap::new();
                            for res in &Tables::get_matches_from(&[&prefix.read().name(), suffix].concat(), &t.root_res) {
                                let rres = res.read();
                                for (sid, context) in &rres.contexts {
                                    let rcontext = context.read();
                                    if let Some(_) = rcontext.subs {
                                        let (rid, suffix) = Tables::get_best_key(prefix, suffix, sid);
                                        sexs.insert(*sid, (Arc::downgrade(&rcontext.session), rid, suffix));
                                    }
                                }
                            }
                            Some(sexs)
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
