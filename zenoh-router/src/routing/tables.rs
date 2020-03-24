use async_trait::async_trait;
use std::sync::{Arc, Weak};
use spin::RwLock;
use std::collections::{HashMap};
use zenoh_protocol::core::rname::intersect;
use zenoh_protocol::core::{ResKey, ZInt};
use zenoh_protocol::io::ArcSlice;
use zenoh_protocol::proto::{Primitives, SubInfo, SubMode, Reliability, Mux, DeMux, WhatAmI};
use zenoh_protocol::session::{SessionHandler, MsgHandler};
use crate::routing::resource::*;
use crate::routing::face::{Face, FaceHdl};

/// # Example: 
/// ```
///   use async_std::sync::Arc;
///   use zenoh_protocol::core::PeerId;
///   use zenoh_protocol::io::ArcSlice;
///   use zenoh_protocol::proto::WhatAmI::Peer;
///   use zenoh_protocol::session::SessionManager;
///   use zenoh_router::routing::tables::TablesHdl;
/// 
///   async{
///     // implement Primitives trait
///     use zenoh_protocol::proto::Mux;
///     use zenoh_protocol::session::DummyHandler;
///     let dummyPrimitives = Arc::new(Mux::new(Arc::new(DummyHandler::new())));
///   
///     // Instanciate routing tables
///     let tables = Arc::new(TablesHdl::new());
/// 
///     // Instanciate SessionManager and plug it to the routing tables
///     let manager = SessionManager::new(0, Peer, PeerId{id: vec![1, 2]}, 0, tables.clone());
/// 
///     // Declare new primitives
///     let primitives = tables.new_primitives(dummyPrimitives).await;
///     
///     // Use primitives
///     primitives.data(&"/demo".to_string().into(), true, &None, &ArcSlice::from(vec![1, 2])).await;
/// 
///     // Close primitives
///     primitives.close().await;
///   };
/// 
/// ```
pub struct TablesHdl {
    tables: Arc<RwLock<Tables>>,
}

impl TablesHdl {
    pub fn new() -> TablesHdl {
        TablesHdl {
            tables: Tables::new()
        }
    }
    
    pub async fn new_primitives(&self, primitives: Arc<dyn Primitives + Send + Sync>) -> Arc<dyn Primitives + Send + Sync> {
        Arc::new(FaceHdl {
            tables: self.tables.clone(), 
            face: Tables::declare_session(&self.tables, WhatAmI::Client, primitives).await.upgrade().unwrap(),
        })
    }
}

impl Default for TablesHdl {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SessionHandler for TablesHdl {
    async fn new_session(&self, whatami: WhatAmI, session: Arc<dyn MsgHandler + Send + Sync>) -> Arc<dyn MsgHandler + Send + Sync> {
        Arc::new(DeMux::new(FaceHdl {
            tables: self.tables.clone(), 
            face: Tables::declare_session(&self.tables, whatami, Arc::new(Mux::new(session))).await.upgrade().unwrap(),
        }))
    }
}

pub type Route = HashMap<usize, (Weak<RwLock<Face>>, u64, String)>;

pub struct Tables {
    sex_counter: usize,
    root_res: Arc<RwLock<Resource>>,
    faces: HashMap<usize, Arc<RwLock<Face>>>,
}

impl Tables {

    pub fn new() -> Arc<RwLock<Tables>> {
        Arc::new(RwLock::new(Tables {
            sex_counter: 0,
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

    pub async fn declare_session(tables: &Arc<RwLock<Tables>>, whatami: WhatAmI, primitives: Arc<dyn Primitives + Send + Sync>) -> Weak<RwLock<Face>> {
        let (face, subs) = {
            let mut t = tables.write();
            let sid = t.sex_counter;
            t.sex_counter += 1;
            let newface = t.faces.entry(sid).or_insert_with(|| Face::new(sid, whatami.clone(), primitives.clone())).clone();
            
            // @TODO temporarily propagate to everybody (clients)
            // if whatami != WhatAmI::Client {
            if true {
                let mut local_id = 0;
                let subs = t.faces.iter().map(|(id, face)| {
                    if *id != sid {
                        let rface = face.read();
                        rface.subs.iter().map(|sub| {
                            let (nonwild_prefix, wildsuffix) = {
                                let rsub = sub.read();
                                match &rsub.nonwild_prefix {
                                    None => {
                                        local_id += 1;
                                        (Some((sub.clone(), local_id)), "".to_string())
                                    }
                                    Some((nonwild_prefix, wildsuffix)) => {
                                        if ! nonwild_prefix.read().name().is_empty() {
                                            local_id += 1;
                                            (Some((nonwild_prefix.clone(), local_id)), wildsuffix.clone())
                                        }else {
                                            (None, rsub.name())
                                        }
                                    }
                                }
                            };

                            match nonwild_prefix {
                                Some((nonwild_prefix, local_id)) => {
                                    let mut wnonwild_prefix = nonwild_prefix.write();
                                    wnonwild_prefix.contexts.insert(sid, 
                                        Arc::new(RwLock::new(Context {
                                            face: newface.clone(),
                                            local_rid: Some(local_id),
                                            remote_rid: None,
                                            subs: None,
                                    })));
                                    newface.write().local_mappings.insert(local_id, nonwild_prefix.clone());
                                    (Some((wnonwild_prefix.name(), local_id)), wildsuffix)
                                }
                                None => {
                                    (None, wildsuffix)
                                }
                            }
                        }).collect::<Vec<(Option<(String, ZInt)>, String)>>()
                    } else {
                        vec![]
                    }
                }).collect::<Vec<Vec<(Option<(String, ZInt)>, String)>>>().concat();
                (Arc::downgrade(&newface), Some(subs))
            } else {
                (Arc::downgrade(&newface), None)
            }
        };

        if let Some(subs) = subs {
            // @TODO: manage propagation of Subscriber.info
            let sub_info = SubInfo { reliability: Reliability::Reliable, mode: SubMode::Push, period: None }; 
            for (nonwild_prefix, wildsuffix) in subs {
                match nonwild_prefix {
                    Some((nonwild_prefix, local_id)) => {
                        primitives.resource(local_id, &ResKey::RName(nonwild_prefix)).await;
                        primitives.subscriber(&ResKey::RIdWithSuffix(local_id, wildsuffix), &sub_info).await;
                    }
                    None => {
                        primitives.subscriber(&ResKey::RName(wildsuffix), &sub_info).await;
                    }
                }
            }
        }

        face
    }

    pub async fn undeclare_session(tables: &Arc<RwLock<Tables>>, sex: &Weak<RwLock<Face>>) {
        let mut t = tables.write();
        match sex.upgrade() {
            Some(sex) => {
                let mut wsex = sex.write();
                for mapping in wsex.remote_mappings.values() {
                    Resource::clean(&mapping);
                }
                wsex.remote_mappings.clear();
                for mapping in wsex.local_mappings.values() {
                    Resource::clean(&mapping);
                }
                wsex.local_mappings.clear();
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
                if rcontext.subs.is_some() {
                    let (rid, suffix) = Tables::get_best_key(res, "", *sid);
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
        let matches = Tables::get_matches_from(&res.read().name(), from);

        fn matches_contain(matches: &[Weak<RwLock<Resource>>], res: &Arc<RwLock<Resource>>) -> bool {
            for match_ in matches {
                if Arc::ptr_eq(&match_.upgrade().unwrap(), res) {
                    return true
                }
            }
            false
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

    pub async fn declare_resource(tables: &Arc<RwLock<Tables>>, sex: &Weak<RwLock<Face>>, rid: u64, prefixid: u64, suffix: &str) {
        let t = tables.write();
        match sex.upgrade() {
            Some(sex) => {
                let rsex = sex.read();
                match rsex.remote_mappings.get(&rid) {
                    Some(_res) => {
                        // if _res.read().name() != rname {
                        //     // TODO : mapping change 
                        // }
                    }
                    None => {
                        let prefix = {
                            match prefixid {
                                0 => {Some(&t.root_res)}
                                prefixid => {rsex.get_mapping(&prefixid)}
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
                                                    local_rid: None,
                                                    remote_rid: Some(rid),
                                                    subs: None,
                                                }))
                                            );
                                        }
                                    }
                                }
                                drop(rsex);
                                Tables::build_matches_direct_tables(&res);
                                sex.write().remote_mappings.insert(rid, res);
                            }
                            None => println!("Declare resource with unknown prefix {}!", prefixid)
                        }
                    }
                }
            }
            None => println!("Declare resource for closed session!")
        }
    }

    pub async fn undeclare_resource(tables: &Arc<RwLock<Tables>>, sex: &Weak<RwLock<Face>>, rid: u64) {
        let _t = tables.write();
        match sex.upgrade() {
            Some(sex) => {
                let mut wsex = sex.write();
                match wsex.remote_mappings.remove(&rid) {
                    Some(res) => {Resource::clean(&res)}
                    None => println!("Undeclare unknown resource!")
                }
            }
            None => println!("Undeclare resource for closed session!")
        }
    }

    pub async fn declare_subscription(tables: &Arc<RwLock<Tables>>, sex: &Weak<RwLock<Face>>, prefixid: u64, suffix: &str, sub_info: &SubInfo) {
        let route = {
            let t = tables.write();
            match sex.upgrade() {
                Some(sex) => {
                    let mut wsex = sex.write();
                    let prefix = {
                        match prefixid {
                            0 => {Some(&t.root_res)}
                            prefixid => {wsex.get_mapping(&prefixid)}
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
                                                local_rid: None,
                                                remote_rid: None,
                                                subs: Some(false),
                                            }))
                                        );
                                    }
                                }
                            }
                            
                            Tables::build_matches_direct_tables(&res);
                            
                            let mut wprefix = prefix.write();
                            let mut route = HashMap::new();

                            for (id, face) in &t.faces {
                                if wsex.id != *id {
                                    let mut wface = face.write();
                                    if wsex.whatami != WhatAmI::Peer || wface.whatami != WhatAmI::Peer {
                                        if let Some(ctx) = wprefix.contexts.get(id) {
                                            let mut wctx = ctx.write();
                                            if let Some(rid) = wctx.local_rid {
                                                route.insert(*id, (wface.primitives.clone(), false, rid, suffix));
                                            } else if let Some(rid) = wctx.remote_rid {
                                                route.insert(*id, (wface.primitives.clone(), false, rid, suffix));
                                            } else {
                                                let rid = wface.get_next_local_id();
                                                wctx.local_rid = Some(rid);
                                                wface.local_mappings.insert(rid, prefix.clone());
    
                                                route.insert(*id, (wface.primitives.clone(), true, rid, suffix));
                                            }
                                        } else {
                                            let rid = wface.get_next_local_id();
                                            wprefix.contexts.insert(*id, 
                                                Arc::new(RwLock::new(Context {
                                                    face: face.clone(),
                                                    local_rid: Some(rid),
                                                    remote_rid: None,
                                                    subs: None,
                                            })));
                                            wface.local_mappings.insert(rid, prefix.clone());

                                            route.insert(*id, (wface.primitives.clone(), true, rid, suffix));
                                        }
                                    }
                                }
                            }
                            let prefixname = wprefix.name();
                            drop(wprefix);
                            wsex.subs.push(res);
                            
                            Some((prefixname, route))
                        }
                        None => {println!("Declare subscription for unknown rid {}!", prefixid); None}
                    }
                }
                None => {println!("Declare subscription for closed session!"); None}
            }
        };
        // if let Some(faces) = result {
        //     for (face in faces {
        //         face.subscriber(&(name.clone().into()), sub_info).await;
        //     }
        // }
        if let Some((prefixname, faces)) = route {
            for (_id, (primitives, declare_res, rid, suffix)) in faces {
                if declare_res {
                    primitives.resource(rid, &(prefixname.clone()).into()).await
                }
                primitives.subscriber(&(rid, suffix).into(), sub_info).await
            }
        }
    }

    pub async fn undeclare_subscription(tables: &Arc<RwLock<Tables>>, sex: &Weak<RwLock<Face>>, prefixid: u64, suffix: &str) {
        let t = tables.write();
        match sex.upgrade() {
            Some(sex) => {
                let mut wsex = sex.write();
                let prefix = {
                    match prefixid {
                        0 => {Some(&t.root_res)}
                        prefixid => {wsex.get_mapping(&prefixid)}
                    }
                };
                match prefix {
                    Some(prefix) => {
                        match Resource::get_resource(prefix, suffix) {
                            Some(res) => {
                                let res = res.upgrade().unwrap();
                                {
                                    let wres = res.write();
                                    if let Some(ctx) = wres.contexts.get(&wsex.id) {
                                        ctx.write().subs = None;
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
        if rname.starts_with('/') {
            match rname[1..].find('/') {
                Some(idx) => {(&rname[0..(idx+1)], &rname[(idx+1)..])}
                None => (rname, "")
            }
        } else {
            match rname.find('/') {
                Some(idx) => {(&rname[0..(idx)], &rname[(idx)..])}
                None => (rname, "")
            }
        }
    }

    fn get_matches_from(rname: &str, from: &Arc<RwLock<Resource>>) -> Vec<Weak<RwLock<Resource>>> {
        let mut matches = Vec::new();
        if from.read().parent.is_none() {
            for child in from.read().childs.values() {
                matches.append(&mut Tables::get_matches_from(rname, child));
            }
            return matches
        }
        if rname.is_empty() {
            if from.read().suffix == "/**" || from.read().suffix == "/" {
                matches.push(Arc::downgrade(from));
                for child in from.read().childs.values() {
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
            for child in from.read().childs.values() {
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
    fn get_best_key(prefix: &Arc<RwLock<Resource>>, suffix: &str, sid: usize) -> (u64, String) {
        fn get_best_key_(prefix: &Arc<RwLock<Resource>>, suffix: &str, sid: usize, checkchilds: bool) -> (u64, String) {
            let rprefix = prefix.read();
            if checkchilds && ! suffix.is_empty() {
                let (chunk, rest) = Tables::fst_chunk(suffix);
                if let Some(child) = rprefix.childs.get(chunk) {
                    return get_best_key_(child, rest, sid, true)
                }
            }
            if let Some(ctx) = rprefix.contexts.get(&sid) {
                if let Some(rid) = ctx.read().local_rid {
                    return (rid, suffix.to_string())
                } else if let Some(rid) = ctx.read().remote_rid {
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

    pub fn route_data_to_map(tables: &Arc<RwLock<Tables>>, sex: &Weak<RwLock<Face>>, rid: u64, suffix: &str) 
    -> Option<Route> {

        let t = tables.read();

        let build_route = |prefix: &Arc<RwLock<Resource>>, suffix: &str| {
            let consolidate = |matches: &Vec<Weak<RwLock<Resource>>>| {
                let mut sexs = HashMap::new();
                for res in matches {
                    let res = res.upgrade().unwrap();
                    let rres = res.read();
                    for (sid, context) in &rres.contexts {
                        let rcontext = context.read();
                        if rcontext.subs.is_some() && ! sexs.contains_key(sid)
                        {
                            let (rid, suffix) = Tables::get_best_key(prefix, suffix, *sid);
                            sexs.insert(*sid, (Arc::downgrade(&rcontext.face), rid, suffix));
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
                match rsex.get_mapping(&rid) {
                    Some(res) => {
                        match suffix {
                            "" => {Some(res.read().route.clone())}
                            suffix => {
                                build_route(res, suffix)
                            }
                        }
                    }
                    None => {
                        if rid == 0 {
                            build_route(&t.root_res, suffix)
                        } else {
                            println!("Route data with unknown rid {}!", rid); None
                        }
                    }
                }
            }
            None => {println!("Route data for closed session!"); None}
        }
    }

    pub async fn route_data(tables: &Arc<RwLock<Tables>>, sex: &Weak<RwLock<Face>>, rid: u64, suffix: &str, reliable:bool, info: &Option<ArcSlice>, payload: &ArcSlice) {
        match sex.upgrade() {
            Some(strongsex) => {
                if let Some(outfaces) = Tables::route_data_to_map(tables, sex, rid, suffix) {
                    for (_id, (face, rid, suffix)) in outfaces {
                        if ! Weak::ptr_eq(sex, &face) {
                            // TODO move primitives out of inner mutability
                            let strongface = face.upgrade().unwrap();
                            let primitives = {
                                let rface = strongface.read();
                                if strongsex.read().whatami != WhatAmI::Peer || rface.whatami != WhatAmI::Peer {
                                    Some(rface.primitives.clone())
                                } else {
                                    None
                                }
                            };
                            if let Some(primitives) = primitives {
                                primitives.data(&(rid, suffix).into(), reliable, info, payload).await
                            }
                        }
                    }
                }
            }
            None => {println!("Route data for closed session!")}
        }
    }
}
