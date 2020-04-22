use async_trait::async_trait;
use async_std::sync::RwLock;
use std::sync::{Arc, Weak};
use std::collections::{HashMap};
use zenoh_protocol::core::rname::intersect;
use zenoh_protocol::core::ResKey;
use zenoh_protocol::io::RBuf;
use zenoh_protocol::proto::{Primitives, SubInfo, SubMode, Reliability, Mux, DeMux, WhatAmI};
use zenoh_protocol::session::{SessionHandler, MsgHandler};
use crate::routing::resource::*;
use crate::routing::face::{Face, FaceHdl};

/// # Example: 
/// ```
///   use async_std::sync::Arc;
///   use zenoh_protocol::core::PeerId;
///   use zenoh_protocol::io::RBuf;
///   use zenoh_protocol::proto::WhatAmI::Peer;
///   use zenoh_protocol::session::{SessionManager, SessionManagerConfig};
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
///     let config = SessionManagerConfig {
///         version: 0,
///         whatami: Peer,
///         id: PeerId{id: vec![1, 2]},
///         handler: tables.clone(),
///         lease: None,        // Use the default lease
///         resolution: None,   // Use the default sequence number resolution
///         batchsize: None,    // Use the default batch size
///         timeout: None       // Use the default timeout when opening a session
///     };
///     let manager = SessionManager::new(config);
/// 
///     // Declare new primitives
///     let primitives = tables.new_primitives(dummyPrimitives).await;
///     
///     // Use primitives
///     primitives.data(&"/demo".to_string().into(), true, &None, RBuf::from(vec![1, 2])).await;
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

pub type Route = HashMap<usize, (Arc<Face>, u64, String)>;

pub struct Tables {
    sex_counter: usize,
    root_res: Arc<Resource>,
    faces: HashMap<usize, Arc<Face>>,
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
    pub fn _get_root(&self) -> &Arc<Resource> {
        &self.root_res
    }

    pub async fn print(tables: &Arc<RwLock<Tables>>) {
        Resource::print_tree(&tables.read().await.root_res)
    }

    pub async fn declare_session(tables: &Arc<RwLock<Tables>>, whatami: WhatAmI, primitives: Arc<dyn Primitives + Send + Sync>) -> Weak<Face> {
        unsafe {
            let mut t = tables.write().await;
            let sid = t.sex_counter;
            t.sex_counter += 1;
            let mut newface = t.faces.entry(sid).or_insert_with(|| Face::new(sid, whatami.clone(), primitives.clone())).clone();
            
            // @TODO temporarily propagate to everybody (clients)
            // if whatami != WhatAmI::Client {
            if true {
                let mut local_id = 0;
                for (id, face) in t.faces.iter() {
                    if *id != sid {
                        for sub in face.subs.iter() {
                            let (nonwild_prefix, wildsuffix) = {
                                match &sub.nonwild_prefix {
                                    None => {
                                        local_id += 1;
                                        (Some((sub.clone(), local_id)), "".to_string())
                                    }
                                    Some((nonwild_prefix, wildsuffix)) => {
                                        if ! nonwild_prefix.name().is_empty() {
                                            local_id += 1;
                                            (Some((nonwild_prefix.clone(), local_id)), wildsuffix.clone())
                                        }else {
                                            (None, sub.name())
                                        }
                                    }
                                }
                            };

                            match nonwild_prefix {
                                Some((mut nonwild_prefix, local_id)) => {
                                    Arc::get_mut_unchecked(&mut nonwild_prefix).contexts.insert(sid, 
                                        Arc::new(Context {
                                            face: newface.clone(),
                                            local_rid: Some(local_id),
                                            remote_rid: None,
                                            subs: None,
                                            stor: false,
                                            eval: false,
                                    }));
                                    Arc::get_mut_unchecked(&mut newface).local_mappings.insert(local_id, nonwild_prefix.clone());

                                    let sub_info = SubInfo { reliability: Reliability::Reliable, mode: SubMode::Push, period: None }; 
                                    primitives.resource(local_id, &ResKey::RName(nonwild_prefix.name())).await;
                                    primitives.subscriber(&ResKey::RIdWithSuffix(local_id, wildsuffix), &sub_info).await;
                                }
                                None => {
                                    let sub_info = SubInfo { reliability: Reliability::Reliable, mode: SubMode::Push, period: None }; 
                                    primitives.subscriber(&ResKey::RName(wildsuffix), &sub_info).await;
                                }
                            }
                        }
                    }
                }

                for (id, face) in t.faces.iter() {
                    if *id != sid {
                        for sto in face.stos.iter() {
                            let (nonwild_prefix, wildsuffix) = {
                                match &sto.nonwild_prefix {
                                    None => {
                                        local_id += 1;
                                        (Some((sto.clone(), local_id)), "".to_string())
                                    }
                                    Some((nonwild_prefix, wildsuffix)) => {
                                        if ! nonwild_prefix.name().is_empty() {
                                            local_id += 1;
                                            (Some((nonwild_prefix.clone(), local_id)), wildsuffix.clone())
                                        }else {
                                            (None, sto.name())
                                        }
                                    }
                                }
                            };

                            match nonwild_prefix {
                                Some((mut nonwild_prefix, local_id)) => {
                                    Arc::get_mut_unchecked(&mut nonwild_prefix).contexts.insert(sid, 
                                        Arc::new(Context {
                                            face: newface.clone(),
                                            local_rid: Some(local_id),
                                            remote_rid: None,
                                            subs: None,
                                            stor: false,
                                            eval: false,
                                    }));
                                    Arc::get_mut_unchecked(&mut newface).local_mappings.insert(local_id, nonwild_prefix.clone());
                                    
                                    primitives.resource(local_id, &ResKey::RName(nonwild_prefix.name())).await;
                                    primitives.storage(&ResKey::RIdWithSuffix(local_id, wildsuffix)).await;
                                }
                                None => {
                                    primitives.storage(&ResKey::RName(wildsuffix)).await;
                                }
                            }
                        }
                    }
                }
            }
            Arc::downgrade(&newface)
        }
    }

    pub async fn undeclare_session(tables: &Arc<RwLock<Tables>>, sex: &Weak<Face>) {
        let mut t = tables.write().await;
        match sex.upgrade() {
            Some(mut sex) => unsafe {
                let sex = Arc::get_mut_unchecked(&mut sex);
                for mut mapping in sex.remote_mappings.values_mut() {
                    Resource::clean(&mut mapping);
                }
                sex.remote_mappings.clear();
                for mut mapping in sex.local_mappings.values_mut() {
                    Resource::clean(&mut mapping);
                }
                sex.local_mappings.clear();
                while let Some(mut res) = sex.subs.pop() {
                    Resource::clean(&mut res);
                }
                t.faces.remove(&sex.id);
            }
            None => println!("Undeclare closed session!")
        }
    }

    unsafe fn build_direct_tables(res: &mut Arc<Resource>) {
        let mut dests = HashMap::new();
        for match_ in &res.matches {
            for (sid, context) in &match_.upgrade().unwrap().contexts {
                if context.subs.is_some() || context.stor {
                    let (rid, suffix) = Tables::get_best_key(res, "", *sid);
                    dests.insert(*sid, (context.face.clone(), rid, suffix));
                }
            }
        }
        Arc::get_mut_unchecked(res).route = dests;
    }

    unsafe fn build_matches_direct_tables(res: &mut Arc<Resource>) {
        Tables::build_direct_tables(res);

        let resclone = res.clone();
        for match_ in &mut Arc::get_mut_unchecked(res).matches {
            if ! Arc::ptr_eq(&match_.upgrade().unwrap(), &resclone) {
                Tables::build_direct_tables(&mut match_.upgrade().unwrap());
            }
        }
    }

    unsafe fn make_and_match_resource(from: &Arc<Resource>, prefix: &mut Arc<Resource>, suffix: &str) -> Arc<Resource> {
        let mut res = Resource::make_resource(prefix, suffix);
        let mut matches = Tables::get_matches_from(&res.name(), from);

        fn matches_contain(matches: &[Weak<Resource>], res: &Arc<Resource>) -> bool {
            for match_ in matches {
                if Arc::ptr_eq(&match_.upgrade().unwrap(), res) {
                    return true
                }
            }
            false
        }
        
        for match_ in &mut matches {
            let mut match_ = match_.upgrade().unwrap();
            if ! matches_contain(&match_.matches, &res) {
                Arc::get_mut_unchecked(&mut match_).matches.push(Arc::downgrade(&res));
            }
        }
        Arc::get_mut_unchecked(&mut res).matches = matches;
        res
    }

    pub async fn declare_resource(tables: &Arc<RwLock<Tables>>, sex: &Weak<Face>, rid: u64, prefixid: u64, suffix: &str) {
        let t = tables.write().await;
        match sex.upgrade() {
            Some(mut sex) => {
                match sex.remote_mappings.get(&rid) {
                    Some(_res) => {
                        // if _res.read().name() != rname {
                        //     // TODO : mapping change 
                        // }
                    }
                    None => {
                        let prefix = {
                            match prefixid {
                                0 => {Some(t.root_res.clone())}
                                prefixid => {sex.get_mapping(&prefixid).cloned()}
                            }
                        };
                        match prefix {
                            Some(mut prefix) => unsafe {
                                let mut res = Tables::make_and_match_resource(&t.root_res, &mut prefix, suffix);
                                let mut ctx = Arc::get_mut_unchecked(&mut res).contexts.entry(sex.id).or_insert_with( ||
                                    Arc::new(Context {
                                        face: sex.clone(),
                                        local_rid: None,
                                        remote_rid: Some(rid),
                                        subs: None,
                                        stor: false,
                                        eval: false,
                                    })
                                ).clone();

                                if sex.local_mappings.get(&rid).is_some() && ctx.local_rid == None {
                                    let local_rid = Arc::get_mut_unchecked(&mut sex).get_next_local_id();
                                    Arc::get_mut_unchecked(&mut ctx).local_rid = Some(local_rid);

                                    Arc::get_mut_unchecked(&mut sex).local_mappings.insert(local_rid, res.clone());

                                    sex.primitives.clone().resource(local_rid, ResKey::RName(res.name())).await;
                                }

                                Arc::get_mut_unchecked(&mut sex).remote_mappings.insert(rid, res.clone());
                                Tables::build_matches_direct_tables(&mut res);
                            }
                            None => println!("Declare resource with unknown prefix {}!", prefixid)
                        }
                    }
                }
            }
            None => println!("Declare resource for closed session!")
        }
    }

    pub async fn undeclare_resource(tables: &Arc<RwLock<Tables>>, sex: &Weak<Face>, rid: u64) {
        let _t = tables.write().await;
        match sex.upgrade() {
            Some(mut sex) => unsafe {
                match Arc::get_mut_unchecked(&mut sex).remote_mappings.remove(&rid) {
                    Some(mut res) => {Resource::clean(&mut res)}
                    None => println!("Undeclare unknown resource!")
                }
            }
            None => println!("Undeclare resource for closed session!")
        }
    }

    pub async fn declare_subscription(tables: &Arc<RwLock<Tables>>, sex: &Weak<Face>, prefixid: u64, suffix: &str, sub_info: &SubInfo) {
        let mut t = tables.write().await;
        match sex.upgrade() {
            Some(mut sex) => {
                let prefix = {
                    match prefixid {
                        0 => {Some(t.root_res.clone())}
                        prefixid => {sex.get_mapping(&prefixid).cloned()}
                    }
                };
                match prefix {
                    Some(mut prefix) => unsafe {
                        let mut res = Tables::make_and_match_resource(&t.root_res, &mut prefix, suffix);
                        {
                            let res = Arc::get_mut_unchecked(&mut res);
                            match res.contexts.get_mut(&sex.id) {
                                Some(mut ctx) => {
                                    Arc::get_mut_unchecked(&mut ctx).subs = Some(sub_info.clone());
                                }
                                None => {
                                    res.contexts.insert(sex.id, 
                                        Arc::new(Context {
                                            face: sex.clone(),
                                            local_rid: None,
                                            remote_rid: None,
                                            subs: Some(sub_info.clone()),
                                            stor: false,
                                            eval: false,
                                        })
                                    );
                                }
                            }
                        }

                        for (id, face) in &mut t.faces {
                            if sex.id != *id && (sex.whatami != WhatAmI::Peer || face.whatami != WhatAmI::Peer) {
                                if let Some(mut ctx) = Arc::get_mut_unchecked(&mut prefix).contexts.get_mut(id) {
                                    if let Some(rid) = ctx.local_rid {
                                        face.primitives.clone().subscriber((rid, suffix).into(), sub_info.clone()).await;
                                    } else if let Some(rid) = ctx.remote_rid {
                                        face.primitives.clone().subscriber((rid, suffix).into(), sub_info.clone()).await;
                                    } else {
                                        let rid = face.get_next_local_id();
                                        Arc::get_mut_unchecked(&mut ctx).local_rid = Some(rid);
                                        Arc::get_mut_unchecked(face).local_mappings.insert(rid, prefix.clone());

                                        face.primitives.clone().resource(rid, prefix.name().into()).await;
                                        face.primitives.clone().subscriber((rid, suffix).into(), sub_info.clone()).await;
                                    }
                                } else {
                                    let rid = face.get_next_local_id();
                                    Arc::get_mut_unchecked(&mut prefix).contexts.insert(*id, 
                                        Arc::new(Context {
                                            face: face.clone(),
                                            local_rid: Some(rid),
                                            remote_rid: None,
                                            subs: None,
                                            stor: false,
                                            eval: false,
                                    }));
                                    Arc::get_mut_unchecked(face).local_mappings.insert(rid, prefix.clone());

                                    face.primitives.clone().resource(rid, prefix.name().into()).await;
                                    face.primitives.clone().subscriber((rid, suffix).into(), sub_info.clone()).await;
                                }
                            }
                        }
                        Tables::build_matches_direct_tables(&mut res);
                        Arc::get_mut_unchecked(&mut sex).subs.push(res);
                    }
                    None => println!("Declare subscription for unknown rid {}!", prefixid)
                }
            }
            None => println!("Declare subscription for closed session!")
        }
    }

    pub async fn undeclare_subscription(tables: &Arc<RwLock<Tables>>, sex: &Weak<Face>, prefixid: u64, suffix: &str) {
        let t = tables.write().await;
        match sex.upgrade() {
            Some(mut sex) => {
                let prefix = {
                    match prefixid {
                        0 => {Some(&t.root_res)}
                        prefixid => {sex.get_mapping(&prefixid)}
                    }
                };
                match prefix {
                    Some(prefix) => {
                        match Resource::get_resource(prefix, suffix) {
                            Some(mut res) => unsafe {
                                if let Some(mut ctx) = Arc::get_mut_unchecked(&mut res).contexts.get_mut(&sex.id) {
                                    Arc::get_mut_unchecked(&mut ctx).subs = None;
                                }
                                Arc::get_mut_unchecked(&mut sex).subs.retain(|x| ! Arc::ptr_eq(&x, &res));
                                Resource::clean(&mut res)
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

    pub async fn declare_storage(tables: &Arc<RwLock<Tables>>, sex: &Weak<Face>, prefixid: u64, suffix: &str) {
        let mut t = tables.write().await;
        match sex.upgrade() {
            Some(mut sex) => {
                let prefix = {
                    match prefixid {
                        0 => {Some(t.root_res.clone())}
                        prefixid => {sex.get_mapping(&prefixid).cloned()}
                    }
                };
                match prefix {
                    Some(mut prefix) => unsafe {
                        let mut res = Tables::make_and_match_resource(&t.root_res, &mut prefix, suffix);
                        {
                            match Arc::get_mut_unchecked(&mut res).contexts.get_mut(&sex.id) {
                                Some(mut ctx) => {
                                    Arc::get_mut_unchecked(&mut ctx).stor = true;
                                }
                                None => {
                                    Arc::get_mut_unchecked(&mut res).contexts.insert(sex.id, 
                                        Arc::new(Context {
                                            face: sex.clone(),
                                            local_rid: None,
                                            remote_rid: None,
                                            subs: None,
                                            stor: true,
                                            eval: false,
                                        })
                                    );
                                }
                            }
                        }

                        for (id, face) in &mut t.faces {
                            if sex.id != *id && (sex.whatami != WhatAmI::Peer || face.whatami != WhatAmI::Peer) {
                                if let Some(mut ctx) = Arc::get_mut_unchecked(&mut prefix).contexts.get_mut(id) {
                                    if let Some(rid) = ctx.local_rid {
                                        face.primitives.clone().storage((rid, suffix).into()).await;
                                    } else if let Some(rid) = ctx.remote_rid {
                                        face.primitives.clone().storage((rid, suffix).into()).await;
                                    } else {
                                        let rid = face.get_next_local_id();
                                        Arc::get_mut_unchecked(&mut ctx).local_rid = Some(rid);
                                        Arc::get_mut_unchecked(face).local_mappings.insert(rid, prefix.clone());

                                        face.primitives.clone().resource(rid, prefix.name().into()).await;
                                        face.primitives.clone().storage((rid, suffix).into()).await;
                                    }
                                } else {
                                    let rid = face.get_next_local_id();
                                    Arc::get_mut_unchecked(&mut prefix).contexts.insert(*id, 
                                        Arc::new(Context {
                                            face: face.clone(),
                                            local_rid: Some(rid),
                                            remote_rid: None,
                                            subs: None,
                                            stor: false,
                                            eval: false,
                                    }));
                                    Arc::get_mut_unchecked(face).local_mappings.insert(rid, prefix.clone());

                                    face.primitives.clone().resource(rid, prefix.name().into()).await;
                                    face.primitives.clone().storage((rid, suffix).into()).await;
                                }
                            }
                        }
                        Tables::build_matches_direct_tables(&mut res);
                        Arc::get_mut_unchecked(&mut sex).stos.push(res);
                    }
                    None => println!("Declare storage for unknown rid {}!", prefixid)
                }
            }
            None => println!("Declare storage for closed session!")
        }
    }

    pub async fn undeclare_storage(tables: &Arc<RwLock<Tables>>, sex: &Weak<Face>, prefixid: u64, suffix: &str) {
        let t = tables.write().await;
        match sex.upgrade() {
            Some(mut sex) => {
                let prefix = {
                    match prefixid {
                        0 => {Some(&t.root_res)}
                        prefixid => {sex.get_mapping(&prefixid)}
                    }
                };
                match prefix {
                    Some(prefix) => {
                        match Resource::get_resource(prefix, suffix) {
                            Some(mut res) => unsafe {
                                if let Some(mut ctx) = Arc::get_mut_unchecked(&mut res).contexts.get_mut(&sex.id) {
                                    Arc::get_mut_unchecked(&mut ctx).stor = false;
                                }
                                Arc::get_mut_unchecked(&mut sex).subs.retain(|x| ! Arc::ptr_eq(&x, &res));
                                Resource::clean(&mut res)
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

    fn get_matches_from(rname: &str, from: &Arc<Resource>) -> Vec<Weak<Resource>> {
        let mut matches = Vec::new();
        if from.parent.is_none() {
            for child in from.childs.values() {
                matches.append(&mut Tables::get_matches_from(rname, child));
            }
            return matches
        }
        if rname.is_empty() {
            if from.suffix == "/**" || from.suffix == "/" {
                matches.push(Arc::downgrade(from));
                for child in from.childs.values() {
                    matches.append(&mut Tables::get_matches_from(rname, child));
                }
            }
            return matches
        }
        let (chunk, rest) = Tables::fst_chunk(rname);
        if intersect(chunk, &from.suffix) {
            if rest.is_empty() || rest == "/" || rest == "/**" {
                matches.push(Arc::downgrade(from));
            } else if chunk == "/**" || from.suffix == "/**" {
                matches.append(&mut Tables::get_matches_from(rest, from));
            }
            for child in from.childs.values() {
                matches.append(&mut Tables::get_matches_from(rest, child));
                if chunk == "/**" || from.suffix == "/**" {
                    matches.append(&mut Tables::get_matches_from(rname, child));
                }
            }
        }
        matches
    }

    pub async fn get_matches(tables: &Arc<RwLock<Tables>>, rname: &str) -> Vec<Weak<Resource>> {
        let t = tables.read().await;
        Tables::get_matches_from(rname, &t.root_res)
    }

    #[inline]
    fn get_best_key(prefix: &Arc<Resource>, suffix: &str, sid: usize) -> (u64, String) {
        fn get_best_key_(prefix: &Arc<Resource>, suffix: &str, sid: usize, checkchilds: bool) -> (u64, String) {
            if checkchilds && ! suffix.is_empty() {
                let (chunk, rest) = Tables::fst_chunk(suffix);
                if let Some(child) = prefix.childs.get(chunk) {
                    return get_best_key_(child, rest, sid, true)
                }
            }
            if let Some(ctx) = prefix.contexts.get(&sid) {
                if let Some(rid) = ctx.local_rid {
                    return (rid, suffix.to_string())
                } else if let Some(rid) = ctx.remote_rid {
                    return (rid, suffix.to_string())
                }
            }
            match &prefix.parent {
                Some(parent) => {get_best_key_(&parent, &[&prefix.suffix, suffix].concat(), sid, false)}
                None => {(0, suffix.to_string())}
            }
        }
        get_best_key_(prefix, suffix, sid, true)
    }

    pub async fn route_data_to_map(tables: &Arc<RwLock<Tables>>, sex: &Weak<Face>, rid: u64, suffix: &str) 
    -> Option<Route> {

        let t = tables.read().await;

        let build_route = |prefix: &Arc<Resource>, suffix: &str| {
            Some(match Resource::get_resource(prefix, suffix) {
                Some(res) => {res.route.clone()}
                None => {
                    let mut sexs = HashMap::new();
                    for res in Tables::get_matches_from(&[&prefix.name(), suffix].concat(), &t.root_res) {
                        let res = res.upgrade().unwrap();
                        for (sid, context) in &res.contexts {
                            if context.subs.is_some() || context.stor {
                                sexs.entry(*sid).or_insert_with( || {
                                    let (rid, suffix) = Tables::get_best_key(prefix, suffix, *sid);
                                    (context.face.clone(), rid, suffix)
                                });
                            }
                        }
                    };
                    sexs
                }
            })
        };

        match sex.upgrade() {
            Some(sex) => {
                match sex.get_mapping(&rid) {
                    Some(res) => {
                        match suffix {
                            "" => {Some(res.route.clone())}
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

    pub async fn route_data(tables: &Arc<RwLock<Tables>>, sex: &Weak<Face>, rid: u64, suffix: &str, reliable:bool, info: &Option<RBuf>, payload: RBuf) {
        match sex.upgrade() {
            Some(strongsex) => {
                if let Some(outfaces) = Tables::route_data_to_map(tables, sex, rid, suffix).await {
                    for (_id, (face, rid, suffix)) in outfaces {
                        if ! Arc::ptr_eq(&strongsex, &face) {
                            // TODO move primitives out of inner mutability
                            let primitives = {
                                if strongsex.whatami != WhatAmI::Peer || face.whatami != WhatAmI::Peer {
                                    Some(face.primitives.clone())
                                } else {
                                    None
                                }
                            };
                            if let Some(primitives) = primitives {
                                primitives.data((rid, suffix).into(), reliable, info.clone(), payload.clone()).await
                            }
                        }
                    }
                }
            }
            None => {println!("Route data for closed session!")}
        }
    }
}
