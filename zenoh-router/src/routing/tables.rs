use async_trait::async_trait;
use async_std::sync::RwLock;
use std::sync::{Arc, Weak};
use std::collections::{HashMap};
use zenoh_protocol::core::{ResKey, ZInt};
use zenoh_protocol::io::RBuf;
use zenoh_protocol::proto::{Primitives, SubInfo, SubMode, Reliability, Mux, DeMux, WhatAmI, QueryTarget, QueryConsolidation, Reply};
use zenoh_protocol::session::{SessionHandler, MsgHandler};
use crate::routing::resource::*;
use crate::routing::face::{Face, FaceHdl};
use crate::routing::query::Query;

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
///         handler: tables.clone()
///     };
///     let manager = SessionManager::new(config, None);
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

pub type DataRoute = HashMap<usize, (Arc<Face>, u64, String)>;
pub type QueryRoute = HashMap<usize, (Arc<Face>, u64, String, u64)>;

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

    #[allow(clippy::trivially_copy_pass_by_ref)]
    fn get_mapping<'a>(&'a self, face: &'a Face, rid: &ZInt) -> Option<&'a Arc<Resource>> {
        match rid {
            0 => {Some(&self.root_res)}
            rid => {face.get_mapping(rid)}
        }
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
                            let (nonwild_prefix, wildsuffix) = Resource::nonwild_prefix(sub);
                            match nonwild_prefix {
                                Some(mut nonwild_prefix) => {
                                    local_id += 1;
                                    Arc::get_mut_unchecked(&mut nonwild_prefix).contexts.insert(sid, 
                                        Arc::new(Context {
                                            face: newface.clone(),
                                            local_rid: Some(local_id),
                                            remote_rid: None,
                                            subs: None,
                                            qabl: false,
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

                        for qabl in face.qabl.iter() {
                            let (nonwild_prefix, wildsuffix) = Resource::nonwild_prefix(qabl);
                            match nonwild_prefix {
                                Some(mut nonwild_prefix) => {
                                    local_id += 1;
                                    Arc::get_mut_unchecked(&mut nonwild_prefix).contexts.insert(sid, 
                                        Arc::new(Context {
                                            face: newface.clone(),
                                            local_rid: Some(local_id),
                                            remote_rid: None,
                                            subs: None,
                                            qabl: false,
                                    }));
                                    Arc::get_mut_unchecked(&mut newface).local_mappings.insert(local_id, nonwild_prefix.clone());

                                    primitives.resource(local_id, &ResKey::RName(nonwild_prefix.name())).await;
                                    primitives.queryable(&ResKey::RIdWithSuffix(local_id, wildsuffix)).await;
                                }
                                None => {
                                    primitives.queryable(&ResKey::RName(wildsuffix)).await;
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
                if context.subs.is_some() {
                    let (rid, suffix) = Resource::get_best_key(res, "", *sid);
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
                        match t.get_mapping(&sex, &prefixid).cloned() {
                            Some(mut prefix) => unsafe {
                                let mut res = Resource::make_resource(&mut prefix, suffix);
                                Resource::match_resource(&t.root_res, &mut res);
                                let mut ctx = Arc::get_mut_unchecked(&mut res).contexts.entry(sex.id).or_insert_with( ||
                                    Arc::new(Context {
                                        face: sex.clone(),
                                        local_rid: None,
                                        remote_rid: Some(rid),
                                        subs: None,
                                        qabl: false,
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
                match t.get_mapping(&sex, &prefixid).cloned() {
                    Some(mut prefix) => unsafe {
                        let mut res = Resource::make_resource(&mut prefix, suffix);
                        Resource::match_resource(&t.root_res, &mut res);
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
                                            qabl: false,
                                        })
                                    );
                                }
                            }
                        }

                        for (id, face) in &mut t.faces {
                            if sex.id != *id && (sex.whatami != WhatAmI::Peer || face.whatami != WhatAmI::Peer) {
                                let (nonwild_prefix, wildsuffix) = Resource::nonwild_prefix(&res);
                                match nonwild_prefix {
                                    Some(mut nonwild_prefix) => {
                                        if let Some(mut ctx) = Arc::get_mut_unchecked(&mut nonwild_prefix).contexts.get_mut(id) {
                                            if let Some(rid) = ctx.local_rid {
                                                face.primitives.clone().subscriber((rid, wildsuffix).into(), sub_info.clone()).await;
                                            } else if let Some(rid) = ctx.remote_rid {
                                                face.primitives.clone().subscriber((rid, wildsuffix).into(), sub_info.clone()).await;
                                            } else {
                                                let rid = face.get_next_local_id();
                                                Arc::get_mut_unchecked(&mut ctx).local_rid = Some(rid);
                                                Arc::get_mut_unchecked(face).local_mappings.insert(rid, nonwild_prefix.clone());

                                                face.primitives.clone().resource(rid, nonwild_prefix.name().into()).await;
                                                face.primitives.clone().subscriber((rid, wildsuffix).into(), sub_info.clone()).await;
                                            }
                                        } else {
                                            let rid = face.get_next_local_id();
                                            Arc::get_mut_unchecked(&mut nonwild_prefix).contexts.insert(*id, 
                                                Arc::new(Context {
                                                    face: face.clone(),
                                                    local_rid: Some(rid),
                                                    remote_rid: None,
                                                    subs: None,
                                                    qabl: false,
                                            }));
                                            Arc::get_mut_unchecked(face).local_mappings.insert(rid, nonwild_prefix.clone());

                                            face.primitives.clone().resource(rid, nonwild_prefix.name().into()).await;
                                            face.primitives.clone().subscriber((rid, wildsuffix).into(), sub_info.clone()).await;
                                        }
                                    }
                                    None => {
                                        face.primitives.clone().subscriber(ResKey::RName(wildsuffix), sub_info.clone()).await;
                                    }
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
                match t.get_mapping(&sex, &prefixid) {
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

    pub async fn declare_queryable(tables: &Arc<RwLock<Tables>>, sex: &Weak<Face>, prefixid: u64, suffix: &str) {
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
                        let mut res = Resource::make_resource(&mut prefix, suffix);
                        Resource::match_resource(&t.root_res, &mut res);
                        {
                            match Arc::get_mut_unchecked(&mut res).contexts.get_mut(&sex.id) {
                                Some(mut ctx) => {
                                    Arc::get_mut_unchecked(&mut ctx).qabl = true;
                                }
                                None => {
                                    Arc::get_mut_unchecked(&mut res).contexts.insert(sex.id, 
                                        Arc::new(Context {
                                            face: sex.clone(),
                                            local_rid: None,
                                            remote_rid: None,
                                            subs: None,
                                            qabl: true,
                                        })
                                    );
                                }
                            }
                        }

                        for (id, face) in &mut t.faces {
                            if sex.id != *id && (sex.whatami != WhatAmI::Peer || face.whatami != WhatAmI::Peer) {
                                let (nonwild_prefix, wildsuffix) = Resource::nonwild_prefix(&res);
                                match nonwild_prefix {
                                    Some(mut nonwild_prefix) => {
                                        if let Some(mut ctx) = Arc::get_mut_unchecked(&mut nonwild_prefix).contexts.get_mut(id) {
                                            if let Some(rid) = ctx.local_rid {
                                                face.primitives.clone().queryable((rid, wildsuffix).into()).await;
                                            } else if let Some(rid) = ctx.remote_rid {
                                                face.primitives.clone().queryable((rid, wildsuffix).into()).await;
                                            } else {
                                                let rid = face.get_next_local_id();
                                                Arc::get_mut_unchecked(&mut ctx).local_rid = Some(rid);
                                                Arc::get_mut_unchecked(face).local_mappings.insert(rid, nonwild_prefix.clone());

                                                face.primitives.clone().resource(rid, nonwild_prefix.name().into()).await;
                                                face.primitives.clone().queryable((rid, wildsuffix).into()).await;
                                            }
                                        } else {
                                            let rid = face.get_next_local_id();
                                            Arc::get_mut_unchecked(&mut nonwild_prefix).contexts.insert(*id, 
                                                Arc::new(Context {
                                                    face: face.clone(),
                                                    local_rid: Some(rid),
                                                    remote_rid: None,
                                                    subs: None,
                                                    qabl: false,
                                            }));
                                            Arc::get_mut_unchecked(face).local_mappings.insert(rid, nonwild_prefix.clone());

                                            face.primitives.clone().resource(rid, nonwild_prefix.name().into()).await;
                                            face.primitives.clone().queryable((rid, wildsuffix).into()).await;
                                        }
                                    }
                                    None => {
                                        face.primitives.clone().queryable(ResKey::RName(wildsuffix)).await;
                                    }
                                }
                            }
                        }
                        Tables::build_matches_direct_tables(&mut res);
                        Arc::get_mut_unchecked(&mut sex).qabl.push(res);
                    }
                    None => println!("Declare queryable for unknown rid {}!", prefixid)
                }
            }
            None => println!("Declare queryable for closed session!")
        }
    }

    pub async fn undeclare_queryable(tables: &Arc<RwLock<Tables>>, sex: &Weak<Face>, prefixid: u64, suffix: &str) {
        let t = tables.write().await;
        match sex.upgrade() {
            Some(mut sex) => {
                match t.get_mapping(&sex, &prefixid) {
                    Some(prefix) => {
                        match Resource::get_resource(prefix, suffix) {
                            Some(mut res) => unsafe {
                                if let Some(mut ctx) = Arc::get_mut_unchecked(&mut res).contexts.get_mut(&sex.id) {
                                    Arc::get_mut_unchecked(&mut ctx).qabl = false;
                                }
                                Arc::get_mut_unchecked(&mut sex).subs.retain(|x| ! Arc::ptr_eq(&x, &res));
                                Resource::clean(&mut res)
                            }
                            None => println!("Undeclare unknown queryable!")
                        }
                    }
                    None => println!("Undeclare queryable with unknown prefix!")
                }
            }
            None => println!("Undeclare queryable for closed session!")
        }
    }

    pub async fn get_matches(tables: &Arc<RwLock<Tables>>, rname: &str) -> Vec<Weak<Resource>> {
        let t = tables.read().await;
        Resource::get_matches_from(rname, &t.root_res)
    }

    pub async fn route_data_to_map(tables: &Arc<RwLock<Tables>>, sex: &Weak<Face>, rid: u64, suffix: &str) -> Option<DataRoute> {
        let t = tables.read().await;

        match sex.upgrade() {
            Some(sex) => {
                match t.get_mapping(&sex, &rid) {
                    Some(prefix) => {
                        match Resource::get_resource(prefix, suffix) {
                            Some(res) => {Some(res.route.clone())}
                            None => {
                                let mut sexs = HashMap::new();
                                for res in Resource::get_matches_from(&[&prefix.name(), suffix].concat(), &t.root_res) {
                                    let res = res.upgrade().unwrap();
                                    for (sid, context) in &res.contexts {
                                        if context.subs.is_some() {
                                            sexs.entry(*sid).or_insert_with( || {
                                                let (rid, suffix) = Resource::get_best_key(prefix, suffix, *sid);
                                                (context.face.clone(), rid, suffix)
                                            });
                                        }
                                    }
                                };
                                Some(sexs)
                            }
                        }
                    }
                    None => {
                        println!("Route data with unknown rid {}!", rid); None
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

    pub async fn route_query_to_map(tables: &Arc<RwLock<Tables>>, sex: &Weak<Face>, qid: ZInt, rid: u64, suffix: &str/*, _predicate: &str, */
    /*_qid: ZInt, _target: &Option<QueryTarget>, _consolidation: &QueryConsolidation*/) -> Option<QueryRoute> {
        let t = tables.write().await;

        match sex.upgrade() {
            Some(sex) => {
                match t.get_mapping(&sex, &rid) {
                    Some(prefix) => {
                        let query = Arc::new(Query {src_face: sex.clone(), src_qid: qid});
                        let mut sexs = HashMap::new();
                        for res in Resource::get_matches_from(&[&prefix.name(), suffix].concat(), &t.root_res) {
                            unsafe {
                                let mut res = res.upgrade().unwrap();
                                for (sid, context) in &mut Arc::get_mut_unchecked(&mut res).contexts {
                                    if context.qabl && ! Arc::ptr_eq(&sex, &context.face)
                                    {
                                        sexs.entry(*sid).or_insert_with( || {
                                            let (rid, suffix) = Resource::get_best_key(prefix, suffix, *sid);
                                            let face = Arc::get_mut_unchecked(&mut Arc::get_mut_unchecked(context).face);
                                            face.next_qid += 1;
                                            let qid = face.next_qid;
                                            face.pending_queries.insert(qid, query.clone());
                                            (context.face.clone(), rid, suffix, qid)
                                        });
                                    }
                                }
                            }
                        };
                        Some(sexs)
                    }
                    None => {println!("Route query with unknown rid {}!", rid); None}
                }
            }
            None => {println!("Route query for closed session!"); None}
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn route_query(tables: &Arc<RwLock<Tables>>, sex: &Weak<Face>, rid: u64, suffix: &str, predicate: &str, 
                            qid: ZInt, target: QueryTarget, consolidation: QueryConsolidation) {
        match sex.upgrade() {
            Some(strongsex) => {
                if let Some(outfaces) = Tables::route_query_to_map(tables, sex, qid, rid, suffix).await {
                    for (_id, (face, rid, suffix, qid)) in outfaces {
                        let primitives = {
                            if strongsex.whatami != WhatAmI::Peer || face.whatami != WhatAmI::Peer {
                                Some(face.primitives.clone())
                            } else {
                                None
                            }
                        };
                        if let Some(primitives) = primitives {
                            primitives.query((rid, suffix).into(), predicate.to_string(), qid, target.clone(), consolidation.clone()).await
                        }
                    }
                }
            }
            None => {println!("Route data for closed session!")}
        }
    }

    pub async fn route_reply(tables: &Arc<RwLock<Tables>>, sex: &Weak<Face>, qid: ZInt, reply: &Reply) {
        let _t = tables.write().await;

        match sex.upgrade() {
            Some(mut strongsex) => {
                match strongsex.pending_queries.get(&qid) {
                    Some(query) => {
                        match reply {
                            Reply::ReplyData {..} | Reply::SourceFinal {..} => {
                                query.src_face.primitives.clone().reply(query.src_qid, reply.clone()).await;
                            }
                            Reply::ReplyFinal {..} => {
                                unsafe {
                                    let query = strongsex.pending_queries.get(&qid).unwrap().clone();
                                    Arc::get_mut_unchecked(&mut strongsex).pending_queries.remove(&qid);
                                    if Arc::strong_count(&query) == 1 {
                                        query.src_face.primitives.clone().reply(query.src_qid, Reply::ReplyFinal).await;
                                    }
                                }
                            }
                        }
                    }
                    None => {println!("Route reply for unknown query!")}
                }
            }
            None => {println!("Route reply for closed session!")}
        }
    }

}
