use async_trait::async_trait;
use async_std::sync::RwLock;
use async_std::sync::{Arc, Weak};
use std::collections::{HashMap};

use zenoh_protocol::core::{ResKey, ZInt};
use zenoh_protocol::proto::{Primitives, SubInfo, SubMode, Reliability, Mux, DeMux, WhatAmI, whatami};
use zenoh_protocol::session::{SessionHandler, MsgHandler};

use crate::routing::face::{Face, FaceHdl};

pub use crate::routing::resource::*;
pub use crate::routing::pubsub::*;
pub use crate::routing::queries::*;

/// # Example: 
/// ```
///   use async_std::sync::Arc;
///   use zenoh_protocol::core::PeerId;
///   use zenoh_protocol::io::RBuf;
///   use zenoh_protocol::proto::whatami::PEER;
///   use zenoh_protocol::session::{SessionManager, SessionManagerConfig};
///   use zenoh_router::routing::broker::Broker;
/// 
///   async{
///     // implement Primitives trait
///     use zenoh_protocol::proto::Mux;
///     use zenoh_protocol::session::DummyHandler;
///     let dummyPrimitives = Arc::new(Mux::new(Arc::new(DummyHandler::new())));
///   
///     // Instanciate broker
///     let broker = Arc::new(Broker::new());
/// 
///     // Instanciate SessionManager and plug it to the broker
///     let config = SessionManagerConfig {
///         version: 0,
///         whatami: PEER,
///         id: PeerId{id: vec![1, 2]},
///         handler: broker.clone()
///     };
///     let manager = SessionManager::new(config, None);
/// 
///     // Declare new primitives
///     let primitives = broker.new_primitives(dummyPrimitives).await;
///     
///     // Use primitives
///     primitives.data(&"/demo".to_string().into(), true, &None, RBuf::from(vec![1, 2])).await;
/// 
///     // Close primitives
///     primitives.close().await;
///   };
/// 
/// ```
pub struct Broker {
    pub tables: Arc<RwLock<Tables>>,
}

impl Broker {
    pub fn new() -> Broker {
        Broker {
            tables: Tables::new()
        }
    }
    
    pub async fn new_primitives(&self, primitives: Arc<dyn Primitives + Send + Sync>) -> Arc<dyn Primitives + Send + Sync> {
        Arc::new(FaceHdl {
            tables: self.tables.clone(), 
            face: Tables::declare_session(&self.tables, whatami::CLIENT, primitives).await.upgrade().unwrap(),
        })
    }
}

impl Default for Broker {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SessionHandler for Broker {
    async fn new_session(&self, whatami: WhatAmI, session: Arc<dyn MsgHandler + Send + Sync>) -> Arc<dyn MsgHandler + Send + Sync> {
        Arc::new(DeMux::new(FaceHdl {
            tables: self.tables.clone(), 
            face: Tables::declare_session(&self.tables, whatami, Arc::new(Mux::new(session))).await.upgrade().unwrap(),
        }))
    }
}

pub struct Tables {
    face_counter: usize,
    pub(crate) root_res: Arc<Resource>,
    pub(crate) faces: HashMap<usize, Arc<Face>>,
}

impl Tables {

    pub fn new() -> Arc<RwLock<Tables>> {
        Arc::new(RwLock::new(Tables {
            face_counter: 0,
            root_res: Resource::root(),
            faces: HashMap::new(),
        }))
    }

    #[doc(hidden)]
    pub fn _get_root(&self) -> &Arc<Resource> {
        &self.root_res
    }

    pub async fn print(tables: &Arc<RwLock<Tables>>) -> String {
        Resource::print_tree(&tables.read().await.root_res)
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub(crate) fn get_mapping<'a>(&'a self, face: &'a Face, rid: &ZInt) -> Option<&'a Arc<Resource>> {
        match rid {
            0 => {Some(&self.root_res)}
            rid => {face.get_mapping(rid)}
        }
    }

    pub async fn declare_session(tables: &Arc<RwLock<Tables>>, whatami: WhatAmI, primitives: Arc<dyn Primitives + Send + Sync>) -> Weak<Face> {
        unsafe {
            let mut t = tables.write().await;
            let sid = t.face_counter;
            log::debug!("New face {}", sid);
            t.face_counter += 1;
            let mut newface = t.faces.entry(sid).or_insert_with(|| Face::new(sid, whatami, primitives.clone())).clone();
            
            // @TODO temporarily propagate to everybody (clients)
            // if whatami != whatami::CLIENT {
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

    pub async fn undeclare_session(tables: &Arc<RwLock<Tables>>, face: &Weak<Face>) {
        let mut t = tables.write().await;
        match face.upgrade() {
            Some(mut face) => unsafe {
                log::debug!("Close face {}", face.id);
                let face = Arc::get_mut_unchecked(&mut face);
                for mut mapping in face.remote_mappings.values_mut() {
                    Resource::clean(&mut mapping);
                }
                face.remote_mappings.clear();
                for mut mapping in face.local_mappings.values_mut() {
                    Resource::clean(&mut mapping);
                }
                face.local_mappings.clear();
                while let Some(mut res) = face.subs.pop() {
                    Resource::clean(&mut res);
                }
                t.faces.remove(&face.id);
            }
            None => log::error!("Face already closed!")
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

    pub(crate) unsafe fn build_matches_direct_tables(res: &mut Arc<Resource>) {
        Tables::build_direct_tables(res);

        let resclone = res.clone();
        for match_ in &mut Arc::get_mut_unchecked(res).matches {
            if ! Arc::ptr_eq(&match_.upgrade().unwrap(), &resclone) {
                Tables::build_direct_tables(&mut match_.upgrade().unwrap());
            }
        }
    }
    
    pub async fn get_matches(tables: &Arc<RwLock<Tables>>, rname: &str) -> Vec<Weak<Resource>> {
        let t = tables.read().await;
        Resource::get_matches_from(rname, &t.root_res)
    }

}
