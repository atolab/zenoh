use std::sync::Arc;
use std::collections::HashMap;
use zenoh_protocol::core::{ZInt, ResKey};
use zenoh_protocol::proto::{WhatAmI, QueryTarget, QueryConsolidation, Reply};
use crate::routing::broker::Tables;
use crate::routing::face::Face;
use crate::routing::resource::{Resource, Context};

pub(crate) struct Query {
    src_face: Arc<Face>,
    src_qid: ZInt,
}

type QueryRoute = HashMap<usize, (Arc<Face>, u64, String, u64)>;

pub(crate) async fn declare_queryable(tables: &mut Tables, face: &mut Arc<Face>, prefixid: u64, suffix: &str) {
    let prefix = {
        match prefixid {
            0 => {Some(tables.root_res.clone())}
            prefixid => {face.get_mapping(&prefixid).cloned()}
        }
    };
    match prefix {
        Some(mut prefix) => unsafe {
            let mut res = Resource::make_resource(&mut prefix, suffix);
            Resource::match_resource(&tables.root_res, &mut res);
            {
                match Arc::get_mut_unchecked(&mut res).contexts.get_mut(&face.id) {
                    Some(mut ctx) => {
                        Arc::get_mut_unchecked(&mut ctx).qabl = true;
                    }
                    None => {
                        Arc::get_mut_unchecked(&mut res).contexts.insert(face.id, 
                            Arc::new(Context {
                                face: face.clone(),
                                local_rid: None,
                                remote_rid: None,
                                subs: None,
                                qabl: true,
                            })
                        );
                    }
                }
            }

            for (id, someface) in &mut tables.faces {
                if face.id != *id && (face.whatami != WhatAmI::Peer || someface.whatami != WhatAmI::Peer) {
                    let (nonwild_prefix, wildsuffix) = Resource::nonwild_prefix(&res);
                    match nonwild_prefix {
                        Some(mut nonwild_prefix) => {
                            if let Some(mut ctx) = Arc::get_mut_unchecked(&mut nonwild_prefix).contexts.get_mut(id) {
                                if let Some(rid) = ctx.local_rid {
                                    someface.primitives.clone().queryable((rid, wildsuffix).into()).await;
                                } else if let Some(rid) = ctx.remote_rid {
                                    someface.primitives.clone().queryable((rid, wildsuffix).into()).await;
                                } else {
                                    let rid = someface.get_next_local_id();
                                    Arc::get_mut_unchecked(&mut ctx).local_rid = Some(rid);
                                    Arc::get_mut_unchecked(someface).local_mappings.insert(rid, nonwild_prefix.clone());

                                    someface.primitives.clone().resource(rid, nonwild_prefix.name().into()).await;
                                    someface.primitives.clone().queryable((rid, wildsuffix).into()).await;
                                }
                            } else {
                                let rid = face.get_next_local_id();
                                Arc::get_mut_unchecked(&mut nonwild_prefix).contexts.insert(*id, 
                                    Arc::new(Context {
                                        face: someface.clone(),
                                        local_rid: Some(rid),
                                        remote_rid: None,
                                        subs: None,
                                        qabl: false,
                                }));
                                Arc::get_mut_unchecked(someface).local_mappings.insert(rid, nonwild_prefix.clone());

                                someface.primitives.clone().resource(rid, nonwild_prefix.name().into()).await;
                                someface.primitives.clone().queryable((rid, wildsuffix).into()).await;
                            }
                        }
                        None => {
                            someface.primitives.clone().queryable(ResKey::RName(wildsuffix)).await;
                        }
                    }
                }
            }
            Tables::build_matches_direct_tables(&mut res);
            Arc::get_mut_unchecked(face).qabl.push(res);
        }
        None => println!("Declare queryable for unknown rid {}!", prefixid)
    }
}

pub async fn undeclare_queryable(tables: &mut Tables, face: &mut Arc<Face>, prefixid: u64, suffix: &str) {
    match tables.get_mapping(&face, &prefixid) {
        Some(prefix) => {
            match Resource::get_resource(prefix, suffix) {
                Some(mut res) => unsafe {
                    if let Some(mut ctx) = Arc::get_mut_unchecked(&mut res).contexts.get_mut(&face.id) {
                        Arc::get_mut_unchecked(&mut ctx).qabl = false;
                    }
                    Arc::get_mut_unchecked(face).subs.retain(|x| ! Arc::ptr_eq(&x, &res));
                    Resource::clean(&mut res)
                }
                None => println!("Undeclare unknown queryable!")
            }
        }
        None => println!("Undeclare queryable with unknown prefix!")
    }
}

async fn route_query_to_map(tables: &mut Tables, face: &Arc<Face>, qid: ZInt, rid: u64, suffix: &str/*, _predicate: &str, */
/*_qid: ZInt, _target: &Option<QueryTarget>, _consolidation: &QueryConsolidation*/) -> Option<QueryRoute> {
    match tables.get_mapping(&face, &rid) {
        Some(prefix) => {
            let query = Arc::new(Query {src_face: face.clone(), src_qid: qid});
            let mut faces = HashMap::new();
            for res in Resource::get_matches_from(&[&prefix.name(), suffix].concat(), &tables.root_res) {
                unsafe {
                    let mut res = res.upgrade().unwrap();
                    for (sid, context) in &mut Arc::get_mut_unchecked(&mut res).contexts {
                        if context.qabl && ! Arc::ptr_eq(&face, &context.face)
                        {
                            faces.entry(*sid).or_insert_with( || {
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
            Some(faces)
        }
        None => {println!("Route query with unknown rid {}!", rid); None}
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn route_query(tables: &mut Tables, face: &Arc<Face>, rid: u64, suffix: &str, predicate: &str, 
                                qid: ZInt, target: QueryTarget, consolidation: QueryConsolidation) {
    if let Some(outfaces) = route_query_to_map(tables, face, qid, rid, suffix).await {
        for (_id, (outface, rid, suffix, qid)) in outfaces {
            let primitives = {
                if face.whatami != WhatAmI::Peer || outface.whatami != WhatAmI::Peer {
                    Some(outface.primitives.clone())
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

pub(crate) async fn route_reply(_tables: &mut Tables, face: &mut Arc<Face>, qid: ZInt, reply: &Reply) {
    match face.pending_queries.get(&qid) {
        Some(query) => {
            match reply {
                Reply::ReplyData {..} | Reply::SourceFinal {..} => {
                    query.src_face.primitives.clone().reply(query.src_qid, reply.clone()).await;
                }
                Reply::ReplyFinal {..} => {
                    unsafe {
                        let query = face.pending_queries.get(&qid).unwrap().clone();
                        Arc::get_mut_unchecked(face).pending_queries.remove(&qid);
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