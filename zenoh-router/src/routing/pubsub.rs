use std::sync::Arc;
use std::collections::HashMap;
use zenoh_protocol::core::ResKey;
use zenoh_protocol::io::RBuf;
use zenoh_protocol::proto::{WhatAmI, SubInfo};
use crate::routing::face::Face;
use crate::routing::tables::Tables;
use crate::routing::resource::{Resource, Context};

pub type DataRoute = HashMap<usize, (Arc<Face>, u64, String)>;

pub async fn declare_subscription(tables: &mut Tables, sex: &mut Arc<Face>, prefixid: u64, suffix: &str, sub_info: &SubInfo) {
    match tables.get_mapping(&sex, &prefixid).cloned() {
        Some(mut prefix) => unsafe {
            let mut res = Resource::make_resource(&mut prefix, suffix);
            Resource::match_resource(&tables.root_res, &mut res);
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

            for (id, face) in &mut tables.faces {
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
            Arc::get_mut_unchecked(sex).subs.push(res);
        }
        None => println!("Declare subscription for unknown rid {}!", prefixid)
    }
}

pub async fn undeclare_subscription(tables: &mut Tables, sex: &mut Arc<Face>, prefixid: u64, suffix: &str) {
    match tables.get_mapping(&sex, &prefixid) {
        Some(prefix) => {
            match Resource::get_resource(prefix, suffix) {
                Some(mut res) => unsafe {
                    if let Some(mut ctx) = Arc::get_mut_unchecked(&mut res).contexts.get_mut(&sex.id) {
                        Arc::get_mut_unchecked(&mut ctx).subs = None;
                    }
                    Arc::get_mut_unchecked(sex).subs.retain(|x| ! Arc::ptr_eq(&x, &res));
                    Resource::clean(&mut res)
                }
                None => println!("Undeclare unknown subscription!")
            }
        }
        None => println!("Undeclare subscription with unknown prefix!")
    }
}

pub async fn route_data_to_map(tables: &Tables, sex: &Arc<Face>, rid: u64, suffix: &str) -> Option<DataRoute> {
    match tables.get_mapping(&sex, &rid) {
        Some(prefix) => {
            match Resource::get_resource(prefix, suffix) {
                Some(res) => {Some(res.route.clone())}
                None => {
                    let mut sexs = HashMap::new();
                    for res in Resource::get_matches_from(&[&prefix.name(), suffix].concat(), &tables.root_res) {
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

pub async fn route_data(tables: &Tables, sex: &Arc<Face>, rid: u64, suffix: &str, reliable:bool, info: &Option<RBuf>, payload: RBuf) {
    if let Some(outfaces) = route_data_to_map(tables, sex, rid, suffix).await {
        for (_id, (face, rid, suffix)) in outfaces {
            if ! Arc::ptr_eq(sex, &face) {
                let primitives = {
                    if sex.whatami != WhatAmI::Peer || face.whatami != WhatAmI::Peer {
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