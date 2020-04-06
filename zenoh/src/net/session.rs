use std::fmt;
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
use std::collections::HashMap;
use async_std::sync::Arc;
use async_trait::async_trait;
use spin::RwLock;
use rand::prelude::*;
use zenoh_protocol:: {
    core::{ rname, PeerId, ResourceId, ResKey, ZError, ZErrorKind },
    io::RBuf,
    proto::{ DataInfo, Primitives, QueryTarget, QueryConsolidation, Reply, WhatAmI },
    session::SessionManager,
    zerror
};
use zenoh_router::routing::tables::TablesHdl;
use super::*;

// rename to avoid conflicts
type TxSession = zenoh_protocol::session::Session;

#[derive(Clone)]
pub struct Session {
    session_manager: SessionManager,
    tx_session: Option<Arc<TxSession>>,
    tables: Arc<TablesHdl>,
    inner: Arc<RwLock<InnerSession>>,
}

impl Session {

    pub(super) async fn new(locator: &str, _ps: Option<Properties>) -> Session {
        let tables = Arc::new(TablesHdl::new());
        
        let mut pid = vec![0, 0, 0, 0];
        rand::thread_rng().fill_bytes(&mut pid);

        let session_manager = SessionManager::new(0, WhatAmI::Peer, PeerId{id: pid}, 0, tables.clone());

        // @TODO: scout if locator = "". For now, replace by "tcp/127.0.0.1:7447"
        let locator = if locator.is_empty() { "tcp/127.0.0.1:7447" } else { &locator };

        let mut tx_session: Option<Arc<TxSession>> = None;

        // @TODO: manage a tcp.port property (and tcp.interface?)
        // try to open TCP port 7447
        if let Err(_err) = session_manager.add_locator(&"tcp/127.0.0.1:7447".parse().unwrap(), None).await {
            // if failed, try to connect to peer on locator
            println!("Unable to open listening TCP port on 127.0.0.1:7447. Try connection to {}", locator);
            match session_manager.open_session(&locator.parse().unwrap()).await {
                Ok(s) => tx_session = Some(Arc::new(s)),
                Err(err) => {
                    println!("Unable to connect to {}! {:?}", locator, err);
                    std::process::exit(-1);
                }
            }
        } else {
            println!("Listening on TCP: 127.0.0.1:7447.");
        }

        let inner = Arc::new(RwLock::new(
            InnerSession::new()
        ));
        let inner2 = inner.clone();
        let session = Session{ session_manager, tx_session, tables, inner };

        let prim = session.tables.new_primitives(Arc::new(session.clone())).await;
        inner2.write().primitives = Some(prim);

        session
    }

    pub async fn close(&self) -> ZResult<()> {
        // @TODO: implement
        println!("---- CLOSE");
        let inner = &mut self.inner.write();
        let primitives = inner.primitives.as_ref().unwrap();

        primitives.close().await;

        if let Some(tx_session) = &self.tx_session {
            return tx_session.close().await
        }
        Ok(())

        // @TODO: session_manager.del_locator()
    }

    pub fn info(&self) -> Properties {
        // @TODO: implement
        println!("---- INFO");
        let mut info = Properties::new();
        info.insert(ZN_INFO_PEER_KEY, b"tcp/somewhere:7887".to_vec());
        info.insert(ZN_INFO_PID_KEY, vec![1u8, 2, 3]);
        info.insert(ZN_INFO_PEER_PID_KEY, vec![4u8, 5, 6]);
        info
    }

    pub async fn declare_resource(&self, resource: ResKey) -> ZResult<ResourceId> {
        let inner = &mut self.inner.write();
        let rid = inner.rid_counter.fetch_add(1, Ordering::SeqCst) as ZInt;
        let rname = inner.reskey_to_resname(&resource)?;
        inner.resources.insert(rid, rname);

        let primitives = inner.primitives.as_ref().unwrap();
        primitives.resource(rid, resource).await;

        Ok(rid)
    }

    pub async fn undeclare_resource(&self, rid: ResourceId) -> ZResult<()> {
        let inner = &mut self.inner.write();

        let primitives = inner.primitives.as_ref().unwrap();
        primitives.forget_resource(rid).await;

        inner.resources.remove(&rid);
        Ok(())
    }

    pub async fn declare_publisher(&self, resource: ResKey) -> ZResult<Publisher> {
        let inner = &mut self.inner.write();

        let id = inner.decl_id_counter.fetch_add(1, Ordering::SeqCst);
        let publ = Publisher{ id, reskey: resource.clone() };
        inner.publishers.insert(id, publ.clone());

        let primitives = inner.primitives.as_ref().unwrap();
        primitives.publisher(resource).await;

        Ok(publ)
    }

    pub async fn undeclare_publisher(&self, publisher: Publisher) -> ZResult<()> {
        let inner = &mut self.inner.write();
        inner.publishers.remove(&publisher.id);

        // Note: there might be several Publishers on the same ResKey.
        // Before calling forget_publisher(reskey), check if this was the last one.
        if !inner.publishers.values().any(|p| p.reskey == publisher.reskey) {
            let primitives = inner.primitives.as_ref().unwrap();
            primitives.forget_publisher(publisher.reskey).await;
        }
        Ok(())
    }

    pub async fn declare_subscriber<DataHandler>(&self, resource: ResKey, info: SubInfo, data_handler: DataHandler) -> ZResult<Subscriber>
        where DataHandler: FnMut(/*res_name:*/ &str, /*payload:*/ RBuf, /*data_info:*/ DataInfo) + Send + Sync + 'static
    {
        let inner = &mut self.inner.write();
        let id = inner.decl_id_counter.fetch_add(1, Ordering::SeqCst);
        let resname = inner.reskey_to_resname(&resource)?;
        let dhandler = Arc::new(RwLock::new(data_handler));
        let sub = Subscriber{ id, reskey: resource.clone(), resname, dhandler, session: self.inner.clone() };
        inner.subscribers.insert(id, sub.clone());

        let primitives = inner.primitives.as_ref().unwrap();
        primitives.subscriber(resource, info).await;

        Ok(sub)
    }

    pub async fn undeclare_subscriber(&self, subscriber: Subscriber) -> ZResult<()>
    {
        let inner = &mut self.inner.write();
        inner.subscribers.remove(&subscriber.id);

        // Note: there might be several Subscribers on the same ResKey.
        // Before calling forget_subscriber(reskey), check if this was the last one.
        if !inner.subscribers.values().any(|s| s.reskey == subscriber.reskey) {
            let primitives = inner.primitives.as_ref().unwrap();
            primitives.forget_subscriber(subscriber.reskey).await;
        }
        Ok(())
    }

    pub async fn declare_storage<DataHandler, QueryHandler>(&self, resource: ResKey, data_handler: DataHandler, query_handler: QueryHandler) -> ZResult<Storage>
        where DataHandler: FnMut(/*res_name:*/ &str, /*payload:*/ RBuf, /*data_info:*/ DataInfo) + Send + Sync + 'static ,
        QueryHandler: FnMut(/*res_name:*/ &str, /*predicate:*/ &str, /*replies_sender:*/ &RepliesSender, /*query_handle:*/ QueryHandle) + Send + Sync + 'static
    {
        let inner = &mut self.inner.write();
        let id = inner.decl_id_counter.fetch_add(1, Ordering::SeqCst);
        let resname = inner.reskey_to_resname(&resource)?;
        let dhandler = Arc::new(RwLock::new(data_handler));
        let qhandler = Arc::new(RwLock::new(query_handler));
        let sto = Storage{ id, reskey: resource.clone(), resname, dhandler, qhandler };
        inner.storages.insert(id, sto.clone());

        let primitives = inner.primitives.as_ref().unwrap();
        primitives.storage(resource).await;

        // @TODO: REMOVE; Just to test storage callback:
        let payload = RBuf::from(vec![1,2,3]);
        let info = DataInfo::make(None, None, None, None, None, None, None);
        let sto2 = &mut inner.storages.get(&id).unwrap();
        let dhandler = &mut *sto2.dhandler.write();
        dhandler("/A/B", payload, info);
        let qhandler = &mut *sto2.qhandler.write();
        qhandler("/A/**", "starttime=now()-1h", 
            &|handle, replies| {
                println!("------- STORAGE RepliesSender for {} {:?}", handle, replies);
            },
            42
        );

        Ok(sto)
    }

    pub async fn undeclare_storage(&self, storage: Storage) -> ZResult<()> {
        let inner = &mut self.inner.write();
        inner.storages.remove(&storage.id);

        // Note: there might be several Storages on the same ResKey.
        // Before calling forget_storage(reskey), check if this was the last one.
        if !inner.storages.values().any(|s| s.reskey == storage.reskey) {
            let primitives = inner.primitives.as_ref().unwrap();
            primitives.forget_storage(storage.reskey).await;
        }
        Ok(())
    }

    pub async fn declare_eval<QueryHandler>(&self, resource: ResKey, query_handler: QueryHandler) -> ZResult<Eval>
        where QueryHandler: FnMut(/*res_name:*/ &str, /*predicate:*/ &str, /*replies_sender:*/ &RepliesSender, /*query_handle:*/ QueryHandle) + Send + Sync + 'static
    {
        let inner = &mut self.inner.write();
        let id = inner.decl_id_counter.fetch_add(1, Ordering::SeqCst);
        let qhandler = Arc::new(RwLock::new(query_handler));
        let eva = Eval{ id, reskey: resource.clone(), qhandler };
        inner.evals.insert(id, eva.clone());

        let primitives = inner.primitives.as_ref().unwrap();
        primitives.eval(resource).await;

        // @TODO: REMOVE; Just to test eval callback:
        let eva2 = &mut inner.evals.get(&id).unwrap();
        let qhandler = &mut *eva2.qhandler.write();
        qhandler("/A/**", "starttime=now()-1h", 
            &|handle, replies| {
                println!("------- EVAL RepliesSender for {} {:?}", handle, replies);
            },
            42
        );

        Ok(eva)

    }

    pub async fn undeclare_eval(&self, eval: Eval) -> ZResult<()> {
        let inner = &mut self.inner.write();
        inner.evals.remove(&eval.id);

        // Note: there might be several Evals on the same ResKey.
        // Before calling forget_eval(reskey), check if this was the last one.
        if !inner.evals.values().any(|e| e.reskey == eval.reskey) {
            let primitives = inner.primitives.as_ref().unwrap();
            primitives.forget_eval(eval.reskey).await;
        }
        Ok(())
    }

    pub async fn write(&self, resource: ResKey, payload: RBuf) -> ZResult<()> {
        let inner = self.inner.read();
        let primitives = inner.primitives.as_ref().unwrap();
        primitives.data(resource, true, None, payload).await;
        Ok(())
    }

    pub async fn query<RepliesHandler>(&self,
        resource:        ResKey,
        predicate:       String,
        replies_handler: RepliesHandler,
        target:          QueryTarget,
        consolidation:   QueryConsolidation
    ) -> ZResult<()>
        where RepliesHandler: FnMut(/*res_name:*/ &str, /*payload:*/ RBuf, /*data_info:*/ DataInfo) + Send + Sync + 'static
    {
        let inner = &mut self.inner.write();
        let qid = inner.qid_counter.fetch_add(1, Ordering::SeqCst);
        inner.queries.insert(qid, Arc::new(RwLock::new(replies_handler)));

        let primitives = inner.primitives.as_ref().unwrap();
        primitives.query(resource, predicate, qid, target, consolidation).await;

        // @TODO: REMOVE; Just to test reply_handler callback:
        let rhandler = &mut *inner.queries.get(&qid).unwrap().write();
        let payload = RBuf::from(vec![1,2,3]);
        let info = DataInfo::make(None, None, None, None, None, None, None);
        rhandler("/A/B", payload, info);

        Ok(())
    }
}

#[async_trait]
impl Primitives for Session {

    async fn resource(&self, rid: ZInt, reskey: ResKey) {
        println!("++++ recv Resource {} {:?} ", rid, reskey);
    }

    async fn forget_resource(&self, rid: ZInt) {
        println!("++++ recv Forget Resource {} ", rid);
    }

    async fn publisher(&self, reskey: ResKey) {
        println!("++++ recv Publisher {:?} ", reskey);
    }

    async fn forget_publisher(&self, reskey: ResKey) {
        println!("++++ recv Forget Publisher {:?} ", reskey);
    }

    async fn subscriber(&self, reskey: ResKey, sub_info: SubInfo) {
        println!("++++ recv Subscriber {:?} , {:?}", reskey, sub_info);
    }

    async fn forget_subscriber(&self, reskey: ResKey) {
        println!("++++ recv Forget Subscriber {:?} ", reskey);
    }

    async fn storage(&self, reskey: ResKey) {
        println!("++++ recv Storage {:?} ", reskey);
    }

    async fn forget_storage(&self, reskey: ResKey) {
        println!("++++ recv Forget Storage {:?} ", reskey);
    }
    
    async fn eval(&self, reskey: ResKey) {
        println!("++++ recv Eval {:?} ", reskey);
    }

    async fn forget_eval(&self, reskey: ResKey) {
        println!("++++ recv Forget Eval {:?} ", reskey);
    }

    async fn data(&self, reskey: ResKey, _reliable: bool, _info: Option<RBuf>, payload: RBuf) {
        let inner = self.inner.read();
        match inner.reskey_to_resname(&reskey) {
            Ok(resname) => {
                // Call matching subscribers
                for sub in inner.subscribers.values() {
                    if rname::intersect(&sub.resname, &resname) {
                        let info = DataInfo::make(None, None, None, None, None, None, None);   // @TODO
                        let handler = &mut *sub.dhandler.write();
                        handler(&resname, payload.clone(), info);
                    }
                }
                // Call matching storages
                for sto in inner.storages.values() {
                    if rname::intersect(&sto.resname, &resname) {
                        let info = DataInfo::make(None, None, None, None, None, None, None);   // @TODO
                        let handler = &mut *sto.dhandler.write();
                        handler(&resname, payload.clone(), info);
                    }
                }
            },
            Err(err) => println!("{}. Dropping received data", err)
        }
    }

    async fn query(&self, reskey: ResKey, predicate: String, _qid: ZInt, _target: QueryTarget, _consolidation: QueryConsolidation) {
        println!("++++ recv Query {:?} ? {} ", reskey, predicate);
    }

    async fn reply(&self, qid: ZInt, reply: Reply) {
        println!("++++ recv Reply {} : {:?} ", qid, reply);
        let inner = &mut self.inner.write();
        let rhandler = &mut * match inner.queries.get(&qid) {
            Some(arc) => arc.write(),
            None => {
                println!("WARNING: received reply for unkown query: {}", qid);
                return
            }
        };
        match reply {
            Reply::ReplyData {reskey, payload, ..} => {
                let resname = match inner.reskey_to_resname(&reskey) {
                    Ok(name) => name,
                    Err(e) => {
                        println!("WARNING: received reply with {}", e);
                        return
                    }
                };
                let info = DataInfo::make(None, None, None, None, None, None, None);   // @TODO
                rhandler(&resname, payload, info);
            }
            Reply::SourceFinal {..} => {} // @ TODO
            Reply::ReplyFinal {..} => {} // @ TODO remove query
        }
    }

    async fn pull(&self, _is_final: bool, reskey: ResKey, _pull_id: ZInt, _max_samples: Option<ZInt>) {
        println!("++++ recv Pull {:?} ", reskey);
    }

    async fn close(&self) {
        println!("++++ recv Close ");
    }
}



pub(crate) struct InnerSession {
    primitives:      Option<Arc<dyn Primitives + Send + Sync>>, // @TODO replace with MaybeUninit ??
    rid_counter:     AtomicUsize,  // @TODO: manage rollover and uniqueness
    qid_counter:     AtomicU64,
    decl_id_counter: AtomicUsize,
    resources:       HashMap<ResourceId, String>,
    publishers:      HashMap<Id, Publisher>,
    subscribers:     HashMap<Id, Subscriber>,
    storages:        HashMap<Id, Storage>,
    evals:           HashMap<Id, Eval>,
    queries:         HashMap<ZInt, Arc<RwLock<RepliesHandler>>>,
}

impl InnerSession {
    pub(crate) fn new() -> InnerSession {
        InnerSession  { 
            primitives:      None,
            rid_counter:     AtomicUsize::new(1),  // Note: start at 1 because 0 is reserved for NO_RESOURCE
            qid_counter:     AtomicU64::new(0),
            decl_id_counter: AtomicUsize::new(0),
            resources:       HashMap::new(),
            publishers:      HashMap::new(),
            subscribers:     HashMap::new(),
            storages:        HashMap::new(),
            evals:           HashMap::new(),
            queries:         HashMap::new(),
        }
    }
}

impl InnerSession {
    pub fn reskey_to_resname(&self, reskey: &ResKey) -> ZResult<String> {
        use super::ResKey::*;
        match reskey {
            RName(name) => Ok(name.clone()),
            RId(rid) => {
                match self.resources.get(&rid) {
                    Some(name) => Ok(name.clone()),
                    None => Err(zerror!(ZErrorKind::UnkownResourceId{rid: *rid}))
                }
            },
            RIdWithSuffix(rid, suffix) => {
                match self.resources.get(&rid) {
                    Some(name) => Ok(name.clone() + suffix),
                    None => Err(zerror!(ZErrorKind::UnkownResourceId{rid: *rid}))
                }
            }
        }
    }
}

impl fmt::Debug for InnerSession {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "InnerSession{{ subscribers: {} }}",
            self.subscribers.len())
    }
}


