use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::HashMap;
use std::mem::MaybeUninit;
use async_std::task;
use async_std::sync::Arc;
use async_trait::async_trait;
use spin::RwLock;
use rand::prelude::*;
use zenoh_protocol:: {
    core::{ rname, PeerId, ResourceId, ResKey, ZError, ZErrorKind },
    io::ArcSlice,
    proto::{ Primitives, QueryTarget, QueryConsolidation, ReplySource, WhatAmI },
    link::Locator,
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

    pub(super) fn new(locator: &str, ps: Option<Properties>) -> Session {
        task::block_on( async {
    
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
        })
    }

    pub fn close(self) -> ZResult<()> {
        // @TODO: implement
        println!("---- CLOSE");
        let ref mut inner = self.inner.write();
        let primitives = inner.primitives.as_ref().unwrap();

        let res = task::block_on( async {
            primitives.close().await;

            if let Some(tx_session) = &self.tx_session {
                return tx_session.close().await
            }
            Ok(())
        });

        // @TODO: session_manager.del_locator()
        res
    }

    pub fn info(&self) -> Properties {
        // @TODO: implement
        println!("---- INFO");
        let mut info = Properties::new();
        info.insert(ZN_INFO_PEER_KEY, "tcp/somewhere:7887".as_bytes().to_vec());
        info.insert(ZN_INFO_PID_KEY, vec![1u8, 2, 3]);
        info.insert(ZN_INFO_PEER_PID_KEY, vec![4u8, 5, 6]);
        info
    }

    pub fn declare_resource(&self, resource: &ResKey) -> ZResult<ResourceId> {
        let ref mut inner = self.inner.write();
        let primitives = inner.primitives.as_ref().unwrap();
        let rid = inner.rid_counter.fetch_add(1, Ordering::SeqCst) as ZInt;

        task::block_on( async {
            primitives.resource(rid, &resource).await;
        });

        let rname = inner.reskey_to_resname(resource)?;
        inner.resources.insert(rid, rname);

        println!("---- DECL RES {} => {:?}", resource, rid);
        Ok(rid)
    }

    pub fn undeclare_resource(&self, rid: &ResourceId) -> ZResult<()> {
        println!("---- UNDECL RES {}", rid);
        self.inner.write().resources.remove(rid);
        Ok(())
    }

    pub fn declare_publisher(&self, resource: &ResKey) -> ZResult<Publisher> {
        // @TODO: implement
        let ref mut inner = self.inner.write();
        let id = inner.id_counter.fetch_add(1, Ordering::SeqCst);
        let publ = Publisher{ id };
        println!("---- DECL PUB on {} => {:?}", resource, publ);
        inner.publishers.insert(id, publ.clone());
        Ok(publ)
    }

    pub fn undeclare_publisher(&self, publisher: Publisher) -> ZResult<()> {
        // @TODO: implement
        println!("---- UNDECL PUB {:?}", publisher);
        self.inner.write().publishers.remove(&publisher.id);
        Ok(())
    }

    pub fn declare_subscriber<DataHandler>(&self, resource: &ResKey, mode: &SubMode, data_handler: DataHandler) -> ZResult<Subscriber>
        where DataHandler: FnMut(/*res_name:*/ &str, /*payload:*/ &[u8], /*data_info:*/ &[u8]) + Send + Sync + 'static
    {
        let ref mut inner = self.inner.write();
        let resname = inner.reskey_to_resname(resource)?;
        let primitives = inner.primitives.as_ref().unwrap();

        task::block_on( async {
            primitives.subscriber(resource, mode).await;
        });

        let id = inner.id_counter.fetch_add(1, Ordering::SeqCst);
        let dhandler = Arc::new(RwLock::new(data_handler));
        let sub = Subscriber{ id, resname, dhandler, session: self.inner.clone() };
        inner.subscribers.insert(id, sub.clone());
        println!("---- DECL SUB on {} with {:?}  => {:?}", resource, mode, sub);        
        Ok(sub)
    }

    pub fn undeclare_subscriber(&self, subscriber: Subscriber) -> ZResult<()>
    {
        // @TODO: implement
        println!("---- UNDECL SUB {:?}", subscriber);
        self.inner.write().subscribers.remove(&subscriber.id);
        Ok(())
    }

    pub fn declare_storage<DataHandler, QueryHandler>(&self, resource: &ResKey, data_handler: DataHandler, query_handler: QueryHandler) -> ZResult<Storage>
        where DataHandler: FnMut(/*res_name:*/ &str, /*payload:*/ &[u8], /*data_info:*/ &[u8]) + Send + Sync + 'static ,
        QueryHandler: FnMut(/*res_name:*/ &str, /*predicate:*/ &str, /*replies_sender:*/ &RepliesSender, /*query_handle:*/ QueryHandle) + Send + Sync + 'static
    {
        // @TODO: implement
        let ref mut inner = self.inner.write();
        let id = inner.id_counter.fetch_add(1, Ordering::SeqCst);
        let dhandler = Arc::new(RwLock::new(data_handler));
        let qhandler = Arc::new(RwLock::new(query_handler));
        let sto = Storage{ id, dhandler, qhandler };
        println!("---- DECL STO on {} => {:?}", resource, sto);
        inner.storages.insert(id, sto.clone());

        // Just to test storage callback:
        let payload = vec![1,2,3];
        let info = vec![4,5,6];
        let ref mut sto2 = inner.storages.get(&id).unwrap();
        let ref mut dhandler = *sto2.dhandler.write();
        dhandler("/A/B".into(), &payload, &info);
        let ref mut qhandler = *sto2.qhandler.write();
        qhandler("/A/**".into(), "starttime=now()-1h".into(), 
            &|handle, replies| {
                println!("------- STORAGE RepliesSender for {} {:?}", handle, replies);
            },
            42
        );

        Ok(sto)
    }

    pub fn undeclare_storage(&self, storage: Storage) -> ZResult<()> {
        // @TODO: implement
        println!("---- UNDECL STO {:?}", storage);
        self.inner.write().storages.remove(&storage.id);
        Ok(())
    }

    pub fn declare_eval<QueryHandler>(&self, resource: &ResKey, query_handler: QueryHandler) -> ZResult<Eval>
        where QueryHandler: FnMut(/*res_name:*/ &str, /*predicate:*/ &str, /*replies_sender:*/ &RepliesSender, /*query_handle:*/ QueryHandle) + Send + Sync + 'static
    {
        // @TODO: implement
        let ref mut inner = self.inner.write();
        let id = inner.id_counter.fetch_add(1, Ordering::SeqCst);
        let qhandler = Arc::new(RwLock::new(query_handler));
        let eva = Eval{ id, qhandler };
        println!("---- DECL EVAL on {} => {:?}", resource, eva);
        inner.evals.insert(id, eva.clone());

        // Just to test eval callback:
        let ref mut eva2 = inner.evals.get(&id).unwrap();
        let ref mut qhandler = *eva2.qhandler.write();
        qhandler("/A/**".into(), "starttime=now()-1h".into(), 
            &|handle, replies| {
                println!("------- EVAL RepliesSender for {} {:?}", handle, replies);
            },
            42
        );

        Ok(eva)

    }

    pub fn undeclare_eval(&self, eval: Eval) -> ZResult<()> {
        // @TODO: implement
        println!("---- UNDECL EVA {:?}", eval);
        Ok(())
    }

    pub fn write(&self, resource: &ResKey, payload: &[u8]) -> ZResult<()> {
        println!("---- WRITE on {} : {:02x?}", resource, payload);
        let inner = self.inner.read();
        let primitives = inner.primitives.as_ref().unwrap();
        task::block_on( async {
            primitives.data(resource, &None, &payload.to_vec().into()).await;
        });
        Ok(())
    }

    pub fn query<RepliesHandler>(&self, resource: &ResKey, predicate: &str, mut replies_handler: RepliesHandler) -> ZResult<()>
        where RepliesHandler: FnMut(/*res_name:*/ &str, /*payload:*/ &[u8], /*data_info:*/ &[u8]) + Send + Sync + 'static
    {
        // @TODO: implement
        println!("---- QUERY on {} {}", resource, predicate);


        // Just to test reply_handler callback:
        let payload = vec![1,2,3];
        let info = vec![];
        replies_handler("/A/B".into(), &payload, &info);

        Ok(())
    }

}

#[async_trait]
impl Primitives for Session {

    async fn resource(&self, rid: ZInt, reskey: &ResKey) {
        println!("++++ recv Resource {} {:?} ", rid, reskey);
    }

    async fn forget_resource(&self, rid: ZInt) {
        println!("++++ recv Forget Resource {} ", rid);
    }

    async fn publisher(&self, reskey: &ResKey) {
        println!("++++ recv Publisher {:?} ", reskey);
    }

    async fn forget_publisher(&self, reskey: &ResKey) {
        println!("++++ recv Forget Publisher {:?} ", reskey);
    }

    async fn subscriber(&self, reskey: &ResKey, mode: &SubMode) {
        println!("++++ recv Subscriber {:?} ", reskey);
    }

    async fn forget_subscriber(&self, reskey: &ResKey) {
        println!("++++ recv Forget Subscriber {:?} ", reskey);
    }

    async fn storage(&self, reskey: &ResKey) {
        println!("++++ recv Storage {:?} ", reskey);
    }

    async fn forget_storage(&self, reskey: &ResKey) {
        println!("++++ recv Forget Storage {:?} ", reskey);
    }
    
    async fn eval(&self, reskey: &ResKey) {
        println!("++++ recv Eval {:?} ", reskey);
    }

    async fn forget_eval(&self, reskey: &ResKey) {
        println!("++++ recv Forget Eval {:?} ", reskey);
    }

    async fn data(&self, reskey: &ResKey, info: &Option<ArcSlice>, payload: &ArcSlice) {
        println!("++++ recv Data {:?} : {:?} ", reskey, payload);
        let inner = self.inner.read();
        let primitives = inner.primitives.as_ref().unwrap();
        match inner.reskey_to_resname(reskey) {
            Ok(resname) =>
                for (_, sub) in &inner.subscribers {
                    if rname::intersect(&sub.resname, &resname) {
                        let info = vec![4,5,6];   // @TODO
                        let ref mut handler = *sub.dhandler.write();
                        handler(&resname, payload.as_slice(), &info);
                    }
                },
            Err(err) => println!("{}. Dropping received data", err)
        }
    }

    async fn query(&self, reskey: &ResKey, predicate: &str, qid: ZInt, target: &Option<QueryTarget>, consolidation: &QueryConsolidation) {
        println!("++++ recv Query {:?} ? {} ", reskey, predicate);
    }

    async fn reply(&self, qid: ZInt, source: &ReplySource, replierid: &Option<PeerId>, reskey: &ResKey, info: &Option<ArcSlice>, payload: &ArcSlice) {
        println!("++++ recv Reply {} : {:?} ", qid, reskey);
    }

    async fn pull(&self, is_final: bool, reskey: &ResKey, pull_id: ZInt, max_samples: &Option<ZInt>) {
        println!("++++ recv Pull {:?} ", reskey);
    }

    async fn close(&self) {
        println!("++++ recv Close ");
    }
}



pub(crate) struct InnerSession {
    primitives:      Option<Arc<dyn Primitives + Send + Sync>>, // @TODO replace with MaybeUninit ??
    rid_counter:     AtomicUsize,
    id_counter:      AtomicUsize,
    resources:       HashMap<ResourceId, String>,
    publishers:      HashMap<Id, Publisher>,
    subscribers:     HashMap<Id, Subscriber>,
    storages:        HashMap<Id, Storage>,
    evals:           HashMap<Id, Eval>,
}

impl InnerSession {
    pub(crate) fn new() -> InnerSession {
        InnerSession  { 
            primitives:  None,
            rid_counter: AtomicUsize::new(0),
            id_counter:  AtomicUsize::new(0),
            resources:   HashMap::new(),
            publishers:  HashMap::new(),
            subscribers: HashMap::new(),
            storages:    HashMap::new(),
            evals:       HashMap::new(),
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
                    None => Err(zerror!(ZErrorKind::UnkownResourceId{rid: rid.clone()}))
                }
            },
            RIdWithSuffix(rid, suffix) => {
                match self.resources.get(&rid) {
                    Some(name) => Ok(name.clone() + suffix),
                    None => Err(zerror!(ZErrorKind::UnkownResourceId{rid: rid.clone()}))
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


