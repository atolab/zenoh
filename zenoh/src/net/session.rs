use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::HashMap;
use async_std::sync::Arc;
use spin::RwLock;

use super::*;




#[derive(Clone, Debug)]
pub struct Session {
    inner: Arc<RwLock<InnerSession>>,
    info: Properties,
}

impl Session {

    pub(super) fn new(info: Properties) -> Session {
        let inner = Arc::new(RwLock::new(
            InnerSession::new()
        ));
        Session{ inner, info }
    }

    pub fn close(&self) -> ZResult<()> {
        // @TODO: implement
        println!("---- CLOSE");
        Ok(())
    }

    pub fn info(&self) -> &Properties {
        // @TODO: implement
        println!("---- INFO");
        &self.info
    }

    pub fn declare_resource(&self, resource: &ResourceKey) -> ZResult<ResourceId> {
        // @TODO: implement
        println!("---- DECL RES {}", resource);
        Ok(ResourceId { id:42 })
    }

    pub fn undeclare_resource(&self, rid: &ResourceId) -> ZResult<()> {
        // @TODO: implement
        println!("---- UNDECL RES {}", rid);
        Ok(())
    }

    pub fn declare_publisher(&self, resource: &ResourceKey) -> ZResult<Publisher> {
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

    pub fn declare_subscriber<DataHandler>(&self, resource: &ResourceKey, mode: SubMode, data_handler: DataHandler) -> ZResult<Subscriber>
        where DataHandler: FnMut(/*res_name:*/ &str, /*payload:*/ &[u8], /*data_info:*/ &[u8]) + 'static
    {
        // @TODO: implement
        let ref mut inner = self.inner.write();
        let id = inner.id_counter.fetch_add(1, Ordering::SeqCst);
        let dhandler = Arc::new(RwLock::new(data_handler));
        let sub = Subscriber{ id, dhandler, session: self.inner.clone() };
        println!("---- DECL SUB on {} with {:?}  => {:?}", resource, mode, sub);
        inner.subscribers.insert(id, sub.clone());

        // Just to test subscriber callback:
        let payload = vec![1,2,3];
        let info = vec![4,5,6];
        let ref mut handler = *inner.subscribers.get(&id).unwrap().dhandler.write();
        handler("/A/B".into(), &payload, &info);
        
        Ok(sub)
    }

    pub fn undeclare_subscriber(&self, subscriber: Subscriber) -> ZResult<()>
    {
        // @TODO: implement
        println!("---- UNDECL SUB {:?}", subscriber);
        self.inner.write().subscribers.remove(&subscriber.id);
        Ok(())
    }

    pub fn declare_storage<DataHandler, QueryHandler>(&self, resource: &ResourceKey, data_handler: DataHandler, query_handler: QueryHandler) -> ZResult<Storage>
        where DataHandler: FnMut(/*res_name:*/ &str, /*payload:*/ &[u8], /*data_info:*/ &[u8]) + 'static ,
        QueryHandler: FnMut(/*res_name:*/ &str, /*predicate:*/ &str, /*replies_sender:*/ &RepliesSender, /*query_handle:*/ QueryHandle) + 'static
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

    pub fn declare_eval<QueryHandler>(&self, resource: &ResourceKey, query_handler: QueryHandler) -> ZResult<Eval>
        where QueryHandler: FnMut(/*res_name:*/ &str, /*predicate:*/ &str, /*replies_sender:*/ &RepliesSender, /*query_handle:*/ QueryHandle) + 'static
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

    pub fn write(&self, resource: &ResourceKey, payload: &[u8]) -> ZResult<()> {
        // @TODO: implement
        println!("---- WRITE on {} : {:02x?}", resource, payload);
        Ok(())
    }

    pub fn query<RepliesHandler>(&self, resource: &ResourceKey, predicate: &str, mut replies_handler: RepliesHandler) -> ZResult<()>
        where RepliesHandler: FnMut(/*res_name:*/ &str, /*payload:*/ &[u8], /*data_info:*/ &[u8]) + 'static
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

type Id = usize;


pub(crate) struct InnerSession {
    id_counter:  AtomicUsize,
    publishers:  HashMap<Id, Publisher>,
    subscribers: HashMap<Id, Subscriber>,
    storages:    HashMap<Id, Storage>,
    evals:       HashMap<Id, Eval>,
}

impl InnerSession {
    pub(crate) fn new() -> InnerSession {
        InnerSession  { 
            id_counter:  AtomicUsize::new(0),
            publishers:  HashMap::new(),
            subscribers: HashMap::new(),
            storages:    HashMap::new(),
            evals:       HashMap::new(),
        }
    }
}

impl fmt::Debug for InnerSession {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "InnerSession{{ subscribers: {} }}",
            self.subscribers.len())
    }
}


#[derive(Clone)]
pub struct Publisher {
    id: Id,
}

impl PartialEq for Publisher {
    fn eq(&self, other: &Publisher) -> bool {
        self.id == other.id
    }
}

impl fmt::Debug for Publisher {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Publisher{{ id:{} }}", self.id)
    }
}


#[derive(Clone)]
pub struct Subscriber {
    id: Id,
    dhandler: Arc<RwLock<DataHandler>>,
    session: Arc<RwLock<InnerSession>>
}

impl Subscriber {
    pub fn pull(&self) -> ZResult<()> {
        // @TODO: implement
        println!("---- PULL on {:?}", self);
        Ok(())
    }
}

impl PartialEq for Subscriber {
    fn eq(&self, other: &Subscriber) -> bool {
        self.id == other.id
    }
}

impl fmt::Debug for Subscriber {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Subscriber{{ id:{} }}", self.id)
    }
}

#[derive(Clone)]
pub struct Storage {
    id: Id,
    dhandler: Arc<RwLock<DataHandler>>,
    qhandler: Arc<RwLock<QueryHandler>>,
}

impl PartialEq for Storage {
    fn eq(&self, other: &Storage) -> bool {
        self.id == other.id
    }
}

impl fmt::Debug for Storage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Storage{{ id:{} }}", self.id)
    }
}


#[derive(Clone)]
pub struct Eval {
    id: Id,
    qhandler: Arc<RwLock<QueryHandler>>,
}

impl PartialEq for Eval {
    fn eq(&self, other: &Eval) -> bool {
        self.id == other.id
    }
}

impl fmt::Debug for Eval {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Eval{{ id:{} }}", self.id)
    }
}
