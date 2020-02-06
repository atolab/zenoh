use async_std::sync::{
    Arc,
    RwLock,
    Weak
};
use async_trait::async_trait;
use std::collections::HashMap;

use crate::{
    ArcSelf,
    impl_arc_self
};
use crate::proto::{
    Locator,
    Message
};
use crate::session::{
    Session,
    Link,
    LinkManager,
    SessionCallback
};
use crate::session::link::*;


const MANAGER_SID: usize = 0;

// Define an empty SessionCallback for the listener session
struct EmptyCallback {}

impl EmptyCallback {
    fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl SessionCallback for EmptyCallback {
    async fn receive_message(&self, _message: Message) -> async_std::io::Result<()> {
        Ok(())
    }

    async fn new_session(&self, _session: Arc<Session>) {
    }
}


pub struct SessionManager {
    weak_self: RwLock<Weak<Self>>,
    callback: Arc<dyn SessionCallback + Send + Sync>,
    listener: RwLock<HashMap<Locator, Arc<dyn LinkManager + Send + Sync>>>,
    session: RwLock<HashMap<usize, Arc<Session>>>
}

impl_arc_self!(SessionManager);
impl SessionManager {
    pub fn new(callback: Arc<dyn SessionCallback + Send + Sync>) -> Self {
        Self {
            weak_self: RwLock::new(Weak::new()),
            callback: callback,
            listener: RwLock::new(HashMap::new()),
            session: RwLock::new(HashMap::new())
        }  
    }

    /*************************************/
    /*               INIT                */
    /*************************************/
    pub async fn initialize(&self) {
        // Create the default session for the listeners
        let empty_callback = Arc::new(EmptyCallback::new());
        let session = Arc::new(Session::new(MANAGER_SID, self.get_arc_self(), empty_callback));
        self.session.write().await.insert(MANAGER_SID, session);
    }

    /*************************************/
    /*      LOCATOR AND LINK MANAGER     */
    /*************************************/
    #[inline]
    pub async fn add_locator(&self, locator: &Locator, limit: Option<usize>) -> Option<Arc<dyn LinkManager + Send + Sync>> {
        let link_manager = self.new_link_manager(locator, limit).await;
        let old = self.listener.write().await.insert(locator.clone(), link_manager.clone());
        // match link_manager.start() {
        //     Ok(_) => (),
        //     Err(_) => ()
        // };
        old
    }

    pub async fn del_locator(&self, locator: &Locator) -> Option<Arc<dyn LinkManager + Send + Sync>> {
        let mut guard = self.listener.write().await;
        if let Some(manager) = guard.get_mut(&locator) {
            // match manager.stop() {
            //     Ok(_) => (),
            //     Err(_) => ()
            // };
            return guard.remove(&locator)
        }
        return None
    }

    async fn new_link_manager(&self, locator: &Locator, limit: Option<usize>) -> Arc<dyn LinkManager + Send + Sync> {
        let session = self.session.read().await.get(&MANAGER_SID).unwrap().clone();
        let listener: Arc<dyn LinkManager + Send + Sync> = match locator {
            Locator::Tcp(socket_addr) => {
                let l = Arc::new(ManagerTcp::new(socket_addr.clone(), session, limit));
                l.set_arc_self(&l);
                l
            },
            Locator::Udp(socket_addr) => {
                let l = Arc::new(ManagerUdp::new(socket_addr.clone(), session, limit));
                l.set_arc_self(&l);
                l
            }
        };
        return listener
    }

    /*************************************/
    /*              SESSION              */
    /*************************************/
    #[inline]
    pub async fn add_session(&self, session: Arc<Session>) -> Option<Arc<Session>> {
        self.session.write().await.insert(session.id, session)
    }

    #[inline]
    pub async fn del_session(&mut self, id: usize) -> Option<Arc<Session>> {
        let mut guard = self.session.write().await;
        if let Some(session) = guard.get_mut(&id) {
            session.stop().await;
            return guard.remove(&id)
        }
        return None
    }

    #[inline]
    pub async fn new_session(&self) -> Arc<Session> {
        let mut guard = self.session.write().await;
        let id = guard.len();
        let session = Arc::new(Session::new(id, self.get_arc_self(), self.callback.clone()));
        guard.insert(session.id, session.clone());
        return session
    }

    pub async fn open_session(&self, locator: &Locator) -> Result<Arc<Session>, ()> {
        let session = self.new_session().await;
        // let link: Arc<dyn Link + Send + Sync> = match locator {
        //     Locator::Tcp(socket_addr) => {
        //         let l = Arc::new(LinkTcp::open(socket_addr.clone(), session));
        //     },
        //     Locator::Udp(socket_addr) => {
        //         let l = Arc::new(LinkUdp::new(socket_addr.clone(), session));
        //     }
        // };
        Ok(session)
    }

    /*************************************/
    /*              PROCESS              */
    /*************************************/
    pub async fn process(&self, session: usize, locator: &Locator, message: Message) {
        println!("PROCESSING: {:?} {:?} {:?}", session, locator, message);
        // let session = self.session.read().await.get(&session).unwrap().clone();
        // let link = session.del_link(locator.clone()).await.unwrap();
        // println!("1: {:?}", link.get_locator());
        // let ns = self.new_session().await;
        // println!("2: {}", ns.id);
        // ns.add_link(link.clone()).await;
        // link.set_session(ns).await;
    }
}