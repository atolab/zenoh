use async_std::sync::{
    Arc,
    channel,
    Receiver,
    RwLock,
    Sender,
    Weak
};
use async_std::task::{
    Context, 
    Poll,
    Waker
};
use async_trait::async_trait;
// use crossbeam::channel::{
//     bounded,
//     Sender,
//     Receiver
// };
use std::collections::HashMap;

use crate::{
    ArcSelf,
    zarcself,
    zerror
};
use crate::core::{
    PeerId,
    ZError,
    ZErrorKind
};
use crate::proto::{
    Body,
    Locator,
    LocatorProtocol,
    Message
};
use crate::session::{
    EmptyCallback,
    LinkManager,
    Session,
    SessionCallback
};
use crate::session::link::*;


pub struct SessionManager {
    weak_self: RwLock<Weak<Self>>,
    callback: Arc<dyn SessionCallback + Send + Sync>,
    manager: RwLock<HashMap<LocatorProtocol, Arc<dyn LinkManager + Send + Sync>>>,
    session: RwLock<HashMap<usize, Arc<Session>>>,
    pending: RwLock<HashMap<Locator, Sender<Option<Arc<Session>>>>>
}

zarcself!(SessionManager);
impl SessionManager {
    pub fn new(callback: Arc<dyn SessionCallback + Send + Sync>) -> Self {
        Self {
            weak_self: RwLock::new(Weak::new()),
            callback: callback,
            manager: RwLock::new(HashMap::new()),
            session: RwLock::new(HashMap::new()),
            pending: RwLock::new(HashMap::new())
        }  
    }

    /*************************************/
    /*               INIT                */
    /*************************************/
    pub async fn initialize(&self, arc_self: &Arc<Self>) {
        self.set_arc_self(arc_self);
    }

    /*************************************/
    /*              LISTENER             */
    /*************************************/
    pub async fn new_listener(&self, locator: &Locator, limit: Option<usize>) -> Result<(), ZError> {
        // Automatically create a new link manager for the protocol if it does not exist
        match self.new_manager(&locator.get_proto()).await {
            Ok(_) => (),
            Err(e) => return Err(e)
        }
        match self.manager.read().await.get(&locator.get_proto()) {
            Some(manager) => {
                match manager.new_listener(locator, limit).await {
                    Ok(_) => return Ok(()),
                    Err(e) => return Err(e)
                }
            },
            None => return Err(zerror!(ZErrorKind::Other{
                msg: format!("Trying to add a listener to a Link Manager that does not exist!")
            }))
        }
    }

    pub async fn del_listener(&self, locator: &Locator) -> Result<(), ZError> {
        match self.manager.read().await.get(&locator.get_proto()) {
            Some(manager) => {
                match manager.del_listener(locator).await {
                    Ok(_) => return Ok(()),
                    Err(e) => return Err(e)
                }
            },
            None => return Err(zerror!(ZErrorKind::Other{
                msg: format!("Trying to delete a listener from a Link Manager that does not exist!")
            }))
        }
    }

    /*************************************/
    /*            LINK MANAGER           */
    /*************************************/
    async fn new_manager(&self, protocol: &LocatorProtocol) -> Result<(), ZError> {
        // let session = self.session.read().await.get(&MANAGER_SID).unwrap().clone();
        let r_guard = self.manager.read().await;
        match protocol {
            LocatorProtocol::Tcp => if !r_guard.contains_key(&LocatorProtocol::Tcp) {
                drop(r_guard);
                let m = Arc::new(ManagerTcp::new(self.get_arc_self()));
                m.set_arc_self(&m);
                self.manager.write().await.insert(LocatorProtocol::Tcp, m);
            },
            LocatorProtocol::Udp => if !r_guard.contains_key(&LocatorProtocol::Udp) {
                drop(r_guard);
                let m = Arc::new(ManagerUdp::new());
                m.set_arc_self(&m);
                self.manager.write().await.insert(LocatorProtocol::Udp, m);
            }
        }
        Ok(())
    }

    async fn del_manager(&self, protocol: &LocatorProtocol) -> Result<(), ZError> {
        match self.manager.write().await.remove(protocol) {
            Some(_) => Ok(()),
            None => Err(zerror!(ZErrorKind::Other{
                msg: format!("No available Link Manager for protocol: {}", protocol)
            }))
        }
    }

    /*************************************/
    /*              SESSION              */
    /*************************************/
    async fn add_session(&self, session: Arc<Session>) -> Option<Arc<Session>> {
        self.session.write().await.insert(session.get_id(), session)
    }

    async fn del_session(&mut self, id: usize) -> Option<Arc<Session>> {
        let mut guard = self.session.write().await;
        if let Some(session) = guard.get_mut(&id) {
            session.close().await;
            return guard.remove(&id)
        }
        return None
    }

    async fn new_session(&self) -> Arc<Session> {
        let mut guard = self.session.write().await;
        // TOFIX: How to assign the session ID?
        let id = guard.len();
        // Create and initialize a new session
        let session = Arc::new(Session::new(id, self.get_arc_self(), self.callback.clone()));
        session.initialize(&session).await;
        // Store the new session
        guard.insert(session.get_id(), session.clone());
        return session
    }

    pub async fn open_session(&self, locator: &Locator) -> Result<Arc<Session>, ZError> {
        // Automatically create a new link manager for the protocol if it does not exist
        match self.new_manager(&locator.get_proto()).await {
            Ok(_) => (),
            Err(e) => return Err(e)
        }
        match self.manager.read().await.get(&locator.get_proto()) {
            Some(manager) => {
                // Create a new link
                let callback = Arc::new(EmptyCallback::new());
                let session = Arc::new(Session::new(0, self.get_arc_self(), callback));
                session.initialize(&session).await;
                // The real session is only created after the Open/Accept message exchange
                match manager.new_link(locator, session).await {
                    Ok(link) => {
                        println!("LINK CREATED!");
                        // Create a channel for knowing when a session is open
                        let (sender, receiver) = channel::<Option<Arc<Session>>>(1);
                        self.pending.write().await.insert(locator.clone(), sender);
                        // Send the Open Message
                        // TODO: fix the make_open message parameters
                        let message = Arc::new(Message::make_open(
                            0, None, PeerId{id: Vec::new()}, 0, None, None, None
                        ));
                        match link.send(message.clone()).await {
                            Ok(_) => {
                                // Wait the accept message to finalize the session
                                let session = receiver.recv().await.unwrap();
                                // Set the session on the link
                                if let Some(session) = session {
                                    link.set_session(session.clone()).await.unwrap();
                                    return Ok(session)
                                } else {
                                    return Err(zerror!(ZErrorKind::Other{
                                        msg: format!("Failed to open a connection")
                                    }))
                                }
                                
                            },
                            Err(e) => return Err(e)
                        };
                    },
                    Err(e) => return Err(e)
                }
            },
            None => return Err(zerror!(ZErrorKind::Other{
                msg: format!("Trying to add a link to a Link Manager that does not exist!")
            }))
        }
    }

    /*************************************/
    /*              PROCESS              */
    /*************************************/
    pub async fn process_message(&self, session: Arc<Session>, locator: &Locator, message: Message) -> Result<(), ZError> {
        // Process the message
        match &message.body {
            Body::Accept{ opid, apid, lease } => {
                match self.pending.write().await.remove(locator) {
                    Some(sender) => {
                        println!("ACCEPT from {}", locator);
                        // Need to fix the PeerID look up
                        // For the time being a create a new session for each accept
                        // In the feature a link could be added to an existing session
                        let session = self.new_session().await;
                        sender.send(Some(session)).await;
                    },
                    None => return Err(zerror!(ZErrorKind::InvalidMessage{
                        reason: format!("Received an Accept from a non-open session!")
                    }))
                }
            },
            Body::Close{ pid, reason } => {},
            Body::Open{ version, whatami, pid, lease, locators } => {
                println!("OPEN from {}", locator);
                // TODO: fix the make_accept message parameters
                let message = Arc::new(Message::make_accept(PeerId{id: Vec::new()}, PeerId{id: Vec::new()}, 0, None, None));
                session.schedule(message).await;
            },
            _ => return Err(zerror!(ZErrorKind::InvalidMessage{
                reason: format!("Message not allowed in the session manager: {:?}", message)
            }))
        }
        Ok(())
    }
}