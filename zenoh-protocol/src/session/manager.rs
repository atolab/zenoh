use async_std::sync::{
    Arc,
    channel,
    RwLock,
    Sender,
    Weak
};
use std::collections::HashMap;
use uuid::Uuid;

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
    LinkManager,
    Session,
    SessionCallback
};
use crate::session::link::*;


pub struct SessionManager {
    weak_self: RwLock<Weak<Self>>,
    callback: Arc<dyn SessionCallback + Send + Sync>,
    manager: RwLock<HashMap<LocatorProtocol, Arc<dyn LinkManager + Send + Sync>>>,
    session: RwLock<HashMap<Uuid, Arc<Session>>>,
    pending: RwLock<HashMap<Uuid, Arc<Session>>>,
    channel: RwLock<HashMap<Uuid, Sender<Result<Arc<Session>, ZError>>>>
}

zarcself!(SessionManager);
impl SessionManager {
    pub fn new(callback: Arc<dyn SessionCallback + Send + Sync>) -> Self {
        Self {
            weak_self: RwLock::new(Weak::new()),
            callback: callback,
            manager: RwLock::new(HashMap::new()),
            session: RwLock::new(HashMap::new()),
            pending: RwLock::new(HashMap::new()),
            channel: RwLock::new(HashMap::new())
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
                // let m = Arc::new(ManagerUdp::new());
                // m.set_arc_self(&m);
                // self.manager.write().await.insert(LocatorProtocol::Udp, m);
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
        let res = self.session.write().await.insert(session.get_id(), session.clone());
        session.set_callback(self.callback.clone()).await;
        return res
    }

    pub async fn del_session(&self, id: Uuid, reason: Option<ZError>) -> Result<Arc<Session>, ZError> {
        // Check if it is stored in the established sessions
        let mut guard = self.session.write().await;
        if let Some(_) = guard.get_mut(&id) {
            return Ok(guard.remove(&id).unwrap())
        }
        drop(guard);

        // Check if it is stored in the pending sessions
        let mut guard = self.pending.write().await;
        if let Some(session) = guard.get_mut(&id) {
            // If it is pending, check if the open_session created a channel that needs to be notified
            if let Some(channel) = self.channel.write().await.get(&session.get_id()) {
                channel.send(Err(reason.unwrap())).await;
            }
            return Ok(guard.remove(&id).unwrap())
        }
        drop(guard);

        return Err(zerror!(ZErrorKind::Other{
            msg: format!("Trying to delete a session that does not exist!")
        }))
    }

    pub async fn new_session(&self) -> Arc<Session> {
        // Generate random session ID
        let id = Uuid::new_v4();
        // Create and initialize a new session
        let session = Arc::new(Session::new(id, self.get_arc_self()));
        session.initialize(&session).await;
        // Store the new session
        self.pending.write().await.insert(session.get_id(), session.clone());
        return session
    }

    pub async fn open_session(&self, locator: &Locator) -> Result<Arc<Session>, ZError> {
        // Automatically create a new link manager for the protocol if it does not exist
        match self.new_manager(&locator.get_proto()).await {
            Ok(_) => (),
            Err(e) => return Err(e)
        }

        // Acquire a read lock on the managers
        let guard = self.manager.read().await;
        let manager = match guard.get(&locator.get_proto()) {
            Some(manager) => manager,
            None => return Err(zerror!(ZErrorKind::Other{
                msg: format!("Trying to add a link to a Link Manager that does not exist!")
            }))
        };

        // Create a new empty session
        let session = self.new_session().await;

        // Create a new link calling the Link Manager
        let link = match manager.new_link(locator, session.clone()).await {
            Ok(link) => link,
            Err(e) => return Err(e)
        };

        // Drop the read guard to not keep the lock while waiting for the channel
        drop(guard);

        // Create a channel for knowing when a session is open
        let (sender, receiver) = channel::<Result<Arc<Session>, ZError>>(1);
        self.channel.write().await.insert(session.get_id(), sender);

        // Send the Open Message
        // TODO: fix the make_open message parameters
        let message = Arc::new(Message::make_open(
            0, None, PeerId{id: Vec::new()}, 0, None, None, None
        ));
        match link.send(message.clone()).await {
            Ok(_) => {},
            Err(e) => return Err(e)
        }

        // Wait the accept message to finalize the session
        match receiver.recv().await.unwrap() {
            Ok(session) => {
                // Store the session in the list of active sessions
                self.add_session(session.clone()).await;
                // Set the current session on the link
                link.set_session(session.clone()).await.unwrap();
                return Ok(session)
            },
            Err(e) => {
                return Err(e)
            }
        }
    }

    pub async fn close_session(&self, id: Uuid, reason: Option<ZError>) -> Result<(), ZError> {   
        match self.del_session(id, None).await {
            Ok(session) => return session.close(reason).await,
            Err(e) => return Err(e)
        }
    }

    /*************************************/
    /*              PROCESS              */
    /*************************************/
    pub async fn process_message(&self, session: Arc<Session>, src: &Locator, dst: &Locator, message: Message) -> Result<(), ZError> {
        // Process the message
        match &message.body {
            Body::Accept{ opid, apid, lease } => {
                match self.channel.write().await.remove(&session.get_id()) {
                    Some(sender) => {
                        // Need to fix the PeerID look up
                        // For the time being a create a new session for each accept
                        // In the feature a link could be added to an existing session
                        // let session = self.new_session().await;
                        if !self.session.read().await.contains_key(&session.get_id()) {
                            let _ = self.del_session(session.get_id(), None);
                        }
                        sender.send(Ok(session)).await;
                    },
                    None => return Err(zerror!(ZErrorKind::InvalidMessage{
                        reason: format!("Received an Accept from a non-open session!")
                    }))
                }
            },
            Body::Close{ pid, reason } => {},
            Body::Open{ version, whatami, pid, lease, locators } => {
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