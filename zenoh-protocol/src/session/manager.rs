use async_std::sync::{
    Arc,
    channel,
    RwLock,
    Sender
};
use std::collections::HashMap;
use std::fmt;

use crate::{
    zerror,
    zrwopt
};
use crate::core::{
    PeerId,
    ZError,
    ZErrorKind,
    ZInt,
    ZResult
};
use crate::proto::{
    Message,
    WhatAmI
};
use crate::session::{
    EmptyCallback,
    Transport,
    SessionCallback,
    SessionHandler
};
use crate::link::{
    Link,
    LinkManager,
    Locator,
    LocatorProtocol,
};


pub struct SessionManager(Arc<SessionManagerInner>);

impl SessionManager {
    pub fn new(version: u8, whatami: WhatAmI, id: PeerId, lease: ZInt, 
        handler: Arc<dyn SessionHandler + Send + Sync>
    ) -> Self {
        // Create the inner session manager
        let inner = Arc::new(SessionManagerInner::new(version, whatami, id.clone(), lease, handler));

        // Create a session used to establish new connections
        // This session wrapper does not require to contact the upper layer
        let callback = Arc::new(EmptyCallback::new());
        let session = Arc::new(Session::new(inner.clone(), 0, id, lease, callback));
        // Start the session
        session.initialize(&session);
        // Add the session to the inner session manager
        // At this stage there is no contention, the try_write will succeed
        *inner.initial.try_write().unwrap() = Some(session);

        Self(inner)
    }

    /*************************************/
    /*              SESSION              */
    /*************************************/
    pub async fn open_session(&self, locator: &Locator) -> ZResult<Arc<Session>> {
        // Automatically create a new link manager for the protocol if it does not exist
        let manager = self.0.new_link_manager(&self.0, &locator.get_proto()).await?;

        // Create a channel for knowing when a session is open
        let (sender, receiver) = channel::<ZResult<NotificationNewSession>>(1);

        zrwopt!(self.0.initial).open(manager, locator, sender).await?;

        // Wait the accept message to finalize the session
        let notification = match receiver.recv().await {
            Some(session) => match session {
                Ok(s) => s,
                Err(e) => return Err(e)
            },
            None => return Err(zerror!(ZErrorKind::Other{
                descr: format!("Open session failed unexpectedly!")
            }))
        };

        // Check if an already established session exists with the peer
        let session = self.0.get_or_new_session(&self.0, &notification.peer).await?;

        // Move the link to the target session
        self.0.move_link(&notification.dst, &notification.src, &session.transport).await?;

        // Set the lease on the session
        session.transport.set_lease(notification.lease);

        Ok(session)
    }

    pub async fn close_session(&self, peer: &PeerId, reason: Option<ZError>) -> ZResult<()> {
        let session = self.0.del_session(peer, None).await?;
        session.close(reason).await
    }

    pub async fn get_sessions(&self) -> Vec<Arc<Session>> {
        let mut vec = Vec::new();
        for s in self.0.session.read().await.values() {
            vec.push(s.clone());
        }
        vec
    }


    /*************************************/
    /*              LISTENER             */
    /*************************************/
    pub async fn add_locator(&self, locator: &Locator, _limit: Option<usize>) -> ZResult<()> {
        let manager = self.0.new_link_manager(&self.0, &locator.get_proto()).await?;
        manager.new_listener(locator).await
    }

    pub async fn del_locator(&self, locator: &Locator) -> ZResult<()> {
        let manager = self.0.get_link_manager(&locator.get_proto()).await?;
        manager.del_listener(locator).await?;
        if manager.get_listeners().await.len() == 0 {
            self.0.del_link_manager(&locator.get_proto()).await?;
        }
        Ok(())
    }

    pub async fn get_locators(&self) -> Vec<Locator> {
        self.0.get_locators().await
    }
}

struct IDManager(Vec<usize>);

impl IDManager {
    fn new() -> Self {
        let mut v = Vec::new();
        v.push(0);
        Self(v)
    }

    fn new_id(&mut self) -> usize {
        let id = self.0.remove(0);
        if self.0.len() == 0 {
            self.0.insert(0, id + 1);
        }
        id
    }

    fn del_id(&mut self, id: usize) {
        for i in 0..self.0.len() {
            if self.0[i] > id {
                self.0.insert(i, id);
                return
            }
        }
        self.0.push(id);
    }
}

pub struct SessionManagerInner {
    pub(crate) version: u8,
    pub(crate) whatami: WhatAmI,
    pub(crate) id: PeerId,
    pub(crate) lease: ZInt,
    handler: Arc<dyn SessionHandler + Send + Sync>,
    initial: RwLock<Option<Arc<Session>>>,
    protocol: RwLock<HashMap<LocatorProtocol, Arc<LinkManager>>>,
    session: RwLock<HashMap<PeerId, Arc<Session>>>,
    id_mgmt: RwLock<IDManager>,
}

impl SessionManagerInner {
    fn new(version: u8, whatami: WhatAmI, id: PeerId, lease: ZInt, 
        handler: Arc<dyn SessionHandler + Send + Sync>
    ) -> Self {
        Self {
            version,
            whatami,
            id,
            lease,
            handler,
            initial: RwLock::new(None),
            protocol: RwLock::new(HashMap::new()),
            session: RwLock::new(HashMap::new()),
            id_mgmt: RwLock::new(IDManager::new())
        }
    }

    /*************************************/
    /*            LINK MANAGER           */
    /*************************************/
    async fn new_link_manager(&self, a_self: &Arc<Self>, protocol: &LocatorProtocol) -> ZResult<Arc<LinkManager>> {
        match self.get_link_manager(protocol).await {
            Ok(manager) => Ok(manager),
            Err(_) => {
                let lm = Arc::new(LinkManager::new(a_self.clone(), protocol));
                self.protocol.write().await.insert(protocol.clone(), lm.clone());
                Ok(lm)
            }
        }
    }

    async fn get_link_manager(&self, protocol: &LocatorProtocol) -> ZResult<Arc<LinkManager>> {
        match self.protocol.read().await.get(protocol) {
            Some(manager) => Ok(manager.clone()),
            None => Err(zerror!(ZErrorKind::Other{
                descr: format!("Link Manager not found for protocol ({})", protocol)
            }))
        }
    }

    async fn del_link_manager(&self, protocol: &LocatorProtocol) -> ZResult<()> {
        match self.protocol.write().await.remove(protocol) {
            Some(_) => Ok(()),
            None => Err(zerror!(ZErrorKind::Other{
                descr: format!("No available Link Manager for protocol: {}", protocol)
            }))
        }
    }

    async fn get_locators(&self) -> Vec<Locator> {
        let mut vec: Vec<Locator> = Vec::new();
        for p in self.protocol.read().await.values() {
            vec.extend_from_slice(&p.get_listeners().await);
        }
        vec
    }

    async fn move_link(&self, src: &Locator, dst: &Locator, transport: &Arc<Transport>) -> ZResult<()> {
        let manager = self.get_link_manager(&src.get_proto()).await?;
        manager.move_link(src, dst, transport.clone()).await
    }

    /*************************************/
    /*              SESSION              */
    /*************************************/
    pub(crate) fn get_initial_session(&self) -> Arc<Session> {
        zrwopt!(self.initial).clone()
    }

    // async fn find_session(&self, id: usize) -> Option<Arc<SessionWrapper>> {
    //     let mut session = None;
    //     for s in self.session.read().await.values() {
    //         if s.get_session().await.get_id() == id {
    //             session = Some(s.clone());
    //             break
    //         }
    //     }
    //     session
    // }

    async fn get_or_new_session(&self, a_self: &Arc<Self>, peer: &PeerId) -> ZResult<Arc<Session>> {
        let r_guard = self.session.read().await;
        match r_guard.get(peer) {
            Some(wrapper) => Ok(wrapper.clone()),
            None => {
                drop(r_guard);
                self.new_session(a_self, peer).await
            }
        }
    }
    
    async fn del_session(&self, peer: &PeerId, _reason: Option<ZError>) -> ZResult<Arc<Session>> {
        match self.session.write().await.remove(peer) {
            Some(session) => {
                self.id_mgmt.write().await.del_id(session.id);
                self.handler.del_session(&session).await;
                Ok(session)
            }
            None =>  Err(zerror!(ZErrorKind::Other{
                descr: format!("Trying to delete a session that does not exist!")
            }))
        }
    }

    async fn new_session(&self, a_self: &Arc<Self>, peer: &PeerId) -> ZResult<Arc<Session>> {
        if let Some(_) = self.session.read().await.get(peer) {
            return Err(zerror!(ZErrorKind::Other{
                descr: format!("Session with peer ({:?}) already exists.", peer)
            }))
        }

        // Get the callback from the upper layer
        let callback = self.handler.get_callback(peer).await;
        // Dynamically create a new session ID
        let id = self.id_mgmt.write().await.new_id();
        // Create the session object
        let session = Arc::new(Session::new(a_self.clone(), id, peer.clone(), self.lease, callback));
        // Add the session to the list of active sessions
        self.session.write().await.insert(peer.clone(), session.clone());
        // Start the session 
        session.initialize(&session);
        // Notify the upper layer that a new session has been created
        self.handler.new_session(session.clone()).await;

        Ok(session)
    }
}


struct NotificationNewSession {
    peer: PeerId,
    lease: ZInt,
    src: Locator,
    dst: Locator
}

impl NotificationNewSession {
    fn new(peer: PeerId, lease: ZInt, src: Locator, dst: Locator) -> Self {
        Self { peer, lease, src, dst }
    }
}


pub struct Session {
    pub id: usize,
    pub peer: PeerId,
    pub(crate) transport: Arc<Transport>,
    manager: Arc<SessionManagerInner>,
    channel: RwLock<HashMap<(Locator, Locator), Sender<ZResult<NotificationNewSession>>>>
}

impl Session {
    fn new(manager: Arc<SessionManagerInner>, id: usize, peer: PeerId, 
        lease: ZInt, callback: Arc<dyn SessionCallback + Send + Sync>
    ) -> Self {
        Self {
            id,
            peer,
            transport: Arc::new(Transport::new(lease, callback)),
            manager,
            channel: RwLock::new(HashMap::new())
        }
    }

    fn initialize(&self, a_self: &Arc<Self>) {
        *self.transport.session.try_write().unwrap() = Some(a_self.clone());
        Transport::start(self.transport.clone());
    }

    async fn open(&self, manager: Arc<LinkManager>, locator: &Locator, 
        sender: Sender<ZResult<NotificationNewSession>>
    ) -> ZResult<()> {
        // Create a new link associated to self.session by calling the Link Manager
        let link = manager.new_link(locator, self.transport.clone()).await?;

        // Store the sender for the callback to be used in the process_message
        let key = (link.get_src(), link.get_dst());
        self.channel.write().await.insert(key, sender);

        // // Build the fields for the Open Message
        let version = self.manager.version;
        let whatami = self.manager.whatami.clone();
        let peer_id = self.manager.id.clone();
        let lease = self.manager.lease;
        let locators = self.manager.get_locators().await;
        let locators = match locators.len() {
            0 => None,
            _ => Some(locators)
        };
        // This is should always be None for Open Messages
        let conduit_id = None;
        // Parameter of open_session
        let properties = None;

        // Build the Open Message
        let message = Arc::new(Message::make_open(
            version, whatami, peer_id, lease, locators, conduit_id, properties
        ));

        // Schedule the message for transmission
        let priority = Some(0);                             // High priority
        let link = Some((link.get_src(), link.get_dst()));   // The link to reply on 
        self.transport.schedule(message, priority, link).await;

        Ok(())
    }

    async fn close(&self, reason: Option<ZError>) -> ZResult<()> {
        // PeerId
        let peer_id = Some(self.manager.id.clone());
        // Reason
        let reason_id = 0u8;
        // This is should always be None for Open Messages
        let conduit_id = None;
        // Parameter of open_session
        let properties = None;

        // Build the Close Message
        let message = Arc::new(Message::make_close(
            peer_id, reason_id, conduit_id, properties
        ));

        // Send the message for transmission
        let priority = Some(0); // High priority
        let link = None;        // The preferred link to reply on 
        self.transport.send(message, priority, link).await;
        // Close the session
        self.transport.close(reason).await
    }

    pub async fn send(&self, message: Arc<Message>) -> usize {
        self.transport.schedule(message, None, None).await
    }

    pub async fn get_links(&self) -> Vec<Link> {
        self.transport.get_links().await
    }

    /*************************************/
    /*              PROCESS              */
    /*************************************/
    pub(crate) async fn process_accept(&self, src: &Locator, dst: &Locator, 
        opid: &PeerId, apid: &PeerId, lease: &ZInt
    ) -> ZResult<()> {
        // Check if the opener peer of this accept was me
        if opid != &self.manager.id {
            return Err(zerror!(ZErrorKind::InvalidMessage{
                descr: format!("Received an Accept with an Opener Peer ID different from self!")
            }))
        }

        // Check if had previously triggered the opening of a new connection
        let key = (dst.clone(), src.clone());
        match self.channel.write().await.remove(&key) {
            Some(sender) => {
                let notification = NotificationNewSession::new(
                    apid.clone(), lease.clone(), src.clone(), dst.clone()
                );
                Ok(sender.send(Ok(notification)).await)
            },
            None => Err(zerror!(ZErrorKind::InvalidMessage{
                descr: format!("Received an Accept from a non pending connection!")
            }))
        }
    }

    pub(crate) async fn process_close(&self, _src: &Locator, _dst: &Locator,
        pid: &Option<PeerId>, _reason: &u8
    ) -> ZResult<()> {
        if pid != &Some(self.peer.clone()) {
            return Err(zerror!(ZErrorKind::InvalidMessage{
                descr: format!("Received a Close with a  Peer ID different from self!")
            }))
        } 
        self.manager.del_session(&self.peer, None).await?;
        self.transport.close(None).await
    }

    pub(crate) async fn process_open(&self, src: &Locator, dst: &Locator, 
        version: &u8, _whatami: &WhatAmI, pid: &PeerId, lease: &ZInt, _locators: &Option<Vec<Locator>> 
    ) -> ZResult<()> { // Ignore whatami and locators for the time being
        // Check if the version is supported
        if version > &self.manager.version {
            return Err(zerror!(ZErrorKind::Other{
                descr: format!("Zenoh version not supported ({}).", version)
            }))
        }

        // Check if an already established session exists with the peer
        let target = self.manager.get_or_new_session(&self.manager, pid).await?;

        // Move the transport link to the transport of the target session
        self.manager.move_link(dst, src, &target.transport).await?;

        // Set the lease to the transport
        target.transport.set_lease(*lease);

        // Build Accept message
        let conduit_id = None;  // Conduit ID always None
        let properties = None; // Properties always None for the time being. May change in the future.
        let message = Arc::new(Message::make_accept(
            pid.clone(), self.manager.id.clone(), self.manager.lease, conduit_id, properties
        ));

        // Schedule the message for transmission
        let priority = Some(0);                         // High priority
        let link = Some((dst.clone(), src.clone()));    // The link to reply on 
        target.transport.schedule(message, priority, link).await;

        Ok(())
    }
}

impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Session (Id {:?}, {:?}), {:?}", self.id, self.peer, self.transport)
    }
}

// impl Drop for Session {
//     fn drop(&mut self) {
//         println!("> Dropping Session ({:?})", self.peer);
//     }
// }