use async_std::sync::{
    Arc,
    channel,
    RwLock,
    Sender
};
use std::collections::HashMap;

use crate::zerror;
use crate::core::{
    PeerId,
    ZError,
    ZErrorKind,
    ZInt,
    ZResult
};
use crate::proto::{
    Body,
    Locator,
    LocatorProtocol,
    Message,
    WhatAmI
};
use crate::session::{
    ArcSelf,
    EmptyCallback,
    LinkManager,
    Session,
    SessionCallback,
    SessionHandler
};
use crate::session::link::*;


struct IDManager {
    empty: Vec<usize>,
}

impl IDManager {
    fn new() -> Self {
        let mut v = Vec::new();
        // The first available ID is 0
        v.push(0);
        return Self {
            empty: v
        }
    }

    pub fn new_id(&mut self) -> usize {
        // Get the first available ID
        let id = self.empty.remove(0);
        if self.empty.len() == 0 {
            self.empty.insert(0, id + 1);
        }
        return id
    }

    pub fn del_id(&mut self, id: usize) {
        for i in 0..self.empty.len() {
            if self.empty[i] > id {
                self.empty.insert(i, id);
                return
            }
        }
        self.empty.push(id);
    }
}


pub struct SessionManager {
    arc: ArcSelf<Self>,
    version: u8,
    whatami: WhatAmI,
    id: PeerId,
    lease: ZInt,
    handler: Arc<dyn SessionHandler + Send + Sync>,
    manager: RwLock<HashMap<LocatorProtocol, Arc<dyn LinkManager + Send + Sync>>>,
    session: RwLock<HashMap<usize, Arc<Session>>>,
    sid_mgmt: RwLock<IDManager>,
    channel: RwLock<HashMap<usize, Sender<Result<Arc<Session>, ZError>>>>
}

impl SessionManager {
    pub fn new(version: u8, whatami: WhatAmI, id: PeerId, lease: ZInt, 
        handler: Arc<dyn SessionHandler + Send + Sync>
    ) -> Arc<Self> {
        let m = Arc::new(Self {
            arc: ArcSelf::new(),
            version: version,
            whatami: whatami,
            id: id,
            lease: lease,
            handler: handler,
            manager: RwLock::new(HashMap::new()),
            session: RwLock::new(HashMap::new()),
            sid_mgmt: RwLock::new(IDManager::new()),
            channel: RwLock::new(HashMap::new())
        });
        m.arc.set(&m);
        return m
    }

    /*************************************/
    /*              LISTENER             */
    /*************************************/
    pub async fn new_listener(&self, locator: &Locator, _limit: Option<usize>) -> ZResult<()> {
        // Automatically create a new link manager for the protocol if it does not exist
        self.new_manager(&locator.get_proto()).await?;
        match self.manager.read().await.get(&locator.get_proto()) {
            Some(manager) => return manager.new_listener(locator).await,
            None => return Err(zerror!(ZErrorKind::Other{
                descr: format!("Trying to add a listener to a Link Manager that does not exist!")
            }))
        }
    }

    pub async fn del_listener(&self, locator: &Locator) -> ZResult<()> {
        match self.manager.read().await.get(&locator.get_proto()) {
            Some(manager) => {
                manager.del_listener(locator).await?;
                if manager.get_listeners().await.len() == 0 {
                    self.del_manager(&locator.get_proto()).await?;
                }
                return Ok(())
            },
            None => return Err(zerror!(ZErrorKind::Other{
                descr: format!("Trying to delete a listener from a Link Manager that does not exist!")
            }))
        }
    }

    /*************************************/
    /*            LINK MANAGER           */
    /*************************************/
    async fn new_manager(&self, protocol: &LocatorProtocol) -> ZResult<()> {
        let r_guard = self.manager.read().await;
        match protocol {
            LocatorProtocol::Tcp => if !r_guard.contains_key(&LocatorProtocol::Tcp) {
                drop(r_guard);
                let m = ManagerTcp::new(self.arc.get());
                self.manager.write().await.insert(LocatorProtocol::Tcp, m);
            },
            LocatorProtocol::Udp => if !r_guard.contains_key(&LocatorProtocol::Udp) {
                drop(r_guard);
                // let m = ManagerUdp::new();
                // m.set_arc_self(&m);
                // self.manager.write().await.insert(LocatorProtocol::Udp, m);
            }
        }
        Ok(())
    }

    async fn del_manager(&self, protocol: &LocatorProtocol) -> ZResult<()> {
        match self.manager.write().await.remove(protocol) {
            Some(_) => Ok(()),
            None => Err(zerror!(ZErrorKind::Other{
                descr: format!("No available Link Manager for protocol: {}", protocol)
            }))
        }
    }

    async fn move_link(&self, src: &Locator, dst: &Locator, session: &Arc<Session>) -> ZResult<()> {
        match self.manager.read().await.get(&src.get_proto()) {
            Some(manager) => return manager.move_link(src, dst, session.clone()).await,
            None => return Err(zerror!(ZErrorKind::Other{
                descr: format!("Trying to move a Link not associated to any manager!")
            }))
        }
    }

    /*************************************/
    /*              SESSION              */
    /*************************************/
    async fn add_session(&self, session: Arc<Session>) -> Option<Arc<Session>> {
        let res = self.session.write().await.insert(session.get_id(), session.clone());
        // session.set_callback(self.callback.clone()).await;
        return res
    }

    pub async fn del_session(&self, id: usize, _reason: Option<ZError>) -> Result<Arc<Session>, ZError> {
        // If it is pending, check if the open_session created a channel that needs to be notified
        // if let Some(channel) = self.channel.write().await.get(&id) {
        //     println!("{:?} DEL 1", self.whatami);
        //     channel.send(Err(reason.unwrap())).await;
        // }
        // Check if it is stored in the established sessions
        if let Some(session) = self.session.write().await.remove(&id) {
            self.sid_mgmt.write().await.del_id(session.get_id());
            return Ok(session)
        }

        return Err(zerror!(ZErrorKind::Other{
            descr: format!("Trying to delete a session that does not exist!")
        }))
    }

    pub async fn new_session(&self, peer: Option<PeerId>, 
        callback: Arc<dyn SessionCallback + Send + Sync>
    ) -> Arc<Session> {
        // Generate random session ID
        let id = self.sid_mgmt.write().await.new_id();
        // Create and initialize a new session
        let session = Session::new(id, peer, self.lease, self.arc.get(), callback);
        // Store the new session
        self.session.write().await.insert(session.get_id(), session.clone());
        return session
    }

    pub async fn open_session(&self, locator: &Locator) -> Result<Arc<Session>, ZError> {
        // Automatically create a new link manager for the protocol if it does not exist
        self.new_manager(&locator.get_proto()).await?;

        // Acquire a read lock on the managers
        let guard = self.manager.read().await;
        let manager = match guard.get(&locator.get_proto()) {
            Some(manager) => manager,
            None => return Err(zerror!(ZErrorKind::Other{
                descr: format!("Trying to add a link to a Link Manager that does not exist!")
            }))
        };

        // Create a new empty session
        let peer = None;
        let callback = Arc::new(EmptyCallback::new());
        let session = self.new_session(peer, callback).await;

        // Create a new link calling the Link Manager
        let link = manager.new_link(locator, session.clone()).await?;

        // Drop the read guard to not keep the lock while waiting for the channel
        drop(guard);

        // Create a channel for knowing when a session is open
        let (sender, receiver) = channel::<Result<Arc<Session>, ZError>>(1);
        self.channel.write().await.insert(session.get_id(), sender);

        // Build the fields for the Open Message
        let version = self.version;
        let whatami = self.whatami.clone();
        let peer_id = self.id.clone();
        let lease = self.lease;
        let locators = {
            let mut l: Vec<Locator> = Vec::new();
            for m in self.manager.read().await.values() {
                l.extend_from_slice(&m.get_listeners().await);
            }
            match l.len() {
                0 => None,
                _ => Some(l)
            }
        };
        // This is should always be None for Open Messages
        let conduit_id = None;
        // Parameter of open_session
        let properties = None;

        // Build and send the Open Message
        let message = Arc::new(Message::make_open(
            version, whatami, peer_id, lease, locators, conduit_id, properties
        ));
        link.send(message.clone()).await?;

        // Wait the accept message to finalize the session
        match receiver.recv().await {
            Some(session) => match session {
                Ok(s) => return Ok(s),
                Err(e) => return Err(e)
            },
            None => return Err(zerror!(ZErrorKind::Other{
                descr: format!("Open session failed unexpectedly!")
            }))
        }
    }

    pub async fn close_session(&self, id: usize, reason: Option<ZError>) -> Result<(), ZError> {   
        let session = self.del_session(id, None).await?;
        return session.close(reason).await;
    }

    /*************************************/
    /*              PROCESS              */
    /*************************************/
    pub async fn process_message(&self, session: Arc<Session>, src: &Locator, dst: &Locator, message: Message) -> Result<(), ZError> {
        // Process the message
        match &message.body {
            Body::Accept{ opid, apid, lease } => {
                if *opid != self.id {
                    return Err(zerror!(ZErrorKind::InvalidMessage{
                        descr: format!("Received an Accept with an Opener Peer ID different from self!")
                    }))
                }
                match self.channel.write().await.remove(&session.get_id()) {
                    Some(sender) => {
                        // Check if an already established session exists with the peer
                        let peer = Some(apid.clone());
                        let mut sex = None;
                        for s in self.session.read().await.values() {
                            if s.get_peer() == peer {
                                sex = Some(s.clone());
                                break
                            }
                        }
                        let target = match sex {
                            Some(s) => s,
                            None => {
                                let callback = self.handler.new_session(apid).await;
                                self.new_session(peer, callback).await
                            }
                        };

                        // Move the link to the target session
                        self.move_link(dst, src, &target).await?;

                        // Delete the old session
                        self.del_session(session.get_id(), None).await?;

                        // Set the lease on the session
                        target.set_lease(*lease);

                        // Notify the waiting channel in open_session
                        sender.send(Ok(target)).await;
                    },
                    None => return Err(zerror!(ZErrorKind::InvalidMessage{
                        descr: format!("Received an Accept from a non-open session!")
                    }))
                }
            },
            // Body::Close{ pid, reason } => {},
            Body::Close{..} => {},
            Body::Open{ version, whatami: _, pid, lease, locators: _ } => {
                // Ignore whatami and locators for the time being
                if version > &self.version {
                    return Err(zerror!(ZErrorKind::Other{
                        descr: format!("Zenoh version not supported ({}). Supported version is ({})!", version, self.version)
                    }))
                }
                // Build the fields of the Accept
                // Conduit ID always None
                let conduit_id = None;
                // Properties always None for the time being. May change in the future.
                let properties = None; 
                let message = Arc::new(Message::make_accept(
                    pid.clone(), self.id.clone(), self.lease, conduit_id, properties
                ));
                // Create the new session
                let callback = self.handler.new_session(pid).await;
                let target = self.new_session(Some(pid.clone()), callback).await;
                self.move_link(dst, src, &target).await?;
                target.set_lease(*lease);
                target.schedule(message).await;
                // Add the session to the established sessions
                self.add_session(target).await;
            },
            _ => return Err(zerror!(ZErrorKind::InvalidMessage{
                descr: format!("Message not allowed in the session manager: {:?}", message)
            }))
        }
        Ok(())
    }
}

impl Drop for SessionManager {
    fn drop(&mut self) {
        println!("Dropping SM");
    }
}