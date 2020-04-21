use async_std::future;
use async_std::sync::{channel, Arc, RwLock, Sender};
use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use crate::core::{PeerId, ZError, ZErrorKind, ZInt, ZResult};
use crate::link::{Link, LinkManager, Locator, LocatorProtocol};
use crate::proto::{Message, WhatAmI};
use crate::session::defaults::{
    QUEUE_PRIO_CTRL, QUEUE_PRIO_DATA, SESSION_BATCH_SIZE, SESSION_LEASE, SESSION_OPEN_TIMEOUT,
    SESSION_SEQ_NUM_RESOLUTION,
};
use crate::session::{MsgHandler, SessionHandler, Transport};
use crate::zerror;
use zenoh_util::{zasyncread, zasyncwrite, zrwopt};

// Define an empty SessionCallback for the initial session
struct InitialHandler;

impl InitialHandler {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl MsgHandler for InitialHandler {
    async fn handle_message(&self, message: Message) -> ZResult<()> {
        println!(
            "!!! WARNING: InitialHandler::handle_message({:?}) => dropped",
            message.body
        );
        Ok(())
    }

    async fn close(&self) {
        println!("!!! WARNING: InitialHandler::close()");
    }
}

/// # Example:
/// ```
/// use async_std::sync::Arc;
/// use async_trait::async_trait;
/// use zenoh_protocol::core::PeerId;
/// use zenoh_protocol::proto::WhatAmI;
/// use zenoh_protocol::session::{DummyHandler, MsgHandler, SessionHandler, SessionManager, SessionManagerConfig};
///
/// // Create my session handler to be notified when a new session is initiated with me
/// struct MySH;
///
/// impl MySH {
///     fn new() -> MySH {
///         MySH
///     }
/// }
///
/// #[async_trait]
/// impl SessionHandler for MySH {
///     async fn new_session(&self,
///         _wami: WhatAmI,
///         _session: Arc<dyn MsgHandler + Send + Sync>
///     ) -> Arc<dyn MsgHandler + Send + Sync> {
///         Arc::new(DummyHandler::new())
///     }
/// }
///
/// // Create the SessionManager
/// let config = SessionManagerConfig {
///     version: 0,
///     whatami: WhatAmI::Peer,
///     id: PeerId{id: vec![1, 2]},
///     handler: Arc::new(MySH::new()),
///     lease: None,        // Use the default lease
///     resolution: None,   // Use the default sequence number resolution
///     batchsize: None,    // Use the default batch size
///     timeout: None       // Use the default timeout when opening a session
/// };
/// let manager = SessionManager::new(config);
/// ```
#[derive(Clone)]
pub struct SessionManager {
    inner: Arc<SessionManagerInner>,
    timeout: u64,
}

pub struct SessionManagerConfig {
    pub version: u8,
    pub whatami: WhatAmI,
    pub id: PeerId,
    pub handler: Arc<dyn SessionHandler + Send + Sync>,
    pub lease: Option<ZInt>,
    pub resolution: Option<ZInt>,
    pub batchsize: Option<usize>,
    pub timeout: Option<u64>,
}

impl SessionManager {
    pub fn new(config: SessionManagerConfig) -> SessionManager {
        // Set default values if not provided
        let lease = config.lease.unwrap_or(*SESSION_LEASE);
        let resolution = config.resolution.unwrap_or(*SESSION_SEQ_NUM_RESOLUTION);
        let batchsize = config.batchsize.unwrap_or(*SESSION_BATCH_SIZE);
        let timeout = config.timeout.unwrap_or(*SESSION_OPEN_TIMEOUT);

        // Create the inner session manager
        let manager_inner = Arc::new(SessionManagerInner::new(
            config.version,
            config.whatami.clone(),
            config.id.clone(),
            config.handler,
            lease,
            resolution,
            batchsize,
        ));

        // Create a session used to establish new connections
        // This session wrapper does not require to contact the upper layer
        let callback = Arc::new(InitialHandler::new());
        let session_inner = Arc::new(SessionInner::new(
            manager_inner.clone(),
            config.id,
            config.whatami,
            lease,
            resolution,
            batchsize,
            true,
        ));

        // Initiliaze the transport
        session_inner.transport.set_session(session_inner.clone());
        // Initiliaze the callback
        session_inner.transport.set_callback(callback);
        // Add the session to the inner session manager
        manager_inner.set_intial_session(session_inner);

        SessionManager {
            inner: manager_inner,
            timeout,
        }
    }

    /*************************************/
    /*              SESSION              */
    /*************************************/
    pub async fn open_session(&self, locator: &Locator) -> ZResult<Session> {
        // Automatically create a new link manager for the protocol if it does not exist
        let manager = self
            .inner
            .new_link_manager(&self.inner, &locator.get_proto())
            .await?;

        // Create a channel for knowing when a session is open
        let (sender, receiver) = channel::<ZResult<Arc<SessionInner>>>(1);

        // Trigger the open session
        zrwopt!(self.inner.initial)
            .open(manager, locator, sender)
            .await?;

        // Wait the accept message to finalize the session
        let res = future::timeout(Duration::from_millis(self.timeout), receiver.recv()).await;
        if res.is_err() {
            return Err(zerror!(ZErrorKind::Other {
                descr: "Open session has timedout!".to_string()
            }));
        }

        let session_inner = match res.unwrap() {
            Some(res) => match res {
                Ok(session_inner) => session_inner,
                Err(e) => return Err(e),
            },
            None => {
                return Err(zerror!(ZErrorKind::Other {
                    descr: "Open session failed unexpectedly!".to_string()
                }))
            }
        };

        Ok(Session::new(session_inner))
    }

    pub async fn get_sessions(&self) -> Vec<Session> {
        let mut vec = Vec::new();
        for s in zasyncread!(self.inner.sessions).values() {
            vec.push(Session::new(s.clone()));
        }
        vec
    }

    /*************************************/
    /*              LISTENER             */
    /*************************************/
    pub async fn add_locator(&self, locator: &Locator, _limit: Option<usize>) -> ZResult<()> {
        let manager = self
            .inner
            .new_link_manager(&self.inner, &locator.get_proto())
            .await?;
        manager.new_listener(locator).await
    }

    pub async fn del_locator(&self, locator: &Locator) -> ZResult<()> {
        let manager = self.inner.get_link_manager(&locator.get_proto()).await?;
        manager.del_listener(locator).await?;
        if manager.get_listeners().await.is_empty() {
            self.inner.del_link_manager(&locator.get_proto()).await?;
        }
        Ok(())
    }

    pub async fn get_locators(&self) -> Vec<Locator> {
        self.inner.get_locators().await
    }
}

pub struct SessionManagerInner {
    version: u8,
    whatami: WhatAmI,
    id: PeerId,
    handler: Arc<dyn SessionHandler + Send + Sync>,
    lease: ZInt,
    resolution: ZInt,
    batchsize: usize,
    initial: RwLock<Option<Arc<SessionInner>>>,
    protocols: RwLock<HashMap<LocatorProtocol, Arc<LinkManager>>>,
    sessions: RwLock<HashMap<PeerId, Arc<SessionInner>>>,
}

impl SessionManagerInner {
    fn new(
        version: u8,
        whatami: WhatAmI,
        id: PeerId,
        handler: Arc<dyn SessionHandler + Send + Sync>,
        lease: ZInt,
        resolution: ZInt,
        batchsize: usize,
    ) -> SessionManagerInner {
        SessionManagerInner {
            version,
            whatami,
            id,
            handler,
            lease,
            resolution,
            batchsize,
            initial: RwLock::new(None),
            protocols: RwLock::new(HashMap::new()),
            sessions: RwLock::new(HashMap::new()),
        }
    }

    /*************************************/
    /*          INITIALIZATION           */
    /*************************************/
    fn set_intial_session(&self, session: Arc<SessionInner>) {
        *self.initial.try_write().unwrap() = Some(session);
    }

    /*************************************/
    /*            LINK MANAGER           */
    /*************************************/
    async fn new_link_manager(
        &self,
        a_self: &Arc<Self>,
        protocol: &LocatorProtocol,
    ) -> ZResult<Arc<LinkManager>> {
        match self.get_link_manager(protocol).await {
            Ok(manager) => Ok(manager),
            Err(_) => {
                let lm = Arc::new(LinkManager::new(a_self.clone(), protocol));
                zasyncwrite!(self.protocols).insert(protocol.clone(), lm.clone());
                Ok(lm)
            }
        }
    }

    async fn get_link_manager(&self, protocol: &LocatorProtocol) -> ZResult<Arc<LinkManager>> {
        match self.protocols.read().await.get(protocol) {
            Some(manager) => Ok(manager.clone()),
            None => Err(zerror!(ZErrorKind::Other {
                descr: format!("Link Manager not found for protocol ({})", protocol)
            })),
        }
    }

    async fn del_link_manager(&self, protocol: &LocatorProtocol) -> ZResult<()> {
        match zasyncwrite!(self.protocols).remove(protocol) {
            Some(_) => Ok(()),
            None => Err(zerror!(ZErrorKind::Other {
                descr: format!("No available Link Manager for protocol: {}", protocol)
            })),
        }
    }

    async fn get_locators(&self) -> Vec<Locator> {
        let mut vec: Vec<Locator> = Vec::new();
        for p in self.protocols.read().await.values() {
            vec.extend_from_slice(&p.get_listeners().await);
        }
        vec
    }

    /*************************************/
    /*              SESSION              */
    /*************************************/
    pub(crate) fn get_initial_session(&self) -> Arc<SessionInner> {
        zrwopt!(self.initial).clone()
    }

    async fn get_or_new_session(
        &self,
        a_self: &Arc<Self>,
        peer: &PeerId,
        whatami: &WhatAmI,
    ) -> Arc<SessionInner> {
        let r_guard = self.sessions.read().await;
        match r_guard.get(peer) {
            Some(wrapper) => wrapper.clone(),
            None => {
                drop(r_guard);
                self.new_session(a_self, peer, whatami).await.unwrap()
            }
        }
    }
    async fn del_session(&self, peer: &PeerId) -> ZResult<Arc<SessionInner>> {
        match zasyncwrite!(self.sessions).remove(peer) {
            Some(session) => Ok(session),
            None => Err(zerror!(ZErrorKind::Other {
                descr: "Trying to delete a session that does not exist!".to_string()
            })),
        }
    }

    async fn new_session(
        &self,
        a_self: &Arc<Self>,
        peer: &PeerId,
        whatami: &WhatAmI,
    ) -> ZResult<Arc<SessionInner>> {
        if zasyncread!(self.sessions).get(peer).is_some() {
            return Err(zerror!(ZErrorKind::Other {
                descr: format!("Session with peer ({:?}) already exists.", peer)
            }));
        }

        // Create the session object
        let session_inner = Arc::new(SessionInner::new(
            a_self.clone(),
            peer.clone(),
            whatami.clone(),
            self.lease,
            self.resolution,
            self.batchsize,
            false,
        ));
        // Add the session to the list of active sessions
        zasyncwrite!(self.sessions).insert(peer.clone(), session_inner.clone());
        // Set the session on the transport
        session_inner.transport.set_session(session_inner.clone());

        Ok(session_inner)
    }
}

/*************************************/
/*              SESSION              */
/*************************************/

/// [`Session`] is the session handler returned when opening a new session
#[derive(Clone)]
pub struct Session(Arc<SessionInner>);

impl Session {
    fn new(inner: Arc<SessionInner>) -> Self {
        Self(inner)
    }

    pub fn get_peer(&self) -> PeerId {
        self.0.peer.clone()
    }

    pub fn get_transport(&self) -> Arc<Transport> {
        self.0.transport.clone()
    }

    pub async fn close(&self) -> ZResult<()> {
        self.0.close().await
    }

    pub async fn get_links(&self) -> Vec<Link> {
        self.0.transport.get_links().await
    }

    pub async fn add_link(&self, link: Link) -> ZResult<()> {
        self.0.transport.add_link(link).await
    }

    pub async fn del_link(&self, link: &Link) -> ZResult<()> {
        self.0.transport.del_link(link).await
    }

    pub async fn schedule(&self, message: Message, link: Option<Link>) {
        self.0
            .transport
            .schedule(message, *QUEUE_PRIO_DATA, link)
            .await;
    }

    pub async fn schedule_batch(&self, messages: Vec<Message>, link: Option<Link>, cid: Option<ZInt>) {
        self.0
            .transport
            .schedule_batch(messages, *QUEUE_PRIO_DATA, link, cid)
            .await;
    }
}

impl Eq for Session {}

impl PartialEq for Session {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Session ({:?}), {:?})", self.0.peer, self.0.transport)
    }
}

#[allow(clippy::type_complexity)]
pub(crate) struct SessionInner {
    pub(crate) peer: PeerId,
    pub(crate) whatami: WhatAmI,
    pub(crate) transport: Arc<Transport>,
    pub(crate) manager: Arc<SessionManagerInner>,
    channels: RwLock<HashMap<(Locator, Locator), Sender<ZResult<Arc<SessionInner>>>>>,
}

#[async_trait]
impl MsgHandler for SessionInner {
    async fn handle_message(&self, message: Message) -> ZResult<()> {
        self.transport
            .schedule(message, *QUEUE_PRIO_DATA, None)
            .await;
        Ok(())
    }

    async fn close(&self) {}
}

impl SessionInner {
    fn new(
        manager: Arc<SessionManagerInner>,
        peer: PeerId,
        whatami: WhatAmI,
        lease: ZInt,
        resolution: ZInt,
        batchsize: usize,
        is_initial: bool,
    ) -> SessionInner {
        SessionInner {
            peer,
            whatami,
            // @TODO: make the sequence number resolution configurable
            transport: Arc::new(Transport::new(lease, resolution, batchsize, is_initial)),
            manager,
            channels: RwLock::new(HashMap::new()),
        }
    }

    async fn open(
        &self,
        manager: Arc<LinkManager>,
        locator: &Locator,
        sender: Sender<ZResult<Arc<SessionInner>>>,
    ) -> ZResult<()> {
        // Create a new link associated by calling the Link Manager
        let link = manager.new_link(locator, self.transport.clone()).await?;

        // Add the link to the transport
        self.transport.add_link(link.clone()).await?;

        // Store the sender for the callback to be used in the process_message
        let key = (link.get_src(), link.get_dst());
        self.channels.write().await.insert(key, sender);

        // // Build the fields for the Open Message
        let version = self.manager.version;
        let whatami = self.manager.whatami.clone();
        let peer_id = self.manager.id.clone();
        let lease = self.manager.lease;
        let locators = self.manager.get_locators().await;
        let locators = match locators.len() {
            0 => None,
            _ => Some(locators),
        };
        // This is should always be None for Open Messages
        let conduit_id = None;
        // Parameter of open_session
        let properties = None;

        // Build the Open Message
        let message = Message::make_open(
            version, whatami, peer_id, lease, locators, conduit_id, properties,
        );

        // Schedule the message for transmission
        self.transport.send(message, *QUEUE_PRIO_CTRL, Some(link)).await?;

        Ok(())
    }

    async fn close(&self) -> ZResult<()> {
        // PeerId
        let peer_id = Some(self.manager.id.clone());
        // Reason
        let reason_id = 0u8;
        // This is should always be None for Open Messages
        let conduit_id = None;
        // Parameter of open_session
        let properties = None;

        // Build the Close Message
        let message = Message::make_close(peer_id, reason_id, conduit_id, properties);

        // Get the transport links
        let links = self.transport.get_links().await;
        for l in links.iter() {
            self.transport
                .send(message.clone(), *QUEUE_PRIO_DATA, Some(l.clone()))
                .await?;
        }

        // Close the transport
        self.transport.close().await?;

        // Remove the session from the manager
        self.delete().await?;

        Ok(())
    }

    pub(crate) async fn delete(&self) -> ZResult<()> {
        // Remove the session from the manager
        self.manager.del_session(&self.peer).await?;

        Ok(())
    }

    /*************************************/
    /*          PROCESS MESSAGES         */
    /*************************************/
    pub(crate) async fn process_accept(
        &self,
        link: &Link,
        whatami: &WhatAmI,
        opid: &PeerId,
        apid: &PeerId,
        lease: ZInt,
    ) -> ZResult<Arc<Transport>> {
        // Check if the opener peer of this accept was me
        if opid != &self.manager.id {
            return Err(zerror!(ZErrorKind::Other {
                descr: "Received an Accept with wrong Opener Peer Id".to_string()
            }));
        }

        // Check if had previously triggered the opening of a new connection
        let key = (link.get_src(), link.get_dst());
        match self.channels.write().await.remove(&key) {
            Some(sender) => {
                // Remove the link from self
                self.transport.del_link(link).await?;
                // Get a new or an existing session
                let session_inner = self
                    .manager
                    .get_or_new_session(&self.manager, apid, whatami)
                    .await;
                // Configure the lease time on the transport
                session_inner.transport.set_lease(lease);
                // Add the link on the transport
                session_inner.transport.add_link(link.clone()).await?;
                // Set the callback to the transport if needed
                if session_inner.transport.get_callback().is_none() {
                    // Notify the session handler that there is a new session and get back a callback
                    let callback = self.manager.handler.new_session(self.whatami.clone(), session_inner.clone()).await;
                    // Set the callback on the transport
                    session_inner.transport.set_callback(callback);
                }
                // Notify the opener
                sender.send(Ok(session_inner.clone())).await;
                // Return the target transport to use in the link
                Ok(session_inner.transport.clone())
            }
            None => Err(zerror!(ZErrorKind::Other {
                descr: "Received an unsolicited Accept because no Open message was sent"
                    .to_string()
            })),
        }
    }

    pub(crate) async fn process_close(
        &self,
        link: &Link,
        pid: &Option<PeerId>,
        _reason: u8,
    ) {
        // Check if the close target is me
        if pid != &Some(self.peer.clone()) {
            return;
        }

        // Delete the link
        let _ = self.transport.del_link(link).await;
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn process_open(
        &self,
        link: &Link,
        version: u8,
        whatami: &WhatAmI,
        pid: &PeerId,
        lease: ZInt,
        _locators: &Option<Vec<Locator>>,
    ) -> ZResult<Arc<Transport>> {
        // @TODO: Manage locators

        // Check if the version is supported
        if version > self.manager.version {
            return Err(zerror!(ZErrorKind::Other {
                descr: format!("Zenoh version not supported ({})", version)
            }));
        }

        // Check if an already established session exists with the peer
        let target = self
            .manager
            .get_or_new_session(&self.manager, pid, whatami)
            .await;

        // Set the lease to the transport
        target.transport.set_lease(lease);

        // Remove the link from self
        self.transport.del_link(&link).await?;
        // Add the link to the target
        target.transport.add_link(link.clone()).await?;

        // Build Accept message
        let conduit_id = None; // Conduit ID always None
        let properties = None; // Properties always None for the time being. May change in the future.
        let message = Message::make_accept(
            self.manager.whatami.clone(),
            pid.clone(),
            self.manager.id.clone(),
            self.manager.lease,
            conduit_id,
            properties,
        );

        // Schedule the message for transmission
        let res = target.transport.send(message, *QUEUE_PRIO_CTRL, Some(link.clone())).await;

        if res.is_ok() && target.transport.get_callback().is_none() {
            // Notify the session handler that there is a new session and get back a callback
            // NOTE: the read loop of the link the open message was sent on reamins blocked
            //       until the new_session() returns. The read_loop in the various links
            //       waits for any eventual transport to associate to. This is transport is
            //       returned only by the process_ope() -- this function.
            let callback = self.manager.handler.new_session(whatami.clone(), target.clone()).await;
            // Set the callback on the transport
            target.transport.set_callback(callback);
        }

        Ok(target.transport.clone())
    }
}
