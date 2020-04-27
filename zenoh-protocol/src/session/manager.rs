
use async_std::prelude::*;
use async_std::sync::{channel, Arc, RwLock, Sender, Weak};
use async_std::task;
use async_trait::async_trait;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use crate::core::{PeerId, ZError, ZErrorKind, ZInt, ZResult};
use crate::link::{Link, LinkManager, Locator, LocatorProtocol};
use crate::proto::{Message, WhatAmI, close_reason};
use crate::session::defaults::{
    QUEUE_PRIO_CTRL, QUEUE_PRIO_DATA, SESSION_BATCH_SIZE, SESSION_LEASE, 
    SESSION_OPEN_TIMEOUT, SESSION_OPEN_RETRIES, SESSION_SEQ_NUM_RESOLUTION
};
use crate::session::{MsgHandler, Action, SessionHandler, Transport};
use crate::zerror;
use zenoh_util::{zasyncread, zasyncwrite};

// Macro to access the session Weak pointer
macro_rules! zsession {
    ($var:expr) => (
        if let Some(inner) = $var.upgrade() { 
            inner
        } else { 
            return Err(zerror!(ZErrorKind::InvalidSession{
                descr: "Session has been closed".to_string()
            }))
        }
    );
}

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
/// use zenoh_protocol::session::{DummyHandler, MsgHandler, SessionHandler, SessionManager, SessionManagerConfig, SessionManagerOptionalConfig};
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
///     handler: Arc::new(MySH::new())
/// };
/// let manager = SessionManager::new(config, None);
/// 
/// // Create the SessionManager with optional configuration
/// let config = SessionManagerConfig {
///     version: 0,
///     whatami: WhatAmI::Peer,
///     id: PeerId{id: vec![3, 4]},
///     handler: Arc::new(MySH::new())
/// };
/// // Setting a value to None indicates to use the default value
/// let opt_config = SessionManagerOptionalConfig {
///     lease: Some(1_000),     // Set the default lease to 1s
///     resolution: None,       // Use the default sequence number resolution
///     batchsize: None,        // Use the default batch size
///     timeout: Some(10_0000), // Timeout of 10s when opening a session
///     retries: Some(3),       // Tries to open a session 3 times before failure
///     max_sessions: Some(5),  // Accept any number of sessions
///     max_links: None         // Allow any number of links in a single session
/// };
/// let manager_opt = SessionManager::new(config, Some(opt_config));
/// ```

#[derive(Clone)]
pub struct SessionManager(Arc<SessionManagerInner>);

pub struct SessionManagerConfig {
    pub version: u8,
    pub whatami: WhatAmI,
    pub id: PeerId,
    pub handler: Arc<dyn SessionHandler + Send + Sync>
}

pub struct SessionManagerOptionalConfig {
    pub lease: Option<ZInt>,
    pub resolution: Option<ZInt>,
    pub batchsize: Option<usize>,
    pub timeout: Option<u64>,
    pub retries: Option<usize>,
    pub max_sessions: Option<usize>,
    pub max_links: Option<usize>
}

impl SessionManager {
    pub fn new(config: SessionManagerConfig, opt_config: Option<SessionManagerOptionalConfig>) -> SessionManager {
        // Set default optional values
        let mut lease = *SESSION_LEASE;
        let mut resolution = *SESSION_SEQ_NUM_RESOLUTION;
        let mut batchsize = *SESSION_BATCH_SIZE;
        let mut timeout = *SESSION_OPEN_TIMEOUT;
        let mut retries = *SESSION_OPEN_RETRIES;
        let mut max_sessions = None;
        let mut max_links = None;

        // Override default values if provided
        if let Some(opt) = opt_config {
            if let Some(v) = opt.lease {
                lease = v;
            }
            if let Some(v) = opt.resolution {
                resolution = v;
            }
            if let Some(v) = opt.batchsize {
                batchsize = v;
            }
            if let Some(v) = opt.timeout {
                timeout = v;
            }
            if let Some(v) = opt.retries {
                retries = v;
            }
            max_sessions = opt.max_sessions;
            max_links = opt.max_links;
        }

        let inner_config = SessionManagerInnerConfig {
            version: config.version,
            whatami: config.whatami.clone(),
            id: config.id.clone(),
            handler: config.handler,
            lease,
            resolution,
            batchsize,
            timeout,
            retries, 
            max_sessions,
            max_links
        };
        // Create the inner session manager
        let manager_inner = Arc::new(SessionManagerInner::new(inner_config));

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
            true
        ));

        // Initiliaze the transport
        session_inner.transport.init_session(Arc::downgrade(&session_inner));
        // Initiliaze the callback
        session_inner.transport.init_callback(callback);
        // Add the session to the inner session manager
        manager_inner.init_initial_session(session_inner);

        SessionManager(manager_inner)
    }

    /*************************************/
    /*              SESSION              */
    /*************************************/
    pub async fn open_session(&self, locator: &Locator) -> ZResult<Session> {
        // Retrieve the initial session
        let initial = self.0.get_initial_session().await;
        // Create the timeout duration
        let to = Duration::from_millis(self.0.config.timeout);

        // Try a maximum number of times to open a session
        for _ in 0..self.0.config.retries {
            // Automatically create a new link manager for the protocol if it does not exist
            let manager = self.0.get_or_new_link_manager(&self.0, &locator.get_proto()).await;
            // Create a channel for knowing when a session is open
            let (sender, receiver) = channel::<ZResult<Weak<SessionInner>>>(1);

            // Create the open future
            let open_fut = initial.open(manager, locator, sender).timeout(to);
            let channel_fut = receiver.recv().timeout(to);

            // Check the future resul
            match open_fut.try_join(channel_fut).await {
                // Future timeout result
                Ok((_, channel_res)) => match channel_res {
                    // Channel result
                    Some(res) => match res {
                        Ok(session_inner) => return Ok(Session::new(session_inner)),
                        Err(e) => return Err(zerror!(ZErrorKind::Other {
                            descr: format!("Open session error: {}", e)
                        }))
                    },
                    None => return Err(zerror!(ZErrorKind::Other {
                        descr: "Open session failed unexpectedly!".to_string()
                    }))
                },
                Err(_) => continue
            }
        }

        Err(zerror!(ZErrorKind::Other {
            descr: "Open session: maximum number of retries reached".to_string()
        }))
    }

    pub async fn get_sessions(&self) -> Vec<Session> {
        self.0.get_sessions().await
            .drain(..).map(Session::new).collect()
    }

    /*************************************/
    /*              LISTENER             */
    /*************************************/
    pub async fn add_locator(&self, locator: &Locator) -> ZResult<()> {
        let manager = self.0.get_or_new_link_manager(&self.0, &locator.get_proto()).await;
        manager.new_listener(locator).await
    }

    pub async fn del_locator(&self, locator: &Locator) -> ZResult<()> {
        let manager = self.0.get_link_manager(&locator.get_proto()).await?;
        manager.del_listener(locator).await?;
        if manager.get_listeners().await.is_empty() {
            self.0.del_link_manager(&locator.get_proto()).await?;
        }
        Ok(())
    }

    pub async fn get_locators(&self) -> Vec<Locator> {
        self.0.get_locators().await
    }
}

impl Drop for SessionManager {
    fn drop(&mut self) {
        task::block_on(async {
            let mut sessions = self.get_sessions().await;
            for s in sessions.drain(..) {
                let _ = s.close().await;
            }
        });
    }
}


struct SessionManagerInnerConfig {
    version: u8,
    whatami: WhatAmI,
    id: PeerId,
    handler: Arc<dyn SessionHandler + Send + Sync>,
    lease: ZInt,
    resolution: ZInt,
    batchsize: usize,
    timeout: u64,
    retries: usize,
    max_sessions: Option<usize>,
    max_links: Option<usize>
}

pub struct SessionManagerInner {
    config: SessionManagerInnerConfig,
    initial: RwLock<Option<Arc<SessionInner>>>,
    protocols: RwLock<HashMap<LocatorProtocol, Arc<LinkManager>>>,
    sessions: RwLock<HashMap<PeerId, Arc<SessionInner>>>,
}

impl SessionManagerInner {
    fn new(config: SessionManagerInnerConfig) -> SessionManagerInner {
        SessionManagerInner {
            config,
            initial: RwLock::new(None),
            protocols: RwLock::new(HashMap::new()),
            sessions: RwLock::new(HashMap::new()),
        }
    }

    /*************************************/
    /*          INITIALIZATION           */
    /*************************************/
    fn init_initial_session(&self, session: Arc<SessionInner>) {
        *self.initial.try_write().unwrap() = Some(session);
    }

    /*************************************/
    /*            LINK MANAGER           */
    /*************************************/
    async fn get_or_new_link_manager(
        &self,
        a_self: &Arc<Self>,
        protocol: &LocatorProtocol,
    ) -> Arc<LinkManager> {
        loop {
            match self.get_link_manager(protocol).await {
                Ok(manager) => return manager,
                Err(_) => match self.new_link_manager(a_self, protocol).await {
                    Ok(manager) => return manager,
                    Err(_) => continue
                }
            }
        }
    }

    async fn new_link_manager(
        &self,
        a_self: &Arc<Self>,
        protocol: &LocatorProtocol,
    ) -> ZResult<Arc<LinkManager>> {
        let mut w_guard = zasyncwrite!(self.protocols);
        if w_guard.contains_key(protocol) {
            return Err(zerror!(ZErrorKind::Other {
                descr: format!("Link manager for protocol ({}) already exists.", protocol)
            }));
        }

        let lm = Arc::new(LinkManager::new(a_self.clone(), protocol));
        w_guard.insert(protocol.clone(), lm.clone());
        Ok(lm)
    }

    async fn get_link_manager(&self, protocol: &LocatorProtocol) -> ZResult<Arc<LinkManager>> {
        match zasyncread!(self.protocols).get(protocol) {
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
                descr: format!("No available Link Manager for protocol ({})", protocol)
            })),
        }
    }

    async fn get_locators(&self) -> Vec<Locator> {
        let mut vec: Vec<Locator> = Vec::new();
        for p in zasyncread!(self.protocols).values() {
            vec.extend_from_slice(&p.get_listeners().await);
        }
        vec
    }

    /*************************************/
    /*              SESSION              */
    /*************************************/
    pub(crate) async fn get_initial_session(&self) -> Arc<SessionInner> {
        zasyncread!(self.initial).as_ref().unwrap().clone()
    }

    async fn get_or_new_session(
        &self,
        a_self: &Arc<Self>,
        peer: &PeerId,
        whatami: &WhatAmI,
    ) -> Weak<SessionInner> {
        loop {
            match self.get_session(peer).await {
                Ok(session) => return session,
                Err(_) => match self.new_session(a_self, peer, whatami).await {
                    Ok(session) => return session,
                    Err(_) => continue
                }
            }
        }
    }
    async fn del_session(&self, peer: &PeerId) -> ZResult<()> {
        match zasyncwrite!(self.sessions).remove(peer) {
            Some(_) => Ok(()),
            None => Err(zerror!(ZErrorKind::Other {
                descr: format!("Session not found for peer ({:?})", peer)
            })),
        }
    }

    async fn get_session(&self, peer: &PeerId) -> ZResult<Weak<SessionInner>> {
        match zasyncread!(self.sessions).get(peer) {
            Some(session) => Ok(Arc::downgrade(session)),
            None => Err(zerror!(ZErrorKind::Other {
                descr: format!("Session not found for peer ({:?})", peer)
            }))
        }
    }

    async fn get_sessions(&self) -> Vec<Weak<SessionInner>> {
        zasyncread!(self.sessions).values().map(|x| Arc::downgrade(&x)).collect()
    }

    async fn new_session(
        &self,
        a_self: &Arc<Self>,
        peer: &PeerId,
        whatami: &WhatAmI,
    ) -> ZResult<Weak<SessionInner>> {
        let mut w_guard = zasyncwrite!(self.sessions);
        if w_guard.contains_key(peer) {
            return Err(zerror!(ZErrorKind::Other {
                descr: format!("Session with peer ({:?}) already exists", peer)
            }));
        }

        // Create the session object
        let session_inner = Arc::new(SessionInner::new(
            a_self.clone(),
            peer.clone(),
            whatami.clone(),
            self.config.lease,
            self.config.resolution,
            self.config.batchsize,
            false
        ));

        // Create a weak reference to the session
        let weak = Arc::downgrade(&session_inner);
        // Set the session on the transport
        session_inner.transport.init_session(weak.clone());
        // Add the session to the list of active sessions
        w_guard.insert(peer.clone(), session_inner);

        Ok(weak)
    }
}

/*************************************/
/*              SESSION              */
/*************************************/

/// [`Session`] is the session handler returned when opening a new session
#[derive(Clone)]
pub struct Session(Weak<SessionInner>);

impl Session {
    fn new(inner: Weak<SessionInner>) -> Self {
        Self(inner)
    }

    pub fn get_peer(&self) -> ZResult<PeerId> {
        let session = zsession!(self.0);
        Ok(session.peer.clone())
    }

    pub async fn close(&self) -> ZResult<()> {
        let session = zsession!(self.0);
        let to = Duration::from_millis(session.manager.config.timeout);
        match session.close().timeout(to).await {
            Ok(res) => res,
            Err(_) => Err(zerror!(ZErrorKind::Other {
                descr: "Close operation has timed out".to_string()
            }))
        }
    }

    pub async fn get_links(&self) -> ZResult<Vec<Link>> {
        let session = zsession!(self.0);
        Ok(session.transport.get_links().await)
    }

    pub async fn schedule(&self, message: Message, link: Option<Link>) -> ZResult<()> {
        let session = zsession!(self.0);
        session.transport.schedule(message, *QUEUE_PRIO_DATA, link).await;
        Ok(())
    }

    pub async fn schedule_batch(&self, messages: Vec<Message>, link: Option<Link>, cid: Option<ZInt>) -> ZResult<()> {
        let session = zsession!(self.0);
        session.transport.schedule_batch(messages, *QUEUE_PRIO_DATA, link, cid).await;
        task::yield_now().await;
        Ok(())
    }
}

#[async_trait]
impl MsgHandler for Session {
    #[inline]
    async fn handle_message(&self, message: Message) -> ZResult<()> {
        self.schedule(message, None).await
    }

    async fn close(&self) {}
}

impl Eq for Session {}

impl PartialEq for Session {
    fn eq(&self, other: &Self) -> bool {
        Weak::ptr_eq(&self.0, &other.0)
    }
}


impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(session) = self.0.upgrade() {
            write!(f, "Session ({:?})", session.peer)
        } else {
            write!(f, "Session closed")
        }
    }
}


#[allow(clippy::type_complexity)]
pub(crate) struct SessionInner {
    pub(crate) manager: Arc<SessionManagerInner>,
    pub(crate) peer: PeerId,
    pub(crate) whatami: WhatAmI,
    pub(crate) transport: Arc<Transport>,
    is_initial: bool,
    channels: RwLock<HashMap<(Locator, Locator), Sender<ZResult<Weak<SessionInner>>>>>,
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
            manager,
            peer,
            whatami,
            transport: Arc::new(Transport::new(lease, resolution, batchsize)),
            is_initial,
            channels: RwLock::new(HashMap::new()),
        }
    }

    /*************************************/
    /*               LINK                */
    /*************************************/
    pub(crate) async fn add_link(&self, link: Link) -> ZResult<()> {
        self.transport.add_link(link).await
    }

    pub(crate) async fn del_link(&self, link: &Link) -> ZResult<()> {
        // Delete the link from the transport if present
        self.transport.del_link(link).await?;
        // Close the link (e.g., TCP socket)
        let _ = link.close().await;

        // If an open was done on this link and not yet complete, notify the error 
        let key = (link.get_src(), link.get_dst());
        let res = zasyncwrite!(self.channels).remove(&key);
        if let Some(sender) = res {
            let res = Err(zerror!(ZErrorKind::Other{
                descr: format!("Link ({}) disappeared during an open!", link)
            }));
            sender.send(res).await;
        }

        if !self.is_initial && !self.transport.has_links() {
            // Remove the session from the manager
            let _ = self.manager.del_session(&self.peer).await;
            let _ = self.transport.close().await;
        }

        Ok(())
    }

    /*************************************/
    /*            OPEN/CLOSE             */
    /*************************************/
    async fn open(&self, manager: Arc<LinkManager>, locator: &Locator, sender: Sender<ZResult<Weak<SessionInner>>>) -> ZResult<()> {
        // Create a new link associated by calling the Link Manager
        let link = manager.new_link(locator, self.transport.clone()).await?;

        // Add the link to the transport
        self.transport.add_link(link.clone()).await?;

        // Store the sender for the callback to be used in the process_message
        let key = (link.get_src(), link.get_dst());
        zasyncwrite!(self.channels).insert(key, sender);

        // Build the fields for the Open Message
        let version = self.manager.config.version;
        let whatami = self.manager.config.whatami.clone();
        let peer_id = self.manager.config.id.clone();
        let lease = self.manager.config.lease;
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
        // Close the active transport
        self.close_inner().await?;
        // Remove the session from the manager
        self.manager.del_session(&self.peer).await
    }

    async fn close_inner(&self) -> ZResult<()> {
        // Send a close message
        let peer_id = Some(self.manager.config.id.clone());
        let reason_id = close_reason::GENERIC;              
        let conduit_id = None;  // This is should always be None for Close Messages                
        let properties = None;  // Parameter of open_session
        let message = Message::make_close(peer_id, reason_id, conduit_id, properties);

        // Get the transport links
        let links = self.transport.get_links().await;
        let futs: FuturesUnordered<_> = links.iter().map(|l| 
            self.transport.send(message.clone(), *QUEUE_PRIO_DATA, Some(l.clone()))
        ).collect();
        let _ = futs.into_future().await;

        // Close the transport
        let _ = self.transport.close().await;

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
        lease: ZInt
    ) -> Action {
        // Check if the opener peer of this accept was me
        if opid != &self.manager.config.id {
            println!("!!! Received an Accept with wrong Opener Peer Id");
            return Action::Read
        }

        // Check if had previously triggered the opening of a new connection
        let key = (link.get_src(), link.get_dst());
        let res = zasyncwrite!(self.channels).remove(&key);
        if let Some(sender) = res {
            // Remove the link from self
            let res = self.transport.del_link(link).await;
            if res.is_err() {
                return Action::Read
            }
            // Get a new or an existing session
            let weak_session = self.manager.get_or_new_session(&self.manager, apid, whatami).await;
            // Upgrade from Weak to Arc
            let arc_session = if let Some(session) = weak_session.upgrade() {
                session
            } else {
                // The session has been closed
                return Action::Close
            };
            // Configure the lease time on the transport
            arc_session.transport.set_lease(lease);
            // Add the link on the transport
            let res = arc_session.transport.add_link(link.clone()).await;
            if res.is_err() {
                return Action::Read
            }
            // Set the callback to the transport if needed
            if !arc_session.transport.has_callback() {
                // Notify the session handler that there is a new session and get back a callback
                let callback = self.manager.config.handler
                    .new_session(self.whatami.clone(), 
                    Arc::new(Session::new(Arc::downgrade(&arc_session)))
                ).await;
                // Set the callback on the transport
                arc_session.transport.set_callback(callback).await;
            }
            // Notify the opener
            sender.send(Ok(weak_session)).await;
            // Return the target transport to use in the link
            Action::ChangeTransport(arc_session.transport.clone())
        } else { 
            println!("!!! Received an unsolicited Accept because no Open message was sent");
            Action::Read
        }
    }

    pub(crate) async fn process_close(&self, link: &Link, pid: &Option<PeerId>, _reason: u8) -> Action {
        // Check if the close target is me
        if !self.is_initial && pid != &Some(self.peer.clone()) {
            println!("!!! PeerId mismatch on Close message");
            return Action::Read
        }

        // Delete the link
        let _ = self.del_link(link).await;

        Action::Close
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
    ) -> Action {
        // @TODO: Manage locators

        // Check if the version is supported
        if version > self.manager.config.version {
            // Send a close message
            let peer_id = Some(self.manager.config.id.clone());
            let reason_id = close_reason::UNSUPPORTED;              
            let conduit_id = None;  // This is should always be None for Close Messages                
            let properties = None;  // Parameter of open_session
            let message = Message::make_close(peer_id, reason_id, conduit_id, properties);

            // Send the close message for this link
            let _ = self.transport.send(message, *QUEUE_PRIO_DATA, Some(link.clone())).await;

            // Close the link
            return Action::Close
        }

        // Check if an already established session exists with the peer
        let target = self.manager.get_session(pid).await;

        // Check if this open is related to a totally new session (i.e. new peer)
        if target.is_err() {
            // Check if a limit for the maximum number of open sessions is set
            if let Some(limit) = self.manager.config.max_sessions {
                let num = self.manager.get_sessions().await.len();
                // Check if we have reached the session limit
                if num >= limit {
                    // Send a close message
                    let peer_id = Some(self.manager.config.id.clone());
                    let reason_id = close_reason::MAX_SESSIONS;                
                    let conduit_id = None;  // This is should always be None for Close Messages                
                    let properties = None;  // Parameter of open_session
                    let message = Message::make_close(peer_id, reason_id, conduit_id, properties);

                    // Send the close message for this link
                    let _ = self.transport.send(message, *QUEUE_PRIO_DATA, Some(link.clone())).await;

                    // Close the link
                    return Action::Close
                }
            }
        }

        // Get the session associated to the peer
        let target = self.manager.get_or_new_session(&self.manager, pid, whatami).await;
        let target = if let Some(session) = target.upgrade() {
            session
        } else {
            return Action::Close
        };

        // Check if a limit for the maximum number of links associated to a session is set
        if let Some(limit) = self.manager.config.max_links {
            // Check if we have reached the session limit
            if target.transport.num_links() >= limit {
                // Send a close message
                let peer_id = Some(self.manager.config.id.clone());
                let reason_id = close_reason::MAX_LINKS;               
                let conduit_id = None;  // This is should always be None for Close Messages                
                let properties = None;  // Parameter of open_session
                let message = Message::make_close(peer_id, reason_id, conduit_id, properties);

                // Send the close message for this link
                let _ = self.transport.send(message, *QUEUE_PRIO_DATA, Some(link.clone())).await;

                // Close the link
                return Action::Close
            }
        }

        // Set the lease to the transport
        target.transport.set_lease(lease);

        // Remove the link from self
        let res = self.transport.del_link(&link).await;
        if res.is_err() {
            return Action::Close
        }
        // Add the link to the target
        let res = target.transport.add_link(link.clone()).await;
        if res.is_err() {
            return Action::Close
        }

        // Build Accept message
        let conduit_id = None; // Conduit ID always None
        let properties = None; // Properties always None for the time being. May change in the future.
        let message = Message::make_accept(
            self.manager.config.whatami.clone(),
            pid.clone(),
            self.manager.config.id.clone(),
            self.manager.config.lease,
            conduit_id,
            properties,
        );

        // Send the message for transmission
        let res = target.transport.send(message, *QUEUE_PRIO_CTRL, Some(link.clone())).await;

        if res.is_ok() {
            if !target.transport.has_callback() {
                // Notify the session handler that there is a new session and get back a callback
                // NOTE: the read loop of the link the open message was sent on remains blocked
                //       until the new_session() returns. The read_loop in the various links
                //       waits for any eventual transport to associate to. This is transport is
                //       returned only by the process_open() -- this function.
                let callback = self.manager.config.handler.new_session(
                    whatami.clone(), 
                    Arc::new(Session::new(Arc::downgrade(&target)))
                ).await;
                // Set the callback on the transport
                target.transport.set_callback(callback).await;
            }
        } else {
            return Action::Close
        }

        // Return the target transport to use in the link
        Action::ChangeTransport(target.transport.clone())
    }
}

impl fmt::Debug for SessionInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SessionInner ({:?})", self.peer)
    }
}