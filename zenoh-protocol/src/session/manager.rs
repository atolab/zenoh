use async_std::prelude::*;
use async_std::sync::{channel, Arc, RwLock, Weak};
use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use crate::core::{PeerId, ZError, ZErrorKind, ZInt, ZResult};
use crate::link::{Link, LinkManager, LinkManagerBuilder, Locator, LocatorProtocol};
use crate::proto::{Attachment, WhatAmI, ZenohMessage};
use crate::session::defaults::{
    SESSION_BATCH_SIZE, SESSION_LEASE, SESSION_OPEN_TIMEOUT,
    SESSION_OPEN_RETRIES, SESSION_SEQ_NUM_RESOLUTION
};
use crate::session::{Channel, InitialSession, MsgHandler, SessionHandler, Transport};
use crate::zerror;
use zenoh_util::{zasyncread, zasyncwrite};

// Macro to access the session Weak pointer
macro_rules! zchannel {
    ($var:expr) => (
        if let Some(inner) = $var.upgrade() { 
            inner
        } else {
            let e = "Session has been closed".to_string();
            log::trace!("{}", e);
            return Err(zerror!(ZErrorKind::InvalidSession{
                descr: e
            }))
        }
    );
}

/// # Example:
/// ```
/// use async_std::sync::Arc;
/// use async_trait::async_trait;
/// use zenoh_protocol::core::PeerId;
/// use zenoh_protocol::proto::{WhatAmI, whatami};
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
///     whatami: whatami::PEER,
///     id: PeerId{id: vec![1, 2]},
///     handler: Arc::new(MySH::new())
/// };
/// let manager = SessionManager::new(config, None);
/// 
/// // Create the SessionManager with optional configuration
/// let config = SessionManagerConfig {
///     version: 0,
///     whatami: whatami::PEER,
///     id: PeerId{id: vec![3, 4]},
///     handler: Arc::new(MySH::new())
/// };
/// // Setting a value to None indicates to use the default value
/// let opt_config = SessionManagerOptionalConfig {
///     lease: Some(1_000),     // Set the default lease to 1s
///     sn_resolution: None,       // Use the default sequence number resolution
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
    pub sn_resolution: Option<ZInt>,
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
        let mut sn_resolution = *SESSION_SEQ_NUM_RESOLUTION;
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
            if let Some(v) = opt.sn_resolution {
                sn_resolution = v;
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
            whatami: config.whatami,
            pid: config.id.clone(),            
            lease,
            sn_resolution,
            batchsize,
            timeout,
            retries, 
            max_sessions,
            max_links,
            handler: config.handler,
        };
        // Create the inner session manager
        let manager_inner = Arc::new(SessionManagerInner::new(inner_config));
        // Create the initial session used to establish new connections
        let initial_session = Arc::new(InitialSession::new(manager_inner.clone()));
        // Add the session to the inner session manager
        manager_inner.init_initial_session(initial_session);

        SessionManager(manager_inner)
    }

    /*************************************/
    /*              SESSION              */
    /*************************************/
    pub async fn open_session(&self, locator: &Locator, attachment: &Option<Attachment>) -> ZResult<Session> {
        // Retrieve the initial session
        let initial = self.0.get_initial_session().await;
        let transport = self.0.get_initial_transport().await;
        // Create the timeout duration
        let to = Duration::from_millis(self.0.config.timeout);

        // Automatically create a new link manager for the protocol if it does not exist
        let manager = self.0.get_or_new_link_manager(&self.0, &locator.get_proto()).await;
        // Create a new link associated by calling the Link Manager
        let link = match manager.new_link(&locator, &transport).await {
            Ok(link) => link,
            Err(e) => {
                log::error!("Can not to create a link to locator {}: {}", locator, e);
                return Err(e)
            }
        };
        // Create a channel for knowing when a session is open
        let (sender, receiver) = channel::<ZResult<Session>>(1);
        
        // Try a maximum number of times to open a session
        let retries = self.0.config.retries;
        for i in 0..retries {
            // Create the open future
            let open_fut = initial.open(&link, attachment, &sender).timeout(to);
            let channel_fut = receiver.recv().timeout(to);

            // Check the future result
            match open_fut.try_join(channel_fut).await {
                // Future timeout result
                Ok((_, channel_res)) => match channel_res {
                    // Channel result
                    Ok(res) => match res {
                        Ok(session) => return Ok(session),
                        Err(e) => {
                            let e = format!("Can not open a session to {}: {}", locator, e);
                            log::error!("{}", e);
                            return Err(zerror!(ZErrorKind::Other {
                                descr: e
                            }))
                        }
                    },
                    Err(e) => {
                        let e = format!("Can not open a session to {}: {}", locator, e);
                        log::error!("{}", e);
                        return Err(zerror!(ZErrorKind::Other {
                            descr: e
                        }))
                    }
                },
                Err(e) => {
                    log::debug!("Can not open a session to {}: {}. Timeout: {:?}. Attempt: {}/{}", locator, e, to, i, retries);
                    continue
                }
            }
        }

        // Delete the link form the link manager
        let _ = manager.del_link(&link.get_src(), &link.get_dst()).await;

        let e = format!("Can not open a session to {}: maximum number of attemps reached ({})", locator, retries);
        log::error!("{}", e);
        Err(zerror!(ZErrorKind::Other {
            descr: e
        }))
    }

    pub async fn get_sessions(&self) -> Vec<Session> {
        self.0.get_sessions().await
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


pub(crate) struct SessionManagerInnerConfig {
    pub(super) version: u8,
    pub(super) whatami: WhatAmI,
    pub(super) pid: PeerId,
    pub(super) lease: ZInt,
    pub(super) sn_resolution: ZInt,
    pub(super) batchsize: usize,
    pub(super) timeout: u64,
    pub(super) retries: usize,
    pub(super) max_sessions: Option<usize>,
    pub(super) max_links: Option<usize>,
    pub(super) handler: Arc<dyn SessionHandler + Send + Sync>
}

pub(crate) struct SessionManagerInner {
    pub(crate) config: SessionManagerInnerConfig,    
    initial: RwLock<Option<Arc<InitialSession>>>,
    protocols: RwLock<HashMap<LocatorProtocol, LinkManager>>,
    sessions: RwLock<HashMap<PeerId, Arc<Channel>>>,
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
    fn init_initial_session(&self, session: Arc<InitialSession>) {
        *self.initial.try_write().unwrap() = Some(session);
    }

    /*************************************/
    /*            LINK MANAGER           */
    /*************************************/
    async fn get_or_new_link_manager(
        &self,
        a_self: &Arc<Self>,
        protocol: &LocatorProtocol,
    ) -> LinkManager {
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
    ) -> ZResult<LinkManager> {
        let mut w_guard = zasyncwrite!(self.protocols);
        if w_guard.contains_key(protocol) {
            return Err(zerror!(ZErrorKind::Other {
                descr: format!("Can not create the link manager for protocol ({}) because it already exists", protocol)
            }));
        }

        let lm = LinkManagerBuilder::make(a_self.clone(), protocol);
        w_guard.insert(protocol.clone(), lm.clone());
        Ok(lm)
    }

    async fn get_link_manager(&self, protocol: &LocatorProtocol) -> ZResult<LinkManager> {
        match zasyncread!(self.protocols).get(protocol) {
            Some(manager) => Ok(manager.clone()),
            None => {
                Err(zerror!(ZErrorKind::Other {
                    descr: format!("Can not get the link manager for protocol ({}) because it has not been found", protocol)
                }))
            }
        }
    }

    async fn del_link_manager(&self, protocol: &LocatorProtocol) -> ZResult<()> {
        match zasyncwrite!(self.protocols).remove(protocol) {
            Some(_) => Ok(()),
            None => {
                Err(zerror!(ZErrorKind::Other {
                    descr: format!("Can not delete the link manager for protocol ({}) because it has not been found.", protocol)
                }))
            }
        }
    }

    pub(super) async fn get_locators(&self) -> Vec<Locator> {
        let mut vec: Vec<Locator> = Vec::new();
        for p in zasyncread!(self.protocols).values() {
            vec.extend_from_slice(&p.get_listeners().await);
        }
        vec
    }

    /*************************************/
    /*              SESSION              */
    /*************************************/
    pub(crate) async fn get_initial_transport(&self) -> Transport {
        zasyncread!(self.initial).as_ref().unwrap().clone()
    }

    pub(super) async fn get_initial_session(&self) -> Arc<InitialSession> {
        zasyncread!(self.initial).as_ref().unwrap().clone()
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn get_or_new_session(
        &self,
        a_self: &Arc<Self>,
        peer: &PeerId,
        whatami: &WhatAmI,
        lease: ZInt,
        sn_resolution: ZInt,
        initial_sn_tx: ZInt,
        initial_sn_rx: ZInt
    ) -> Session {
        loop {
            match self.get_session(peer).await {
                Ok(session) => return session,
                Err(_) => match self.new_session(
                    a_self, peer, whatami, lease, sn_resolution, initial_sn_tx, initial_sn_rx
                ).await {
                    Ok(session) => return session,
                    Err(_) => continue
                }
            }
        }
    }

    pub(super) async fn del_session(&self, peer: &PeerId) -> ZResult<()> {
        match zasyncwrite!(self.sessions).remove(peer) {
            Some(_) => Ok(()),
            None => {
                let e = format!("Can not delete the session of peer: {}", peer);
                log::trace!("{}", e);
                Err(zerror!(ZErrorKind::Other {
                    descr: e
                }))
            }
        }
    }

    pub(super) async fn get_session(&self, peer: &PeerId) -> ZResult<Session> {
        match zasyncread!(self.sessions).get(peer) {
            Some(channel) => Ok(Session::new(Arc::downgrade(&channel))),
            None => {
                let e = format!("Can not get the session of peer: {}", peer);
                log::trace!("{}", e);
                Err(zerror!(ZErrorKind::Other {
                    descr: e
                }))
            }
        }
    }

    pub(super) async fn get_sessions(&self) -> Vec<Session> {
        zasyncread!(self.sessions).values()
            .map(|x| Session::new(Arc::downgrade(&x))).collect()
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn new_session(
        &self,
        a_self: &Arc<Self>,
        peer: &PeerId,
        whatami: &WhatAmI,
        lease: ZInt,
        sn_resolution: ZInt,
        initial_sn_tx: ZInt,
        initial_sn_rx: ZInt,
    ) -> ZResult<Session> {
        let mut w_guard = zasyncwrite!(self.sessions);
        if w_guard.contains_key(peer) {
            let e = format!("Can not create a new session for peer: {}", peer);
            log::trace!("{}", e);
            return Err(zerror!(ZErrorKind::Other {
                descr: e
            }));
        }

        // Create the channel object
        let a_ch = Arc::new(Channel::new(  
            a_self.clone(),          
            peer.clone(),
            *whatami,            
            lease,
            sn_resolution,
            initial_sn_tx,
            initial_sn_rx,
            self.config.batchsize   
        ));
        // Start the channel
        a_ch.initialize(Arc::downgrade(&a_ch));

        // Create a weak reference to the session
        let session = Session::new(Arc::downgrade(&a_ch));
        // Add the session to the list of active sessions
        w_guard.insert(peer.clone(), a_ch);

        Ok(session)
    }
}

/*************************************/
/*              SESSION              */
/*************************************/

/// [`Session`] is the session handler returned when opening a new session
#[derive(Clone)]
pub struct Session(Weak<Channel>);

impl Session {
    fn new(inner: Weak<Channel>) -> Self {
        Self(inner)
    }

    /*************************************/
    /*         SESSION ACCESSORS         */
    /*************************************/
    pub(super) fn get_transport(&self) -> ZResult<Transport> {
        let channel = zchannel!(self.0);
        Ok(channel)
    }

    pub(super) fn has_callback(&self) -> ZResult<bool> {
        let channel = zchannel!(self.0);
        Ok(channel.has_callback())
    }

    pub(super) async fn set_callback(&self, callback: Arc<dyn MsgHandler + Send + Sync>) -> ZResult<()> {
        let channel = zchannel!(self.0);
        channel.set_callback(callback).await;
        Ok(())
    }

    pub(super) async fn add_link(&self, link: Link) -> ZResult<()> {
        let channel = zchannel!(self.0);
        channel.add_link(link).await?;
        Ok(())
    }

    pub(super) async fn _del_link(&self, link: Link) -> ZResult<()> {
        let channel = zchannel!(self.0);
        channel.del_link(&link).await?;
        Ok(())
    }

    /*************************************/
    /*          PUBLIC ACCESSORS         */
    /*************************************/
    pub fn get_peer(&self) -> ZResult<PeerId> {
        let channel = zchannel!(self.0);
        Ok(channel.get_peer())
    }

    pub fn get_lease(&self) -> ZResult<ZInt> {
        let channel = zchannel!(self.0);
        Ok(channel.get_lease())
    }

    pub fn get_sn_resolution(&self) -> ZResult<ZInt> {
        let channel = zchannel!(self.0);
        Ok(channel.get_sn_resolution())
    }

    pub async fn close(&self) -> ZResult<()> {
        log::trace!("{:?}. Close", self);
        let channel = zchannel!(self.0);
        channel.close().await
    }    

    pub async fn get_links(&self) -> ZResult<Vec<Link>> {
        log::trace!("{:?}. Get links", self);
        let channel = zchannel!(self.0);
        Ok(channel.get_links().await)
    }

    pub async fn schedule(&self, message: ZenohMessage, link: Option<Link>) -> ZResult<()> {  
        log::trace!("{:?}. Schedule: {:?}", self, message);      
        let channel = zchannel!(self.0);
        channel.schedule(message, link).await;
        Ok(())
    }

    pub async fn schedule_batch(&self, messages: Vec<ZenohMessage>, link: Option<Link>) -> ZResult<()> {
        log::trace!("{:?}. Schedule batch: {:?}", self, messages);
        let channel = zchannel!(self.0);
        channel.schedule_batch(messages, link).await;
        Ok(())
    }
}

#[async_trait]
impl MsgHandler for Session {
    #[inline]
    async fn handle_message(&self, message: ZenohMessage) -> ZResult<()> {
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
        if let Some(channel) = self.0.upgrade() {
            write!(f, "Session: {}", channel.get_peer())
        } else {
            write!(f, "Session closed")
        }
    }
}