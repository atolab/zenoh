use async_std::sync::{channel, Arc, RwLock, Sender};
use async_std::task;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::core::{AtomicZInt, ZError, ZErrorKind, ZInt, ZResult};
use crate::link::Link;
use crate::proto::Message;
use crate::session::{ConduitRx, ConduitTx, MsgHandler, SessionInner};
use crate::zerror;
use zenoh_util::{zasyncread, zasyncwrite, zrwopt};

/*************************************/
/*          TRANSPORT                */
/*************************************/

// Struct to add additional fields to the message required for transmission
pub(crate) struct MessageTx {
    // The inner message to transmit
    pub(crate) inner: Message,
    // The preferred link to transmit the Message on
    pub(crate) link: Option<Link>,
    pub(crate) notify: Option<Sender<ZResult<()>>>,
}

// Struct implementing the transport
pub struct Transport {
    // The reference to the session
    session: RwLock<Option<Arc<SessionInner>>>,
    // The callback for Data messages
    callback: RwLock<Option<Arc<dyn MsgHandler + Send + Sync>>>,
    // Mark if this transport is associated to the initial session
    is_initial: bool,
    // The default timeout after which the session is closed if no messages are received
    lease: AtomicZInt,
    // The default resolution to be used for the SN
    resolution: AtomicZInt,
    // The default batchsize
    batchsize: AtomicUsize,
    // The links associated to this transport
    links: RwLock<HashSet<Link>>,
    // Conduits
    tx: RwLock<HashMap<ZInt, Arc<ConduitTx>>>,
    rx: RwLock<HashMap<ZInt, Arc<ConduitRx>>>,
}

impl Transport {
    pub(crate) fn new(
        lease: ZInt,
        resolution: ZInt,
        batchsize: usize,
        is_initial: bool,
    ) -> Transport {
        Transport {
            // Session manager and Callback
            session: RwLock::new(None),
            // The callback for Data messages
            callback: RwLock::new(None),
            // Mark if this transport is associated to the initial session
            is_initial,
            // Session lease
            lease: AtomicZInt::new(lease),
            // Session sequence number resolution
            resolution: AtomicZInt::new(resolution),
            // The default batchsize
            batchsize: AtomicUsize::new(batchsize),
            // The links associated to this transport
            links: RwLock::new(HashSet::new()),
            // The sequence number generator
            tx: RwLock::new(HashMap::new()),
            rx: RwLock::new(HashMap::new()),
        }
    }

    /*************************************/
    /*          INITIALIZATION           */
    /*************************************/
    pub(crate) fn set_session(&self, session: Arc<SessionInner>) {
        *self.session.try_write().unwrap() =  Some(session);
    }

    pub(crate) fn set_callback(&self, callback: Arc<dyn MsgHandler + Send + Sync>) {
        *self.callback.try_write().unwrap() = Some(callback);
    }

    pub(crate) fn get_callback(&self) -> Option<Arc<dyn MsgHandler + Send + Sync>> {
        self.callback.try_read().unwrap().clone()
    }

    /*************************************/
    /*            ACCESSORS              */
    /*************************************/
    pub(crate) fn set_lease(&self, lease: ZInt) {
        self.lease.store(lease, Ordering::Release);
    }

    #[allow(dead_code)]
    pub(crate) fn get_lease(&self) -> ZInt {
        self.lease.load(Ordering::Relaxed)
    }

    #[allow(dead_code)]
    pub(crate) fn set_batchsize(&self, size: usize) {
        self.batchsize.store(size, Ordering::Relaxed);
    }

    pub(crate) fn get_batchsize(&self) -> usize {
        self.batchsize.load(Ordering::Relaxed)
    }

    #[allow(dead_code)]
    pub(crate) fn set_resolution(&self, resolution: ZInt) {
        self.resolution.store(resolution, Ordering::Relaxed);
        // @TODO: set the resolution on the conduits
    }

    pub(crate) fn get_resolution(&self) -> ZInt {
        self.resolution.load(Ordering::Relaxed)
    }

    pub(crate) async fn get_links(&self) -> Vec<Link> {
        zasyncread!(self.links).iter().cloned().collect()
    }

    /*************************************/
    /*            CONDUIT                */
    /*************************************/
    pub(crate) async fn add_conduit_tx(&self, id: ZInt) -> Arc<ConduitTx> {
        // Acquire the lock on the conduits
        let mut guard = zasyncwrite!(self.tx);

        // Get the perameters from the transport
        let resolution = self.get_resolution();
        let batchsize = self.get_batchsize();

        // Add the Tx conduit
        let conduit = Arc::new(ConduitTx::new(id, resolution, batchsize));

        // Add all the available links to the conduit
        // @TODO: perform an intelligent assocation of links to conduits
        let links = zasyncread!(self.links);
        for l in links.iter() {
            let _ = conduit.add_link(l.clone()).await;
            ConduitTx::start(&conduit).await;
        }

        // Add the conduit to the HashMap
        guard.insert(id, conduit.clone());

        conduit
    }

    pub(crate) async fn add_conduit_rx(&self, id: ZInt) -> Arc<ConduitRx> {
        // Acquire the lock on the conduits
        let mut guard = zasyncwrite!(self.rx);

        // Get the perameters from the transport
        let resolution = self.get_resolution();
        let session = zrwopt!(self.session).clone();
        let callback = zrwopt!(self.callback).clone();

        // Add the Rx conduit
        let conduit = Arc::new(ConduitRx::new(id, resolution, session, callback));
        guard.insert(id, conduit.clone());

        conduit
    }

    #[allow(dead_code)]
    pub(crate) async fn del_conduit_tx(&self, id: ZInt) -> Option<Arc<ConduitTx>> {
        // Acquire the lock on the links
        let mut guard = zasyncwrite!(self.tx);

        // Remove the Tx conduit
        let mut conduit = guard.remove(&id);
        if let Some(cond) = conduit {
            // If the conduit was present, stop it
            cond.stop().await;
            // Return the ownership
            conduit = Some(cond)
        }

        conduit
    }

    #[allow(dead_code)]
    pub(crate) async fn del_conduit_rx(&self, id: ZInt) -> Option<Arc<ConduitRx>> {
        // Acquire the lock on the links
        let mut guard = zasyncwrite!(self.rx);

        // Remove the Rx conduit
        guard.remove(&id)
    }

    /*************************************/
    /*               LINK                */
    /*************************************/
    pub(crate) async fn add_link(&self, link: Link) -> ZResult<()> {
        // Acquire the lock on the links
        let mut l_guard = zasyncwrite!(self.links);
        // Acquire the lock on the conduits
        let c_guard = zasyncwrite!(self.tx);

        // Check if this link is not already present
        if l_guard.contains(&link) {
            return Err(zerror!(ZErrorKind::InvalidLink {
                descr: format!("{}", link)
            }));
        }

        // Add the link to the conduits
        // @TODO: Enable a smart allocation of links across the various conduits
        //        The link is added to all the conduits for the time being
        for (_, conduit) in c_guard.iter() {
            let _ = conduit.add_link(link.clone()).await;
            ConduitTx::start(&conduit).await;
        }

        // Add the link to the transport
        l_guard.insert(link);

        Ok(())
    }

    pub(crate) async fn del_link(&self, link: &Link) -> ZResult<()> {
        let is_empty = {
            // Acquire the lock on the links
            let mut l_guard = zasyncwrite!(self.links);
            // Acquire the lock on the conduits
            let c_guard = zasyncread!(self.tx);

            // Delete the link from all the conduits
            for (_, conduit) in c_guard.iter() {
                let _ = conduit.del_link(&link).await;
            }

            // Delete the link from the transport
            if !l_guard.remove(link) {
                return Err(zerror!(ZErrorKind::InvalidLink {
                    descr: format!("{}", link)
                }));
            } 
            
            l_guard.is_empty()
        };
        // Check if there are links left and this is not the initial session
        if is_empty && !self.is_initial {
            // If this transport is not associated to the initial session then
            // close the session, notify the manager, and notify the callback
            let _ = zrwopt!(self.session).delete().await;
            let _ = self.close().await;
        }

        Ok(())
    }

    /*************************************/
    /*        SCHEDULE AND SEND          */
    /*************************************/
    // Schedule the message to be sent asynchronsly
    pub(crate) async fn schedule(
        &self,
        message: Message,
        priority: usize,
        link: Option<Link>
    ) {
        let message = MessageTx {
            inner: message,
            notify: None,
            link
        };
        // Wait for the queue to have space for the message
        self.push_on_conduit_queue(message, priority).await;
    }

    // Schedule a batch of messages to be sent asynchronsly
    pub(crate) async fn schedule_batch(
        &self,
        mut messages: Vec<Message>,
        priority: usize,
        link: Option<Link>,
        cid: Option<ZInt>
    ) {
        let cid: ZInt = cid.unwrap_or(0);
        let messages = messages.drain(..).map(|mut x| {
            x.cid = cid;
            MessageTx {
                inner: x,
                link: link.clone(),
                notify: None,
            }
        }).collect();
        // Wait for the queue to have space for the message
        self.push_batch_on_conduit_queue(messages, priority, cid).await;
    }

    // Schedule the message to be sent asynchronsly and notify once sent
    pub(crate) async fn send(
        &self,
        message: Message,
        priority: usize,
        link: Option<Link>
    ) -> ZResult<()> {
        let (sender, receiver) = channel::<ZResult<()>>(1);
        let message = MessageTx {
            inner: message,
            link,
            notify: Some(sender),
        };
        // Wait for the transmission queue to have space for the message
        self.push_on_conduit_queue(message, priority).await;
        // Wait for the message to be actually sent
        match receiver.recv().await {
            Some(res) => res,
            None => Err(zerror!(ZErrorKind::Other {
                descr: "Send failed unexpectedly!".to_string()
            })),
        }
    }

    async fn push_on_conduit_queue(&self, message: MessageTx, priority: usize) {
        // Push the message on the conduit queue
        // If the conduit does not exist, create it on demand
        let guard = zasyncread!(self.tx);
        if let Some(conduit) = guard.get(&message.inner.cid) {
            conduit.queue.push(message, priority).await;
        } else {
            // Drop the guard to dynamically add the new conduit
            drop(guard);
            // Add the new conduit
            let conduit = self.add_conduit_tx(message.inner.cid).await;
            conduit.queue.push(message, priority).await
        }
    }

    async fn push_batch_on_conduit_queue(&self, messages: Vec<MessageTx>, priority: usize, cid: ZInt) {
        // Push the message on the conduit queue
        // If the conduit does not exist, create it on demand
        let guard = zasyncread!(self.tx);
        if let Some(conduit) = guard.get(&cid) {
            conduit.queue.push_batch(messages, priority).await;
        } else {
            // Drop the guard to dynamically add the new conduit
            drop(guard);
            // Add the new conduit
            let conduit = self.add_conduit_tx(cid).await;
            conduit.queue.push_batch(messages, priority).await;
        }
    }

    /*************************************/
    /*   MESSAGE RECEIVED FROM THE LINK  */
    /*************************************/
    pub async fn receive_message(
        &self,
        link: &Link,
        message: Message,
    ) -> Option<Arc<Transport>> {
        // Acquire a read lock on the conduits
        let guard = zasyncread!(self.rx);
        // If the conduit does not exists, automatically create it
        match guard.get(&message.cid) {
            Some(conduit) => conduit.receive_message(link, message).await,
            None => {
                // Drop the read guard to allow to add the new conduit
                drop(guard);
                // Dynamically add a new conduit
                let rx = self.add_conduit_rx(message.cid).await;
                // Process the message
                rx.receive_message(link, message).await
            }
        }
    }

    /*************************************/
    /*        CLOSE THE TRANSPORT        */
    /*************************************/
    pub async fn close(&self) -> ZResult<()> {
        // Notify the callback
        if self.get_callback().is_some() {
            zrwopt!(self.callback).close().await;
        }

        // Stop the Tx conduits
        for (_, conduit) in zasyncwrite!(self.tx).drain().take(1) {
            conduit.stop().await;
        }

        // Remove and close all the links
        for l in zasyncwrite!(self.links).drain() {
            let _ = l.close().await;
        }

        // Clear the Rx conduits
        zasyncwrite!(self.rx).clear();

        // Remove the reference to the callback
        *zasyncwrite!(self.callback) = None;
        // Remove the reference to the session
        *zasyncwrite!(self.session) = None;

        Ok(())
    }
}

impl Drop for Transport {
    fn drop(&mut self) {
        // @TODO: stop and close the transport
    }
}

impl fmt::Debug for Transport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let links = task::block_on(async {
            let mut s = String::new();
            for l in zasyncread!(self.links).iter() {
                s.push_str(&format!("\n\t[({:?}) => ({:?})]", l.get_src(), l.get_dst()));
            }
            s
        });
        write!(f, "Links:{}", links)
    }
}
