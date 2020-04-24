use async_std::sync::{channel, Arc, RwLock, Sender};
// use async_std::task;
use std::collections::{HashMap, HashSet};
// use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::core::{AtomicZInt, ZError, ZErrorKind, ZInt, ZResult};
use crate::link::Link;
use crate::proto::Message;
use crate::session::{ConduitRx, ConduitTx, MsgHandler, SessionInner};
use crate::zerror;
use zenoh_util::{zasyncread, zasyncwrite};

/*************************************/
/*          TRANSPORT TX             */
/*************************************/

// Struct to add additional fields to the message required for transmission
pub(crate) struct MessageTx {
    // The inner message to transmit
    pub(crate) inner: Message,
    // The preferred link to transmit the Message on
    pub(crate) link: Option<Link>,
    pub(crate) notify: Option<Sender<ZResult<()>>>,
}

// Struct implementing the transmission portion of the transport
pub struct TransportTx {
    // The default resolution to be used for the SN
    resolution: AtomicZInt,
    // The default batchsize
    batchsize: AtomicUsize,
    // The links associated to this transport
    links: RwLock<HashSet<Link>>,
    has_links: AtomicBool,
    // Conduits
    conduits: RwLock<HashMap<ZInt, Arc<ConduitTx>>>
}

impl TransportTx {
    pub(crate) fn new(resolution: ZInt, batchsize: usize) -> TransportTx {
        TransportTx {
            // Session sequence number resolution
            resolution: AtomicZInt::new(resolution),
            // The default batchsize
            batchsize: AtomicUsize::new(batchsize),
            // The links associated to this transport
            links: RwLock::new(HashSet::new()),
            has_links: AtomicBool::new(false),
            // Conduits
            conduits: RwLock::new(HashMap::new()),
        }
    }

    /*************************************/
    /*          ACCESSORS TX             */
    /*************************************/
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

    pub(crate) fn has_links(&self) -> bool {
        self.has_links.load(Ordering::Relaxed)
    }

    pub(crate) async fn get_links(&self) -> Vec<Link> {
        zasyncread!(self.links).iter().cloned().collect()
    }

    /*************************************/
    /*           CONDUIT TX              */
    /*************************************/
    pub(crate) async fn add_conduit(&self, id: ZInt) -> Arc<ConduitTx> {
        // Acquire the lock on the conduits
        let mut guard = zasyncwrite!(self.conduits);

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

    #[allow(dead_code)]
    pub(crate) async fn del_conduit(&self, id: ZInt) -> Option<Arc<ConduitTx>> {
        // Acquire the lock on the links
        let mut guard = zasyncwrite!(self.conduits);

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

    /*************************************/
    /*            LINKS TX               */
    /*************************************/
    pub(crate) async fn add_link(&self, link: Link) -> ZResult<()> {
        // Links
        {
            // Acquire the lock on the links
            let mut l_guard = zasyncwrite!(self.links);

            // Check if this link is not already present
            if l_guard.contains(&link) {
                return Err(zerror!(ZErrorKind::InvalidLink {
                    descr: format!("{}", link)
                }));
            }

            // Add the link to the transport
            l_guard.insert(link.clone());
            self.has_links.store(true, Ordering::Relaxed);
        }
        // Conduits
        {
            // Acquire the lock on the conduits
            let c_guard = zasyncwrite!(self.conduits);
            // Add the link to the conduits
            // @TODO: Enable a smart allocation of links across the various conduits
            //        The link is added to all the conduits for the time being
            for (_, conduit) in c_guard.iter() {
                let _ = conduit.add_link(link.clone()).await;
                ConduitTx::start(&conduit).await;
            }
        }

        Ok(())
    }

    pub(crate) async fn del_link(&self, link: &Link) -> ZResult<()> {
        // Acquire the lock on the links
        let mut l_guard = zasyncwrite!(self.links);
        // Acquire the lock on the conduits
        let c_guard = zasyncread!(self.conduits);

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
        if l_guard.is_empty() {
            self.has_links.store(false, Ordering::Relaxed);
        }

        Ok(())
    }

    /*************************************/
    /*        SCHEDULE AND SEND TX       */
    /*************************************/
    // Schedule the message to be sent asynchronsly
    pub(crate) async fn schedule(&self, message: Message, priority: usize, link: Option<Link>) {
        let message = MessageTx {
            inner: message,
            notify: None,
            link
        };
        // Wait for the queue to have space for the message
        self.push_on_conduit_queue(message, priority).await;
    }

    // Schedule a batch of messages to be sent asynchronsly
    pub(crate) async fn schedule_batch(&self, mut messages: Vec<Message>, priority: usize, link: Option<Link>, cid: Option<ZInt>) {
        let cid: ZInt = cid.unwrap_or(0);
        let messages = messages.drain(..).map(|mut x| {
            x.cid = cid;
            MessageTx {
                inner: x,
                link: link.clone(),
                notify: None
            }
        }).collect();
        // Wait for the queue to have space for the message
        self.push_batch_on_conduit_queue(messages, priority, cid).await;
    }

    // Schedule the message to be sent asynchronsly and notify once sent
    pub(crate) async fn send(&self, message: Message, priority: usize, link: Option<Link>) -> ZResult<()> {
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
        let guard = zasyncread!(self.conduits);
        if let Some(conduit) = guard.get(&message.inner.cid) {
            conduit.queue.push(message, priority).await;
        } else {
            // Drop the guard to dynamically add the new conduit
            drop(guard);
            // Add the new conduit
            let conduit = self.add_conduit(message.inner.cid).await;
            conduit.queue.push(message, priority).await
        }
    }

    async fn push_batch_on_conduit_queue(&self, messages: Vec<MessageTx>, priority: usize, cid: ZInt) {
        // Push the message on the conduit queue
        // If the conduit does not exist, create it on demand
        let guard = zasyncread!(self.conduits);
        if let Some(conduit) = guard.get(&cid) {
            conduit.queue.push_batch(messages, priority).await;
        } else {
            // Drop the guard to dynamically add the new conduit
            drop(guard);
            // Add the new conduit
            let conduit = self.add_conduit(cid).await;
            conduit.queue.push_batch(messages, priority).await;
        }
    }

    /*************************************/
    /*        CLOSE TRANSPORT TX         */
    /*************************************/
    pub async fn close(&self) -> ZResult<()> {
        // Stop the Tx conduits
        for (_, conduit) in zasyncwrite!(self.conduits).drain().take(1) {
            conduit.stop().await;
        }

        // Close all the links
        for l in zasyncwrite!(self.links).drain().take(1) {
            let _ = l.stop().await;
            let _ = l.close().await;
        }

        Ok(())
    }
}


/*************************************/
/*          TRANSPORT RX             */
/*************************************/

// Struct implementing the receiving portion of the transport
pub struct TransportRx {
    // The reference to the session
    session: RwLock<Option<Arc<SessionInner>>>,
    // The callback for Data messages
    callback: RwLock<Option<Arc<dyn MsgHandler + Send + Sync>>>,
    has_callback: AtomicBool,
    // The default timeout after which the session is closed if no messages are received
    lease: AtomicZInt,
    // The default resolution to be used for the SN
    resolution: AtomicZInt,
    // Conduits
    conduits: RwLock<HashMap<ZInt, Arc<ConduitRx>>>
}

impl TransportRx {
    pub(crate) fn new(lease: ZInt, resolution: ZInt) -> TransportRx {
        TransportRx {
            // Session manager and Callback
            session: RwLock::new(None),
            // The callback for Data messages
            callback: RwLock::new(None),
            has_callback: AtomicBool::new(false),
            // Session lease
            lease: AtomicZInt::new(lease),
            // The default resolution to be used for the SN
            resolution: AtomicZInt::new(resolution),
            // Conduits
            conduits: RwLock::new(HashMap::new())
        }
    }

    /*************************************/
    /*        INITIALIZATION RX          */
    /*************************************/
    pub(crate) fn init_session(&self, session: Arc<SessionInner>) {
        *self.session.try_write().unwrap() = Some(session);
    }

    pub(crate) fn init_callback(&self, callback: Arc<dyn MsgHandler + Send + Sync>) {
        *self.callback.try_write().unwrap() = Some(callback);
        self.has_callback.store(true, Ordering::Relaxed);
    }

    /*************************************/
    /*          ACCESSORS RX             */
    /*************************************/
    pub(crate) fn set_lease(&self, lease: ZInt) {
        self.lease.store(lease, Ordering::Release);
    }

    #[allow(dead_code)]
    pub(crate) fn get_lease(&self) -> ZInt {
        self.lease.load(Ordering::Relaxed)
    }

    #[allow(dead_code)]
    pub(crate) fn set_resolution(&self, resolution: ZInt) {
        self.resolution.store(resolution, Ordering::Relaxed);
        // @TODO: set the resolution on the conduits
    }

    pub(crate) fn get_resolution(&self) -> ZInt {
        self.resolution.load(Ordering::Relaxed)
    }

    pub(crate) fn has_callback(&self) -> bool {
        self.has_callback.load(Ordering::Relaxed)
    }

    /*************************************/
    /*            LINKS RX               */
    /*************************************/
    pub(crate) async fn link_err(&self, link: &Link) {
        if let Some(session) = zasyncread!(self.session).as_ref() {
            let _ = session.del_link(link).await;
        }
    }

    /*************************************/
    /*           CONDUIT RX              */
    /*************************************/
    pub(crate) async fn add_conduit(&self, id: ZInt) -> Option<Arc<ConduitRx>> {
        // Acquire the lock on the conduits
        let mut guard = zasyncwrite!(self.conduits);

        // Get the perameters from the transport
        let resolution = self.get_resolution();
        let session = if let Some(session) = zasyncread!(self.session).as_ref() {
            session.clone()
        } else {
            return None;
        };
        let callback = if let Some(callback) = zasyncread!(self.callback).as_ref() {
            callback.clone()
        } else {
            return None;
        };

        // Add the Rx conduit
        let conduit = Arc::new(ConduitRx::new(id, resolution, session, callback));
        guard.insert(id, conduit.clone());

        Some(conduit)
    }

    #[allow(dead_code)]
    pub(crate) async fn del_conduit(&self, id: ZInt) -> Option<Arc<ConduitRx>> {
        // Acquire the lock on the links
        let mut guard = zasyncwrite!(self.conduits);

        // Remove the Rx conduit
        guard.remove(&id)
    }

    /*************************************/
    /*   MESSAGE RECEIVED FROM THE LINK  */
    /*************************************/
    pub async fn receive_message(&self, link: &Link, message: Message) -> Option<Arc<TransportRx>> {
        // Acquire a read lock on the conduits
        let guard = zasyncread!(self.conduits);
        // If the conduit does not exists, automatically create it
        match guard.get(&message.cid) {
            Some(conduit) => conduit.receive_message(link, message).await,
            None => {
                // Drop the read guard to allow to add the new conduit
                drop(guard);
                // Dynamically add a new conduit. The conduit is added iff the
                // transport session and callback have been properly initialized.
                if let Some(rx) = self.add_conduit(message.cid).await {
                    // Process the message
                    rx.receive_message(link, message).await
                } else {
                    // Drop the incoming message since the
                    // transport session is not ready yet.
                    println!(
                        "!!! Message dropped because conduit {} does not exist: {:?}",
                        message.cid, message.body
                    );
                    None
                }
            }
        }
    }

    /*************************************/
    /*        CLOSE THE TRANSPORT        */
    /*************************************/
    pub async fn close(&self) -> ZResult<()> {
        // Notify the callback
        if let Some(callback) = zasyncwrite!(self.callback).take() {
            self.has_callback.store(false, Ordering::Relaxed);
            callback.close().await;
        }

        // Clear the Rx conduits
        zasyncwrite!(self.conduits).clear();

        Ok(())
    }
}

// impl Drop for Transport {
//     fn drop(&mut self) {
//         println!("Dropped {:?}", self);
//         // @TODO: stop and close the transport
//     }
// }

// impl fmt::Debug for Transport {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         let links = task::block_on(async {
//             let mut s = String::new();
//             for l in zasyncread!(self.links).iter() {
//                 s.push_str(&format!("\n\t[({:?}) => ({:?})]", l.get_src(), l.get_dst()));
//             }
//             s
//         });
//         write!(f, "Links:{}", links)
//     }
// }
