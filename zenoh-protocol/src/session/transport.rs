use async_std::sync::{channel, Arc, RwLock, Sender, Weak};
use async_std::task;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::core::{AtomicZInt, ZError, ZErrorKind, ZInt, ZResult};
use crate::link::Link;
use crate::proto::Message;
use crate::session::{Conduit, MsgHandler, SessionInner};
use crate::zerror;
use zenoh_util::{zasyncread, zasyncwrite};


/*************************************/
/*          TRANSPORT TX             */
/*************************************/

pub enum Action {
    ChangeTransport(Arc<Transport>),
    Close,
    Read
}

// Struct to add additional fields to the message required for transmission
pub(crate) struct MessageTx {
    // The inner message to transmit
    pub(crate) inner: Message,
    // The preferred link to transmit the Message on
    pub(crate) link: Option<Link>,
    pub(crate) notify: Option<Sender<ZResult<()>>>,
}

// Struct implementing the transmission portion of the transport
pub struct Transport {
    // The reference to the session
    session: RwLock<Option<Weak<SessionInner>>>,
    // The callback for Data messages
    callback: RwLock<Option<Arc<dyn MsgHandler + Send + Sync>>>,
    // The callback has been set or not
    has_callback: AtomicBool,
    // The default timeout after which the session is closed if no messages are received
    lease: AtomicZInt,
    // The default resolution to be used for the SN
    resolution: AtomicZInt,
    // The default batchsize
    batchsize: AtomicUsize,
    // The links associated to this transport
    links: RwLock<HashSet<Link>>,
    // The transport has no links or not
    num_links: AtomicUsize,
    // Conduits
    conduits: RwLock<HashMap<ZInt, Arc<Conduit>>>
}

impl Transport {
    pub(crate) fn new(lease: ZInt, resolution: ZInt, batchsize: usize) -> Transport {
        Transport {
            // The reference to the session
            session: RwLock::new(None),
            // The callback for Data messages
            callback: RwLock::new(None),
            // The callback has been set or not
            has_callback: AtomicBool::new(false),
            // The default lease
            lease: AtomicZInt::new(lease),
            // Session sequence number resolution
            resolution: AtomicZInt::new(resolution),
            // The default batchsize
            batchsize: AtomicUsize::new(batchsize),
            // The links associated to this transport
            links: RwLock::new(HashSet::new()),
            // The transport has no links or not
            num_links: AtomicUsize::new(0),
            // Conduits
            conduits: RwLock::new(HashMap::new())
        }
    }

    /*************************************/
    /*          INITIALIZATION           */
    /*************************************/
    pub(crate) fn init_session(&self, session: Weak<SessionInner>) {
        *self.session.try_write().unwrap() = Some(session);
    }

    pub(crate) fn init_callback(&self, callback: Arc<dyn MsgHandler + Send + Sync>) {
        *self.callback.try_write().unwrap() = Some(callback);
        self.has_callback.store(true, Ordering::Relaxed);
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
    pub(crate) fn set_resolution(&self, resolution: ZInt) {
        self.resolution.store(resolution, Ordering::Relaxed);
        // @TODO: set the resolution on the conduits
    }

    pub(crate) fn get_resolution(&self) -> ZInt {
        self.resolution.load(Ordering::Relaxed)
    }

    #[allow(dead_code)]
    pub(crate) fn set_batchsize(&self, size: usize) {
        self.batchsize.store(size, Ordering::Relaxed);
    }

    pub(crate) fn get_batchsize(&self) -> usize {
        self.batchsize.load(Ordering::Relaxed)
    }

    pub(crate) fn has_callback(&self) -> bool {
        self.has_callback.load(Ordering::Relaxed)
    }

    pub(crate) fn has_links(&self) -> bool {
        self.num_links.load(Ordering::Relaxed) > 0
    }

    pub(crate) fn num_links(&self) -> usize {
        self.num_links.load(Ordering::Relaxed)
    }

    pub(crate) async fn set_callback(&self, callback: Arc<dyn MsgHandler + Send + Sync>) {
        let mut guard = zasyncwrite!(self.callback);
        self.has_callback.store(true, Ordering::Relaxed);
        for (_, c) in zasyncread!(self.conduits).iter() {
            c.set_callback(callback.clone()).await;
        }
        *guard = Some(callback);
    }

    pub(crate) async fn get_links(&self) -> Vec<Link> {
        zasyncread!(self.links).iter().cloned().collect()
    }

    /*************************************/
    /*            CONDUITS               */
    /*************************************/
    pub(crate) async fn get_or_new_conduit(&self, id: ZInt) -> Arc<Conduit> {
        loop {
            match self.get_conduit(id).await {
                Ok(conduit) => return conduit,
                Err(_) => match self.new_conduit(id).await {
                    Ok(conduit) => return conduit,
                    Err(_) => continue
                }
            }
        }
    }

    pub(crate) async fn get_conduit(&self, id: ZInt) -> ZResult<Arc<Conduit>> {
        match zasyncread!(self.conduits).get(&id) {
            Some(conduit) => Ok(conduit.clone()),
            None => Err(zerror!(ZErrorKind::Other {
                descr: format!("Conduit {} not found", id)
            }))
        }
    }

    pub(crate) async fn new_conduit(&self, id: ZInt) -> ZResult<Arc<Conduit>> {
        let session = if let Some(session) = zasyncread!(self.session).as_ref() {
            session.clone()
        } else {
            panic!("Session is unitialized");
        };

        // Get the perameters from the transport
        let resolution = self.get_resolution();
        let batchsize = self.get_batchsize();

        let conduit = {
            // Acquire the lock on the conduits
            let mut w_guard = zasyncwrite!(self.conduits);
            if w_guard.contains_key(&id) {
                return Err(zerror!(ZErrorKind::Other {
                    descr: format!("Conduit {} already exists", id)
                }));
            }
            // Add the Tx conduit
            let conduit = Arc::new(Conduit::new(id, resolution, batchsize, session));
            // Add the conduit to the HashMap
            w_guard.insert(id, conduit.clone());

            conduit
        };

        // Init the conduit callback if set
        if let Some(callback) = zasyncread!(self.callback).as_ref() {
            conduit.set_callback(callback.clone()).await;
        }

        // Add all the available links to the conduit
        // @TODO: perform an intelligent assocation of links to conduits
        let links = zasyncread!(self.links);
        if !links.is_empty() {
            for l in links.iter() {
                let _ = conduit.add_link(l.clone()).await;
            }
            Conduit::start(&conduit).await;
        }

        Ok(conduit)
    }

    #[allow(dead_code)]
    pub(crate) async fn del_conduit(&self, id: ZInt) -> ZResult<Arc<Conduit>> {
        // Remove the conduit
        if let Some(cond) = zasyncwrite!(self.conduits).remove(&id) {
            // If the conduit was present, stop it
            cond.stop().await;
            return Ok(cond)
        }

        Err(zerror!(ZErrorKind::Other {
            descr: format!("Conduit {} can not be deleted becasue it was not found", id)
        }))
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
            self.num_links.fetch_add(1, Ordering::Relaxed);
        }
        // Conduits
        {
            // Acquire the lock on the conduits
            let c_guard = zasyncread!(self.conduits);
            // Add the link to the conduits
            // @TODO: Enable a smart allocation of links across the various conduits
            //        The link is added to all the conduits for the time being
            for (_, conduit) in c_guard.iter() {
                let _ = conduit.add_link(link.clone()).await;
                Conduit::start(&conduit).await;
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
        self.num_links.fetch_sub(1, Ordering::Relaxed);

        Ok(())
    }

    pub(crate) async fn link_err(&self, link: &Link) {
        if let Some(session) = zasyncread!(self.session).as_ref() {
            if let Some(session) = session.upgrade() {
                let _ = session.del_link(link).await;
            } 
        } 
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
            if let Ok(conduit) = self.new_conduit(message.inner.cid).await {
                conduit.queue.push(message, priority).await
            }
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
            if let Ok(conduit) = self.new_conduit(cid).await {
                conduit.queue.push_batch(messages, priority).await;
            }
        }
    }

    /*************************************/
    /*   MESSAGE RECEIVED FROM THE LINK  */
    /*************************************/
    pub async fn receive_message(&self, link: &Link, message: Message) -> Action {
        let conduit = self.get_or_new_conduit(message.cid).await;
        // Process the message
        conduit.receive_message(link, message).await

    }

    /*************************************/
    /*        CLOSE THE TRANSPORT        */
    /*************************************/
    pub(crate) async fn close(&self) -> ZResult<()> {
        // Notify the callback
        if let Some(callback) = zasyncread!(self.callback).as_ref() {
            callback.close().await;
        }

        // Stop the conduits
        for (_, conduit) in zasyncwrite!(self.conduits).drain().take(1) {
            conduit.stop().await;
        }

        // Close all the links
        for l in zasyncwrite!(self.links).drain().take(1) {
            let _ = l.close().await;
        }

        Ok(())
    }
}

// impl Drop for Transport {
//     fn drop(&mut self) {
//         println!("Dropped transport {:?}", self);
//     }
// }

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
