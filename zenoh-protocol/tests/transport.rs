use async_std::sync::{
    Arc,
    Barrier,
    channel,
    Mutex
};
use async_std::task;
use async_trait::async_trait;
use std::sync::atomic::Ordering;
use std::time::Duration;

use zenoh_protocol::core::{
    AtomicZInt,
    PeerId,
    ResKey,
    ZInt,
    ZResult
};
use zenoh_protocol::io::ArcSlice;
use zenoh_protocol::link::{
    Link,
    LinkDummy
};
use zenoh_protocol::proto::{
    Body,
    Message,
    MessageKind,
    WhatAmI
};
use zenoh_protocol::session::{
    MsgHandler,
    SessionHandler,
    SessionManager
};


// Session Handler for the router
struct SHRouter {
    session: Mutex<Vec<Arc<SCRouter>>>
}

impl SHRouter {
    fn new() -> Self {
        Self {
            session: Mutex::new(Vec::new())
        }
    }
}

#[async_trait]
impl SessionHandler for SHRouter {
    async fn new_session(&self, _session: Arc<dyn MsgHandler + Send + Sync>) -> Arc<dyn MsgHandler + Send + Sync> {
        let arc = Arc::new(SCRouter::new());
        self.session.lock().await.push(arc.clone());
        arc
    }
}

// Session Callback for the router
pub struct SCRouter {
    count: AtomicZInt,
    next: AtomicZInt
}

impl SCRouter {
    pub fn new() -> Self {
        Self {
            count: AtomicZInt::new(0),
            next: AtomicZInt::new(0)
        }
    }
}

#[async_trait]
impl MsgHandler for SCRouter {
    async fn handle_message(&self, message: Message) -> ZResult<()> {
        self.count.fetch_add(1, Ordering::SeqCst);
        match message.get_body() {
            Body::Data{reliable: _,  sn, key: _, info: _, payload: _} |
            Body::Declare{sn, declarations: _} |
            Body::Pull{sn, key: _, pull_id: _, max_samples: _} |
            Body::Query{sn, key: _, predicate: _, qid: _, target: _, consolidation: _} => {
                // println!("EXPECTED {}, RECEIVED {}", self.next.load(Ordering::SeqCst), sn);
                let gap = sn.wrapping_sub(self.next.load(Ordering::SeqCst));
                assert!(gap < ZInt::max_value()/2);
                self.next.store(sn+1, Ordering::SeqCst);
            },
            _ => {}
        }
        Ok(())
    }

    async fn close(&self) {}
}


// Session Handler for the client
struct SHClient {}

impl SHClient {
    fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl SessionHandler for SHClient {
    async fn new_session(&self, _session: Arc<dyn MsgHandler + Send + Sync>) -> Arc<dyn MsgHandler + Send + Sync> {
        Arc::new(SCClient::new())
    }
}

// Session Callback for the client
pub struct SCClient {
    count: AtomicZInt
}

impl SCClient {
    pub fn new() -> Self {
        Self {
            count: AtomicZInt::new(0) 
        }
    }
}

#[async_trait]
impl MsgHandler for SCClient {
    async fn handle_message(&self, _message: Message) -> ZResult<()> {
        self.count.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    async fn close(&self) {}
}


async fn transport_base_inner() {
    let m_barrier = Arc::new(Barrier::new(3));
    let b_router = Arc::new(Barrier::new(2));
    let b_client = Arc::new(Barrier::new(2));

    // Define client IDs and Dummy Addresses
    let client_id = PeerId{id: vec![0u8]};
    let client_addr = "client".to_string();
    let router_id = PeerId{id: vec![1u8]};
    let router_addr = "router".to_string();

    // Create the channels for the dummy links
    let (client_sender, client_receiver) = channel::<Message>(1);
    let (router_sender, router_receiver) = channel::<Message>(1);

    // Reliable messages to send
    let messages_count: ZInt = 1000;

    // Router task
    let c_mbr = m_barrier.clone();
    let c_cbr = b_router.clone();
    let c_rbr = b_client.clone();
    let c_client_id = client_id.clone();
    let c_client_addr = client_addr.clone();
    let c_client_sender = client_sender.clone();
    let c_router_id = router_id.clone();
    let c_router_addr = router_addr.clone();
    task::spawn(async move {
        // Create the router session handler
        let routing = Arc::new(SHRouter::new());

        // Create the transport session manager
        let version = 0u8;
        let whatami = WhatAmI::Router;
        let id = c_router_id;
        let lease = 60;    
        let manager = SessionManager::new(version, whatami, id, lease, routing.clone());

        // Create an empty session with the client
        let session = manager.init_session(&c_client_id).await.unwrap();
        // Manually create a dummy link
        let link_inner = Arc::new(LinkDummy::new(
            c_router_addr, c_client_addr, router_receiver, c_client_sender, 
            session.get_transport()
        ));
        let link = Link::Dummy(link_inner.clone());
        link.start();
        // Manually add the link to the session
        session.add_link(link).await.unwrap();

        // Notify the client
        c_rbr.wait().await;
        // Wait for the client
        c_cbr.wait().await;

        // Wait for the client
        c_cbr.wait().await;

        let count = routing.session.lock().await.get(0).unwrap().count.load(Ordering::SeqCst);
        assert_eq!(count, messages_count);
        let next = routing.session.lock().await.get(0).unwrap().next.load(Ordering::SeqCst);
        assert_eq!(count, next);

        link_inner.set_dropping_probability(0.5).await;

        // Notify the client
        c_cbr.wait().await;

        link_inner.set_dropping_probability(0.0).await;
        link_inner.set_reordering_probability(0.5).await;

        // Notify the client
        c_cbr.wait().await;

        c_mbr.wait().await;
    });

    // Client task
    let c_mbr = m_barrier.clone();
    let c_cbr = b_router.clone();
    let c_rbr = b_client.clone();
    let c_client_id = client_id.clone();
    let c_client_addr = client_addr.clone();
    let c_router_id = router_id.clone();
    let c_router_addr = router_addr.clone();
    let c_router_sender = router_sender.clone();
    task::spawn(async move {
        // Create the client session handler
        let client = Arc::new(SHClient::new());

        // Create the transport session manager
        let version = 0u8;
        let whatami = WhatAmI::Client;
        let id = c_client_id;
        let lease = 60;
        let manager = SessionManager::new(version, whatami, id, lease, client);

        // Create an empty session with the client
        let session = manager.init_session(&c_router_id).await.unwrap();
        // Manually create a dummy link
        let link_inner = Arc::new(LinkDummy::new(
            c_client_addr, c_router_addr, client_receiver, c_router_sender, 
            session.get_transport()
        ));
        let link = Link::Dummy(link_inner.clone());
        link.start();
        // Manually add the link to the session
        session.add_link(link).await.unwrap();

        // Notify the router
        c_rbr.wait().await;
        // Wait for the router
        c_cbr.wait().await;

        // Send reliable messages
        let kind = MessageKind::FullMessage;
        let reliable = false; // This will need to be changed at true
        let sn = 0;
        let key = ResKey::ResName{ name: "test".to_string() };
        let info = None;
        let payload = ArcSlice::new(Arc::new(vec![0u8; 1]), 0, 1);
        let reply_context = None;
        let cid = None;
        let properties = None;

        let message = Message::make_data(kind, reliable, sn, key, info, payload, reply_context, cid, properties);

        // Send the messages, no dropping or reordering in place
        for _ in 0..messages_count { 
            session.schedule(message.clone(), None).await;
        }

        // Leave some time for the messages to arrive
        task::sleep(Duration::from_millis(100)).await;

        // Wait for the router
        c_cbr.wait().await;

        // Send unreliable messages
        let kind = MessageKind::FullMessage;
        let reliable = false; 
        let sn = 0;
        let key = ResKey::ResName{ name: "test".to_string() };
        let info = None;
        let payload = ArcSlice::new(Arc::new(vec![0u8; 1]), 0, 1);
        let reply_context = None;
        let cid = None;
        let properties = None;

        let message = Message::make_data(kind, reliable, sn, key, info, payload, reply_context, cid, properties);

        // Send again the messages, this time they will randomly dropped
        for _ in 0..messages_count { 
            session.schedule(message.clone(), None).await;
        }

        // Leave some time for the messages to arrive
        task::sleep(Duration::from_millis(100)).await;

        // Wait for the router
        c_cbr.wait().await;

        // Send again the messages, this time the sn will be randomly changed
        for _ in 0..messages_count { 
            session.schedule(message.clone(), None).await;
        }

        // Leave some time for the messages to arrive
        task::sleep(Duration::from_millis(100)).await;

        // Wait for the router
        c_cbr.wait().await;

        c_mbr.wait().await;
    });

    m_barrier.wait().await;
}

#[test]
fn transport_base() {
    task::block_on(transport_base_inner());
}