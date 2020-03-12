use async_std::sync::{
    Arc,
    Barrier,
    channel,
    Sender,
    Receiver
};
use async_std::task;
use async_trait::async_trait;

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
struct SHRouter {}

impl SHRouter {
    fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl SessionHandler for SHRouter {
    async fn new_session(&self, _session: Arc<dyn MsgHandler + Send + Sync>) -> Arc<dyn MsgHandler + Send + Sync> {
        Arc::new(SCRouter::new())
    }
}

// Session Callback for the router
pub struct SCRouter {
    next: AtomicZInt
}

impl SCRouter {
    pub fn new() -> Self {
        Self {
            next: AtomicZInt::new(0)
        }
    }
}

#[async_trait]
impl MsgHandler for SCRouter {
    async fn handle_message(&self, _message: Message) -> ZResult<()> {
        Ok(())
    }

    async fn close(&self) {}
}


// Session Handler for the client
struct SHClient {
    next: AtomicZInt
}

impl SHClient {
    fn new() -> Self {
        Self {
            next: AtomicZInt::new(0)
        }
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
    next: AtomicZInt
}

impl SCClient {
    pub fn new() -> Self {
        Self {
            next: AtomicZInt::new(0) 
        }
    }
}

#[async_trait]
impl MsgHandler for SCClient {
    async fn handle_message(&self, _message: Message) -> ZResult<()> {
        Ok(())
    }

    async fn close(&self) {}
}


async fn transport_base_inner() {
    let m_barrier = Arc::new(Barrier::new(3));

    // Define client IDs and Dummy Addresses
    let client_id = PeerId{id: vec![0u8]};
    let client_addr = "client".to_string();
    let router_id = PeerId{id: vec![1u8]};
    let router_addr = "router".to_string();

    // Create the channels for the dummy links
    let (client_sender, client_receiver) = channel::<Message>(1);
    let (router_sender, router_receiver) = channel::<Message>(1);

    // Router task
    let c_mbr = m_barrier.clone();
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
        let manager = SessionManager::new(version, whatami, id, lease, routing);

        // Create an empty session with the client
        let session = manager.init_session(&c_client_id).await.unwrap();
        // Manually create a dummy link
        let link = Link::Dummy(Arc::new(LinkDummy::new(
            c_router_addr, c_client_addr, router_receiver, c_client_sender, session.get_transport())
        ));
        link.start();
        // Manually add the link to the session
        session.add_link(link).await.unwrap();

        c_mbr.wait().await;
    });

    // Client task
    let c_mbr = m_barrier.clone();
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
        let link = Link::Dummy(Arc::new(LinkDummy::new(
            c_client_addr, c_router_addr, client_receiver, c_router_sender, session.get_transport())
        ));
        link.start();
        // Manually add the link to the session
        session.add_link(link).await.unwrap();

        // Send a message
        let kind = MessageKind::FullMessage;
        let reliable = true;
        let sn = 0;
        let key = ResKey::ResName{ name: "test".to_string() };
        let info = None;
        let payload = ArcSlice::new(Arc::new(vec![0u8; 1]), 0, 1);
        let reply_context = None;
        let cid = None;
        let properties = None;

        let message = Message::make_data(kind, reliable, sn, key, info, payload, reply_context, cid, properties);
        let res = session.send(message).await;
        assert!(res.is_ok());

        println!("FATTO");
        c_mbr.wait().await;
    });

    m_barrier.wait().await;
}

#[test]
fn transport_base() {
    task::block_on(transport_base_inner());
}