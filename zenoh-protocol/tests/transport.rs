use async_std::sync::{
    Arc,
    Barrier,
    Mutex
};
use async_std::task;
use async_trait::async_trait;
use std::sync::atomic::Ordering;

use zenoh_protocol::core::{
    AtomicZInt,
    PeerId,
    ResKey,
    ZInt,
    ZResult
};
use zenoh_protocol::io::RBuf;
use zenoh_protocol::link::Locator;
use zenoh_protocol::proto::{
    Body,
    Message,
    MessageKind,
    SeqNum,
    WhatAmI
};
use zenoh_protocol::session::{
    MsgHandler,
    SessionHandler,
    SessionManager, 
    SEQ_NUM_RESOLUTION
};
use zenoh_util::zasynclock;


// Session Handler for the router
struct SHRouter {
    session: Mutex<Vec<Arc<SCRouter>>>,
    resolution: ZInt
}

impl SHRouter {
    fn new(resolution: ZInt) -> Self {
        Self {
            session: Mutex::new(Vec::new()),
            resolution
        }
    }
}

#[async_trait]
impl SessionHandler for SHRouter {
    async fn new_session(&self, _whatami: WhatAmI, _session: Arc<dyn MsgHandler + Send + Sync>) -> Arc<dyn MsgHandler + Send + Sync> {
        let arc = Arc::new(SCRouter::new(self.resolution));
        self.session.lock().await.push(arc.clone());
        arc
    }
}

// Session Callback for the router
pub struct SCRouter {
    count: AtomicZInt,
    last_reliable: Mutex<SeqNum>,
    last_unreliable: Mutex<SeqNum>
}

impl SCRouter {
    pub fn new(resolution: ZInt) -> Self {
        Self {
            count: AtomicZInt::new(0),
            last_reliable: Mutex::new(SeqNum::make(resolution-1, resolution).unwrap()),
            last_unreliable: Mutex::new(SeqNum::make(resolution-1, resolution).unwrap())
        }
    }
}

#[async_trait]
impl MsgHandler for SCRouter {
    async fn handle_message(&self, message: Message) -> ZResult<()> {
        self.count.fetch_add(1, Ordering::AcqRel);
        let is_reliable = message.is_reliable();
        match message.get_body() {
            Body::Data{sn, ..} |
            Body::Declare{sn, ..} |
            Body::Pull{sn, ..} |
            Body::Query{sn, ..} => {
                let mut l = if is_reliable {
                    zasynclock!(self.last_reliable)
                } else {
                    zasynclock!(self.last_unreliable)
                };
                assert!(l.precedes(*sn));
                l.set(*sn).unwrap();
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
    async fn new_session(&self, _whatami: WhatAmI, _session: Arc<dyn MsgHandler + Send + Sync>) -> Arc<dyn MsgHandler + Send + Sync> {
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
    let t_barrier = Arc::new(Barrier::new(2));

    // Define the locator
    let locator: Locator = "tcp/127.0.0.1:8888".parse().unwrap();

    // Define client and router IDs
    let client_id = PeerId{id: vec![0u8]};
    let router_id = PeerId{id: vec![1u8]};

    // Reliable messages to send
    let messages_count: ZInt = 1_000;

    // Router task
    let c_mbr = m_barrier.clone();
    let c_tbr = t_barrier.clone();
    let c_loc = locator.clone();
    let c_router_id = router_id.clone();
    task::spawn(async move {
        // Create the router session handler
        let routing = Arc::new(SHRouter::new(SEQ_NUM_RESOLUTION));

        // Create the transport session manager
        let version = 0u8;
        let whatami = WhatAmI::Router;
        let id = c_router_id;
        let lease = 60;    
        let manager = SessionManager::new(version, whatami, id, lease, routing.clone());

        // Limit the number of connections to 1 for each listener
        // Not implemented at the moment
        let limit = Some(1);
        // Create the listener
        let res = manager.add_locator(&c_loc, limit).await; 
        assert!(res.is_ok());

        // Notify the client
        c_tbr.wait().await;

        // Wait for the end of the client
        c_mbr.wait().await;
    });

    // Client task
    let c_mbr = m_barrier.clone();
    let c_tbr = t_barrier.clone();
    let c_loc = locator.clone();
    let c_client_id = client_id.clone();
    task::spawn(async move {
        // Create the client session handler
        let client = Arc::new(SHClient::new());

        // Create the transport session manager
        let version = 0u8;
        let whatami = WhatAmI::Client;
        let id = c_client_id;
        let lease = 60;
        let manager = SessionManager::new(version, whatami, id, lease, client);

        // Wait for the router
        c_tbr.wait().await;

        // Create an empty session with the client
        // Open session -> This should be accepted
        let res = manager.open_session(&c_loc).await;
        assert_eq!(res.is_ok(), true);
        let session = res.unwrap();

        // Send reliable messages
        let kind = MessageKind::FullMessage;
        let reliable = true;
        let sn = 0;
        let key = ResKey::RName("test".to_string());
        let info = None;
        let payload = RBuf::from(vec![0u8; 1]);
        let reply_context = None;
        let cid = None;
        let properties = None;

        let message = Message::make_data(kind, reliable, sn, key, info, payload, reply_context, cid, properties);

        // Send the messages, no dropping or reordering in place
        for _ in 0..messages_count { 
            session.schedule(message.clone(), None).await;
        }

        // Send unreliable messages
        let kind = MessageKind::FullMessage;
        let reliable = false; 
        let sn = 0;
        let key = ResKey::RName("test".to_string());
        let info = None;
        let payload = RBuf::from(vec![0u8; 1]);
        let reply_context = None;
        let cid = None;
        let properties = None;

        let message = Message::make_data(kind, reliable, sn, key, info, payload, reply_context, cid, properties);

        // Send again the messages, this time they will randomly dropped
        for _ in 0..messages_count { 
            session.schedule(message.clone(), None).await;
        }

        c_mbr.wait().await;
    });

    m_barrier.wait().await;
}

#[test]
fn transport_base() {
    task::block_on(transport_base_inner());
}