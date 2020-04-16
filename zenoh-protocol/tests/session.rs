use async_std::sync::{
    Arc,
    Barrier
};
use async_std::task;
use async_trait::async_trait;
use std::time::Duration;

use zenoh_protocol::core::PeerId;
use zenoh_protocol::link::Locator;
use zenoh_protocol::proto::WhatAmI;
use zenoh_protocol::session::{
    DummyHandler,
    MsgHandler,
    SessionHandler,
    SessionManager,
    SessionManagerConfig
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
    async fn new_session(&self, _whatami: WhatAmI, _session: Arc<dyn MsgHandler + Send + Sync>) -> Arc<dyn MsgHandler + Send + Sync> {
        Arc::new(DummyHandler::new())
    }
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
        Arc::new(DummyHandler::new())
    }
}


async fn run(locator: Locator) {
    let m_barrier = Arc::new(Barrier::new(2));
    let b_router = Arc::new(Barrier::new(2));
    let b_client = Arc::new(Barrier::new(2));

    let client_id = PeerId{id: vec![0u8]};
    let router_id = PeerId{id: vec![1u8]};

    // Router task
    let c_mbr = m_barrier.clone();
    let c_cbr = b_router.clone();
    let c_rbr = b_client.clone();
    let c_loc = locator.clone();
    let c_cid = client_id.clone();
    let c_rid = router_id.clone();
    task::spawn(async move {
        // Create the router session handler
        let routing = Arc::new(SHRouter::new());

        // Create the transport session manager
        let config = SessionManagerConfig {
            version: 0,
            whatami: WhatAmI::Router,
            id: c_rid,
            handler: routing,
            lease: None,
            resolution: None,
            batchsize: None,
            timeout: None
        };
        let manager = SessionManager::new(config);

        // Limit the number of connections to 1 for each listener
        // Not implemented at the moment
        let limit = Some(1);

        // Create the listeners
        let res = manager.add_locator(&c_loc, limit).await; 
        assert_eq!(res.is_ok(), true);
        assert_eq!(manager.get_locators().await.len(), 1);

        // Wait for the client
        c_cbr.wait().await;

        let sessions = manager.get_sessions().await;
        assert_eq!(sessions.len(), 1);
        let ses1 = &sessions[0];
        assert_eq!(ses1.get_peer(), c_cid);
        assert_eq!(ses1.get_links().await.len(), 1);

        // Notify the client
        c_rbr.wait().await;
        // Wait for the client
        c_cbr.wait().await;

        let sessions = manager.get_sessions().await;
        assert_eq!(sessions.len(), 1);
        let ses2 = &sessions[0];
        assert_eq!(ses1, ses2);
        assert_eq!(ses2.get_links().await.len(), 2);

        // Notify the client
        c_rbr.wait().await;
        // Wait for the client
        c_cbr.wait().await;

        // Wait 10ms to give the time to the close message
        // sent by client to arrive at the router and close
        // the actual session
        task::sleep(Duration::from_millis(10)).await;
        let sessions = manager.get_sessions().await;
        assert_eq!(sessions.len(), 0);

        // Stop the listener
        let res = manager.del_locator(&c_loc).await;
        assert_eq!(res.is_ok(), true);
        assert_eq!(manager.get_locators().await.len(), 0);

        // Notify the main task
        c_mbr.wait().await;
    });

    // Client task
    let c_cbr = b_router.clone();
    let c_rbr = b_client.clone();
    let c_loc = locator.clone();
    let c_cid = client_id.clone();
    let c_rid = router_id.clone();
    task::spawn(async move {
        // Create the client session handler
        let client = Arc::new(SHClient::new());

        // Create the transport session manager
        let config = SessionManagerConfig {
            version: 0,
            whatami: WhatAmI::Client,
            id: c_cid,
            handler: client,
            lease: None,
            resolution: None,
            batchsize: None,
            timeout: None
        };
        let manager = SessionManager::new(config);

        // Open session -> This should be accepted
        let res1 = manager.open_session(&c_loc).await;
        assert_eq!(res1.is_ok(), true);
        let ses1 = res1.unwrap();
        assert_eq!(manager.get_sessions().await.len(), 1);
        assert_eq!(ses1.get_peer(), c_rid);
        assert_eq!(ses1.get_links().await.len(), 1);

        // Notify the router
        c_cbr.wait().await;
        // Wait for the router
        c_rbr.wait().await;

        // Open session -> This should be accepted
        let res2 = manager.open_session(&c_loc).await;
        assert_eq!(res2.is_ok(), true);
        let ses2 = res2.unwrap();
        assert_eq!(manager.get_sessions().await.len(), 1);
        assert_eq!(ses1, ses2);
        assert_eq!(ses2.get_links().await.len(), 2);

        // Notify the router 
        c_cbr.wait().await;
        // Wait for the router
        c_rbr.wait().await;
        
        // Close the open session
        let res3 = ses1.close().await;
        assert_eq!(res3.is_ok(), true);
        assert_eq!(manager.get_sessions().await.len(), 0);

        // Notify the router 
        c_cbr.wait().await;
    });

    m_barrier.wait().await;
}

#[test]
fn session_tcp() {
    let locator: Locator = "tcp/127.0.0.1:8888".parse().unwrap();
    task::block_on(run(locator));
}