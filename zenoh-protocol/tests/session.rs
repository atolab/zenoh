use async_std::sync::Arc;
use async_std::task;
use async_trait::async_trait;
use std::error::Error;
use std::time::Duration;
use zenoh_protocol::ArcSelf;
use zenoh_protocol::core::ZError;
use zenoh_protocol::proto::{
    Locator,
    Message
};
use zenoh_protocol::session::{
    Session,
    SessionCallback,
    SessionManager
};

// Define an empty SessionCallback for the routing
struct RoutingCallback {}

impl RoutingCallback {
    fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl SessionCallback for RoutingCallback {
    async fn receive_message(&self, _message: Message) -> Result<(), ZError> {
        Ok(())
    }

    async fn new_session(&self, _session: Arc<Session>) {
    }
}

async fn router(locator: Vec<Locator>) -> Result<(), Box<dyn Error>> {
    // Create the routing table
    let routing = Arc::new(RoutingCallback::new());
    // Create the transport session manager
    let manager = Arc::new(SessionManager::new(routing.clone()));
    // Initialize the transport session manager
    manager.initialize(&manager).await;
    let limit = Some(2);
    // Create and start the listeners
    for l in locator.iter() {
        match manager.new_listener(l, limit).await {
            Ok(_) => (),
            Err(_) => ()
        }
    }
    Ok(())
}

// Define an empty SessionCallback for the client API
struct ClientCallback {}

impl ClientCallback {
    fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl SessionCallback for ClientCallback {
    async fn receive_message(&self, _message: Message) -> Result<(), ZError> {
        Ok(())
    }

    async fn new_session(&self, _session: Arc<Session>) {
    }
}

async fn client(locator: Vec<Locator>) -> Result<(), Box<dyn Error>> {
    // Create the routing table
    let client = Arc::new(ClientCallback::new());
    // Create the transport session manager
    let manager = Arc::new(SessionManager::new(client.clone()));
    // Initialize the transport session manager
    manager.initialize(&manager).await;
    // Create and start the listeners
    for l in locator.iter() {
        for _ in 0..3 {
            println!("\nConnecting to: {}", l);
            match manager.open_session(l).await {
                Ok(session) => {
                    println!("Success connecting to \"{}\" with session id: {}", l, session.get_id());
                },
                Err(e) => {
                    println!("Failure connecting to: {} {}", l, e);
                }
            }
        }
    }
    Ok(())
}

#[ignore]
#[test]
fn session() {
    let mut locator: Vec<Locator> = Vec::new();
    locator.push("tcp/127.0.0.1:8888".parse().unwrap());
    // locator.push("tcp/127.0.0.1:8889".parse().unwrap());
    // locator.push("udp/127.0.0.1:8888".parse().unwrap());

    let l_clone = locator.clone();
    let a = task::spawn(async move {
        match router(l_clone).await {
            Ok(_) => (),
            Err(e) => {
                eprintln!("{}", e);
            }
        }
        async_std::future::pending::<()>().await;
    });

    let b = task::spawn(async {
        // task::sleep(Duration::from_secs(1)).await;
        match client(locator).await {
            Ok(_) => (),
            Err(e) => {
                eprintln!("{}", e);
            }
        }
    });

    task::block_on(async {
        a.await;
        b.await;
    })
}