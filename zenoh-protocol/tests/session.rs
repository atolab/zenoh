use async_std::sync::{
    Arc,
    channel
};
use async_std::task;
use zenoh_protocol::proto::Locator;
use zenoh_protocol::session::{
    EmptyCallback,
    SessionManager
};


async fn run(locator: Locator) {
    let (t_sender, t_receiver) = channel::<()>(1);
    let (r_sender, r_receiver) = channel::<()>(1);

    let l = locator.clone();
    task::spawn(async move {
        // Create the routing table
        let routing = Arc::new(EmptyCallback::new());

        // Create the transport session manager
        let manager = Arc::new(SessionManager::new(routing.clone()));

        // Initialize the transport session manager
        manager.initialize(&manager).await;

        // Limit the number of connections to 1 for each listener
        let limit = Some(1);

        // Create the listeners
        match manager.new_listener(&l, limit).await {
            Ok(_) => (),
            Err(_) => ()
        }

        // Wait to be notified by the client
        r_receiver.recv().await;

        // Stop the listener
        let res = manager.del_listener(&l).await;
        assert_eq!(res.is_ok(), true);

        // Notify the main task
        t_sender.send(()).await;
    });

    // Client task
    let l = locator.clone();
    task::spawn(async move {
        // Create the routing table
        let client = Arc::new(EmptyCallback::new());

        // Create the transport session manager
        let manager = Arc::new(SessionManager::new(client.clone()));

        // Initialize the transport session manager
        manager.initialize(&manager).await;

        // Open session -> This should be accepted
        let res1 = manager.open_session(&l).await;
        assert_eq!(res1.is_ok(), true);

        // Open session -> This should be rejected
        let res2 = manager.open_session(&l).await;
        assert_eq!(res2.is_err(), true);

        // Close the open session
        let session = res1.unwrap();
        let res3 = manager.close_session(session.get_id(), None).await;
        assert_eq!(res3.is_ok(), true);

        // Notify the router we are done
        r_sender.send(()).await;
    });

    t_receiver.recv().await;
}

#[test]
fn session_tcp() {
    let locator: Locator = "tcp/127.0.0.1:8888".parse().unwrap();
    task::block_on(run(locator));
}