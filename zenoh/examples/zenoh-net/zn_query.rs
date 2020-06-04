use std::env;
use std::time::Duration;
use async_std::task;
use zenoh::net::*;


fn main() {
    // for logging
    env_logger::init();

    task::block_on( async {
        let mut args: Vec<String> = env::args().collect();

        let mut options = args.drain(1..);
        let uri     = options.next().unwrap_or("/demo/example/**".to_string());
        let locator = options.next().unwrap_or("".to_string());

        println!("Openning session...");
        let session = open(&locator, None).await.unwrap();

        let replies_handler = move |reply: &Reply| {
            match reply {
                Reply::ReplyData {reskey, payload, ..} => {println!(">> [Reply handler] received reply data {:?} : {}", 
                                                            reskey, std::str::from_utf8(&payload.to_vec()).unwrap())}
                Reply::SourceFinal {..} => {println!(">> [Reply handler] received source final.")}
                Reply::ReplyFinal => {println!(">> [Reply handler] received reply final.")}
            }
        };

        println!("Sending Query '{}'...", uri);
        let _eval = session.query(
            &uri.into(), "",
            replies_handler,
            QueryTarget::default(),
            QueryConsolidation::default()
        ).await.unwrap();

        task::sleep(Duration::from_secs(1)).await;

        session.close().await.unwrap();
    })
}
