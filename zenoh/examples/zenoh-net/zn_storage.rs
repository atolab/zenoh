use std::env;
use std::io::Read;
use std::collections::HashMap;
use async_std::task;
use async_std::sync::Arc;
use spin::RwLock;
use zenoh::net::*;
use zenoh::net::ResKey::*;
use zenoh_protocol::proto::ReplySource;

fn main() {
    // for logging
    env_logger::init();

    task::block_on( async {
        let mut args: Vec<String> = env::args().collect();

        let mut options = args.drain(1..);
        let uri     = options.next().unwrap_or("/demo/example/**".to_string());
        let locator = options.next().unwrap_or("".to_string());

        // Create a HashMap to store the keys/values receveid in data_handler closure.
        // As this map has to be used also in query_handler closure, we need to wrap it
        // in a Arc<RwLock<T>>. Each closure will own a copy of this Arc.
        let stored: Arc<RwLock<HashMap<String, RBuf>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let stored_shared = stored.clone();

        let data_handler = move |res_name: &str, payload: RBuf, _data_info: DataInfo| {
            println!(">> [Subscription listener] Received ('{}': '{}')", res_name, payload);
            stored.write().insert(res_name.into(), payload);
        };

        let query_handler = move |res_name: &str, predicate: &str, replies_sender: &RepliesSender, query_handle: QueryHandle| {
            println!(">> [Query handler   ] Handling '{}?{}'", res_name, predicate);
            let mut result: Vec<(String, RBuf)> = Vec::new();
            let ref st = stored_shared.read();
            for (rname, data) in st.iter() {
                if rname_intersect(res_name, rname) {
                    result.push((rname.to_string(), data.clone()));
                }
            }
            println!(">> Returning: {:?}", result);
            (*replies_sender)(query_handle, result);
        };


        println!("Openning session...");
        let session = open(&locator, None).await.unwrap();

        let sub_info = SubInfo {
            reliability: Reliability::Reliable,
            mode: SubMode::Push,
            period: None
        };

        println!("Declaring Subscriber on {}", uri);
        let sub = session.declare_subscriber(&RName(uri.clone()), &sub_info, data_handler).await.unwrap();

        println!("Declaring Queryable on {}", uri);
        let queryable = session.declare_queryable(&RName(uri), ReplySource::Storage, query_handler).await.unwrap();

        let mut reader = std::io::stdin();
        let mut input = [0u8];
        while input[0] != 'q' as u8 {
            reader.read_exact(&mut input).unwrap();
        }

        session.undeclare_queryable(queryable).await.unwrap();
        session.undeclare_subscriber(sub).await.unwrap();
        session.close().await.unwrap();
    })
}
