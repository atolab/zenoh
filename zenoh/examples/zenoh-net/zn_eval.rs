use std::env;
use std::io::Read;
use async_std::task;
use zenoh::net::*;
use zenoh::net::ResKey::*;

fn main() {
    task::block_on( async {
        let mut args: Vec<String> = env::args().collect();

        let mut options = args.drain(1..);
        let uri     = options.next().unwrap_or("/demo/example/zenoh-rust-eval".to_string());
        let locator = options.next().unwrap_or("".to_string());

        println!("Openning session...");
        let session = open(&locator, None).await.unwrap();

        // We want to use uri in query_handler closure.
        // But as this closure must take the ownership, we clone uri as rname.
        let rname = uri.clone();
        let query_handler = move |res_name: &str, predicate: &str, replies_sender: &RepliesSender, query_handle: QueryHandle| {
            println!(">> [Query handler] Handling '{}?{}'", res_name, predicate);
            let data: RBuf = "Eval from Rust!".as_bytes().into();
            let result: Vec<(&str, RBuf)> = [(&rname[..], data)].to_vec();
            (*replies_sender)(query_handle, result);
        };

        println!("Declaring Queryable on {}", uri);
        let queryable = session.declare_queryable(&RName(uri), query_handler).await.unwrap();

        let mut reader = std::io::stdin();
        let mut input = [0u8];
        while input[0] != 'q' as u8 {
            reader.read_exact(&mut input).unwrap();
        }

        session.undeclare_queryable(queryable).await.unwrap();
        session.close().await.unwrap();
    })
}
