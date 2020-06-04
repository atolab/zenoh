use std::env;
use async_std::prelude::*;
use async_std::task;
use zenoh::net::*;
use zenoh::net::queryable::EVAL;

fn main() {
    // for logging
    env_logger::init();

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
            let result: Vec<(String, RBuf)> = [(rname.clone(), data)].to_vec();
            (*replies_sender)(query_handle, result);
        };

        println!("Declaring Queryable on {}", uri);
        let queryable = session.declare_queryable(&uri.into(), EVAL, query_handler).await.unwrap();

        let mut stdin = async_std::io::stdin();
        let mut input = [0u8];
        while input[0] != 'q' as u8 {
            stdin.read_exact(&mut input).await.unwrap();
        }

        session.undeclare_queryable(queryable).await.unwrap();
        session.close().await.unwrap();
    })
}
