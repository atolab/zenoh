use std::env;
use std::io::Read;
use zenoh::net::*;
use zenoh::net::ResourceKey::*;


fn main() {
    let args: Vec<String> = env::args().collect();

    let uri     = args.get(1).map_or("/demo/example/zenoh-rust-eval", |s| &s);
    let locator = args.get(2).map_or("", |s| &s);

    println!("Openning session...");
    let session = open(locator, None).unwrap();

    // We want to use uri in query_handler closure.
    // But as this closure must take the ownership, we clone uri as rname.
    let rname = uri.to_string();
    let query_handler = move |res_name: &str, predicate: &str, replies_sender: &RepliesSender, query_handle: QueryHandle| {
        println!(">> [Query handler] Handling '{}?{}'", res_name, predicate);
        let data = "Eval from Rust!".as_bytes().to_vec();
        let result: Vec<(&str, Vec<u8>)> = [(&rname[..], data)].to_vec();
        (*replies_sender)(query_handle, result);
    };

    println!("Declaring Eval on {}", uri);
    let eval = session.declare_eval(&RName(uri), query_handler).unwrap();

    let mut reader = std::io::stdin();
    let mut input = [0u8];
    while input[0] != 'q' as u8 {
        reader.read_exact(&mut input).unwrap();
    }

    session.undeclare_eval(eval).unwrap();
    session.close().unwrap();
}
