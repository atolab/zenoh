use std::env;
use zenoh::net::*;
use zenoh::net::ResourceKey::*;

fn main() {
    let args: Vec<String> = env::args().collect();

    let uri     = args.get(1).map_or("/demo/example/zenoh-rs-write", |s| &s);
    let value   = args.get(2).map_or("Write from Rust!", |s| &s);
    let locator = args.get(3).map_or("", |s| &s);

    println!("Openning session...");
    let mut session = open(locator, None).unwrap();

    // Split the uri on the last '/'. 
    // The first part will be declared as resource,
    // and the second as suffix for writing using the resource.
    let mut resource_name = uri.to_string();
    let suffix = resource_name.split_off(resource_name.rfind('/').unwrap());

    println!("Declaring Resource {} ", resource_name);
    let rid = session.declare_resource(&RName(&resource_name)).unwrap();

    println!("Declaring Publisher on {}/**", resource_name);
    let publ = session.declare_publisher(&RIdWithSuffix(&rid, "/**")).unwrap();

    println!("Writing Data ('{}': '{}')...\n", uri, value);
    session.write(&RIdWithSuffix(&rid, &suffix), value.as_bytes()).unwrap();

    session.undeclare_publisher(publ).unwrap();
    session.close().unwrap();
}
