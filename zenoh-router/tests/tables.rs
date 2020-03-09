use async_std::sync::Arc;
use std::convert::TryInto;
use zenoh_protocol::core::rname::intersect;
use zenoh_protocol::proto::Mux;
use zenoh_protocol::session::DummyHandler;
use zenoh_router::routing::tables::Tables;
use zenoh_router::routing::resource::Resource;

#[test]
fn base_test() {
    let tables = Tables::new();
    let primitives = Arc::new(Mux::new(Arc::new(DummyHandler::new())));
    let sex = Tables::declare_session(&tables, primitives.clone());
    Tables::declare_resource(&tables, &sex, 1, 0, "/one/two/three");
    Tables::declare_resource(&tables, &sex, 2, 0, "/one/deux/trois");
    
    Tables::declare_subscription(&tables, &sex, 1, "/four/five");

    Tables::print(&tables);
}

#[test]
fn match_test() {
    let rnames = [
        "/", "/a", "/a/", "/a/b", "/*", "/abc", "/abc/", "/*/", "xxx", 
        "/ab*", "/abcd", "/ab*d", "/ab", "/ab/*", "/a/*/c/*/e", "/a/b/c/d/e", 
        "/a/*b/c/*d/e", "/a/xb/c/xd/e", "/a/c/e", "/a/b/c/d/x/e", "/ab*cd", 
        "/abxxcxxd", "/abxxcxxcd", "/abxxcxxcdx", "/**", "/a/b/c", "/a/b/c/", 
        "/**/", "/ab/**", "/**/xyz", "/a/b/xyz/d/e/f/xyz", "/**/xyz*xyz", 
        "/a/b/xyz/d/e/f/xyz", "/a/**/c/**/e", "/a/b/b/b/c/d/d/d/e", 
        "/a/**/c/*/e/*", "/a/b/b/b/c/d/d/c/d/e/f", "/a/**/c/*/e/*", 
        "/x/abc", "/x/*", "/x/abc*", "/x/*abc", "/x/a*", "/x/a*de", 
        "/x/abc*de", "/x/a*d*e", "/x/a*e", "/x/a*c*e", "/x/ade", "/x/c*", 
        "/x/*d", "/x/*e"
    ];

    let tables = Tables::new();
    let primitives = Arc::new(Mux::new(Arc::new(DummyHandler::new())));
    let sex = Tables::declare_session(&tables, primitives.clone());
    for (i, rname) in rnames.iter().enumerate() {
        Tables::declare_resource(&tables, &sex, i.try_into().unwrap(), 0, rname);
    }

    for rname1 in rnames.iter() {
        let res_matches = Tables::get_matches(&tables, rname1);
        let matches:Vec<String> = res_matches.iter().map(|m| {m.upgrade().unwrap().read().name()}).collect();
        for rname2 in rnames.iter() {
            if matches.contains(&String::from(*rname2)) {
                assert!(   intersect(rname1, rname2));
            } else {
                assert!( ! intersect(rname1, rname2));
            }
        }
    }
}

#[test]
fn clean_test() {
    let tables = Tables::new();

    let primitives = Arc::new(Mux::new(Arc::new(DummyHandler::new())));
    let sex0 = Tables::declare_session(&tables, primitives.clone());
    assert!(sex0.upgrade().is_some());

    // --------------
    Tables::declare_resource(&tables, &sex0, 1, 0, "/todrop1");
    let optres1 = Resource::get_resource(&tables.read()._get_root(), "/todrop1");
    assert!(optres1.is_some());
    let res1 = optres1.unwrap();
    assert!(res1.upgrade().is_some());

    Tables::declare_resource(&tables, &sex0, 2, 0, "/todrop1/todrop11");
    let optres2 = Resource::get_resource(&tables.read()._get_root(), "/todrop1/todrop11");
    assert!(optres2.is_some());
    let res2 = optres2.unwrap();
    assert!(res2.upgrade().is_some());

    Tables::declare_resource(&tables, &sex0, 3, 0, "/**");
    let optres3 = Resource::get_resource(&tables.read()._get_root(), "/**");
    assert!(optres3.is_some());
    let res3 = optres3.unwrap();
    assert!(res3.upgrade().is_some());

    Tables::undeclare_resource(&tables, &sex0, 1);
    assert!(res1.upgrade().is_some());
    assert!(res2.upgrade().is_some());
    assert!(res3.upgrade().is_some());

    Tables::undeclare_resource(&tables, &sex0, 2);
    assert!( ! res1.upgrade().is_some());
    assert!( ! res2.upgrade().is_some());
    assert!(res3.upgrade().is_some());

    Tables::undeclare_resource(&tables, &sex0, 3);
    assert!( ! res1.upgrade().is_some());
    assert!( ! res2.upgrade().is_some());
    assert!( ! res3.upgrade().is_some());

    // --------------
    Tables::declare_resource(&tables, &sex0, 1, 0, "/todrop1");
    let optres1 = Resource::get_resource(&tables.read()._get_root(), "/todrop1");
    assert!(optres1.is_some());
    let res1 = optres1.unwrap();
    assert!(res1.upgrade().is_some());

    Tables::declare_subscription(&tables, &sex0, 0, "/todrop1/todrop11");
    let optres2 = Resource::get_resource(&tables.read()._get_root(), "/todrop1/todrop11");
    assert!(optres2.is_some());
    let res2 = optres2.unwrap();
    assert!(res2.upgrade().is_some());

    Tables::declare_subscription(&tables, &sex0, 1, "/todrop12");
    let optres3 = Resource::get_resource(&tables.read()._get_root(), "/todrop1/todrop12");
    assert!(optres3.is_some());
    let res3 = optres3.unwrap();
    assert!(res3.upgrade().is_some());

    Tables::undeclare_subscription(&tables, &sex0, 1, "/todrop12");
    assert!(res1.upgrade().is_some());
    assert!(res2.upgrade().is_some());
    assert!( ! res3.upgrade().is_some());

    Tables::undeclare_subscription(&tables, &sex0, 0, "/todrop1/todrop11");
    assert!(res1.upgrade().is_some());
    assert!( ! res2.upgrade().is_some());
    assert!( ! res3.upgrade().is_some());

    Tables::undeclare_resource(&tables, &sex0, 1);
    assert!( ! res1.upgrade().is_some());
    assert!( ! res2.upgrade().is_some());
    assert!( ! res3.upgrade().is_some());

    // --------------
    Tables::declare_resource(&tables, &sex0, 2, 0, "/todrop3");
    Tables::declare_subscription(&tables, &sex0, 0, "/todrop3");
    let optres1 = Resource::get_resource(&tables.read()._get_root(), "/todrop3");
    assert!(optres1.is_some());
    let res1 = optres1.unwrap();
    assert!(res1.upgrade().is_some());

    Tables::undeclare_subscription(&tables, &sex0, 0, "/todrop3");
    assert!(res1.upgrade().is_some());

    Tables::undeclare_resource(&tables, &sex0, 2);
    assert!( ! res1.upgrade().is_some());

    // --------------
    Tables::declare_resource(&tables, &sex0, 3, 0, "/todrop4");
    Tables::declare_resource(&tables, &sex0, 4, 0, "/todrop5");
    Tables::declare_subscription(&tables, &sex0, 0, "/todrop5");
    Tables::declare_subscription(&tables, &sex0, 0, "/todrop6");

    let optres1 = Resource::get_resource(&tables.read()._get_root(), "/todrop4");
    assert!(optres1.is_some());
    let res1 = optres1.unwrap();
    let optres2 = Resource::get_resource(&tables.read()._get_root(), "/todrop5");
    assert!(optres2.is_some());
    let res2 = optres2.unwrap();
    let optres3 = Resource::get_resource(&tables.read()._get_root(), "/todrop6");
    assert!(optres3.is_some());
    let res3 = optres3.unwrap();

    assert!(res1.upgrade().is_some());
    assert!(res2.upgrade().is_some());
    assert!(res3.upgrade().is_some());

    Tables::undeclare_session(&tables, &sex0);
    assert!( ! sex0.upgrade().is_some());
    assert!( ! res1.upgrade().is_some());
    assert!( ! res2.upgrade().is_some());
    assert!( ! res3.upgrade().is_some());

}

#[test]
fn client_test() {
    let tables = Tables::new();
    let primitives = Arc::new(Mux::new(Arc::new(DummyHandler::new())));

    let sex0 = Tables::declare_session(&tables, primitives.clone());
    Tables::declare_resource(&tables, &sex0, 11, 0, "/test/client");
    Tables::declare_subscription(&tables, &sex0, 11, "/**");
    Tables::declare_resource(&tables, &sex0, 12, 11, "/z1_pub1");

    let sex1 = Tables::declare_session(&tables, primitives.clone());
    Tables::declare_resource(&tables, &sex1, 21, 0, "/test/client");
    Tables::declare_subscription(&tables, &sex1, 21, "/**");
    Tables::declare_resource(&tables, &sex1, 22, 21, "/z2_pub1");

    let sex2 = Tables::declare_session(&tables, primitives.clone());
    Tables::declare_resource(&tables, &sex2, 31, 0, "/test/client");
    Tables::declare_subscription(&tables, &sex2, 31, "/**");

    
    let result_opt = Tables::route_data_to_map(&tables, &sex0, &0, "/test/client/z1_wr1"); 
    assert!(result_opt.is_some());
    let result = result_opt.unwrap();

    let opt_sex = result.get(&0);
    assert!(opt_sex.is_some());
    let (_, id, suffix) = opt_sex.unwrap();
    assert_eq!(*id, 11);
    assert_eq!(suffix, "/z1_wr1");

    let opt_sex = result.get(&1);
    assert!(opt_sex.is_some());
    let (_, id, suffix) = opt_sex.unwrap();
    assert_eq!(*id, 21);
    assert_eq!(suffix, "/z1_wr1");

    let opt_sex = result.get(&2);
    assert!(opt_sex.is_some());
    let (_, id, suffix) = opt_sex.unwrap();
    assert_eq!(*id, 31);
    assert_eq!(suffix, "/z1_wr1");

    
    let result_opt = Tables::route_data_to_map(&tables, &sex0, &11, "/z1_wr2"); 
    assert!(result_opt.is_some());
    let result = result_opt.unwrap();

    let opt_sex = result.get(&0);
    assert!(opt_sex.is_some());
    let (_, id, suffix) = opt_sex.unwrap();
    assert_eq!(*id, 11);
    assert_eq!(suffix, "/z1_wr2");

    let opt_sex = result.get(&1);
    assert!(opt_sex.is_some());
    let (_, id, suffix) = opt_sex.unwrap();
    assert_eq!(*id, 21);
    assert_eq!(suffix, "/z1_wr2");

    let opt_sex = result.get(&2);
    assert!(opt_sex.is_some());
    let (_, id, suffix) = opt_sex.unwrap();
    assert_eq!(*id, 31);
    assert_eq!(suffix, "/z1_wr2");

    
    let result_opt = Tables::route_data_to_map(&tables, &sex1, &0, "/test/client/**"); 
    assert!(result_opt.is_some());
    let result = result_opt.unwrap();

    let opt_sex = result.get(&0);
    assert!(opt_sex.is_some());
    let (_, id, suffix) = opt_sex.unwrap();
    assert_eq!(*id, 11);
    assert_eq!(suffix, "/**");

    let opt_sex = result.get(&1);
    assert!(opt_sex.is_some());
    let (_, id, suffix) = opt_sex.unwrap();
    assert_eq!(*id, 21);
    assert_eq!(suffix, "/**");

    let opt_sex = result.get(&2);
    assert!(opt_sex.is_some());
    let (_, id, suffix) = opt_sex.unwrap();
    assert_eq!(*id, 31);
    assert_eq!(suffix, "/**");

    
    let result_opt = Tables::route_data_to_map(&tables, &sex0, &12, ""); 
    assert!(result_opt.is_some());
    let result = result_opt.unwrap();

    let opt_sex = result.get(&0);
    assert!(opt_sex.is_some());
    let (_, id, suffix) = opt_sex.unwrap();
    assert_eq!(*id, 12);
    assert_eq!(suffix, "");

    let opt_sex = result.get(&1);
    assert!(opt_sex.is_some());
    let (_, id, suffix) = opt_sex.unwrap();
    assert_eq!(*id, 21);
    assert_eq!(suffix, "/z1_pub1");

    let opt_sex = result.get(&2);
    assert!(opt_sex.is_some());
    let (_, id, suffix) = opt_sex.unwrap();
    assert_eq!(*id, 31);
    assert_eq!(suffix, "/z1_pub1");

    
    let result_opt = Tables::route_data_to_map(&tables, &sex1, &22, ""); 
    assert!(result_opt.is_some());
    let result = result_opt.unwrap();

    let opt_sex = result.get(&0);
    assert!(opt_sex.is_some());
    let (_, id, suffix) = opt_sex.unwrap();
    assert_eq!(*id, 11);
    assert_eq!(suffix, "/z2_pub1");

    let opt_sex = result.get(&1);
    assert!(opt_sex.is_some());
    let (_, id, suffix) = opt_sex.unwrap();
    assert_eq!(*id, 22);
    assert_eq!(suffix, "");

    let opt_sex = result.get(&2);
    assert!(opt_sex.is_some());
    let (_, id, suffix) = opt_sex.unwrap();
    assert_eq!(*id, 31);
    assert_eq!(suffix, "/z2_pub1");
}


