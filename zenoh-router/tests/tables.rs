use zenoh_router::routing::tables::Tables;
use std::convert::TryInto;
use zenoh_protocol::core::rname::intersect;

#[test]
fn base_test() {
    let tables = Tables::new();
    let sex = Tables::declare_session(&tables, 0);
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
    let sex = Tables::declare_session(&tables, 0);
    for (i, rname) in rnames.iter().enumerate() {
        Tables::declare_resource(&tables, &sex, i.try_into().unwrap(), 0, rname);
    }

    for rname1 in rnames.iter() {
        let res_matches = Tables::get_matches(&tables, rname1);
        let matches:Vec<String> = res_matches.iter().map(|m| {m.read().name()}).collect();
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
fn client_test() {
    let tables = Tables::new();

    let sex0 = Tables::declare_session(&tables, 0);
    Tables::declare_resource(&tables, &sex0, 11, 0, "/test/client");
    Tables::declare_subscription(&tables, &sex0, 11, "/**");
    Tables::declare_resource(&tables, &sex0, 12, 11, "/z1_pub1");

    let sex1 = Tables::declare_session(&tables, 1);
    Tables::declare_resource(&tables, &sex1, 21, 0, "/test/client");
    Tables::declare_subscription(&tables, &sex1, 21, "/**");
    Tables::declare_resource(&tables, &sex1, 22, 21, "/z2_pub1");

    let sex2 = Tables::declare_session(&tables, 2);
    Tables::declare_resource(&tables, &sex2, 31, 0, "/test/client");
    Tables::declare_subscription(&tables, &sex2, 31, "/**");

    
    let result_opt = Tables::route_data(&tables, &sex0, &0, "/test/client/z1_wr1"); 
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

    
    let result_opt = Tables::route_data(&tables, &sex0, &11, "/z1_wr2"); 
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

    
    let result_opt = Tables::route_data(&tables, &sex1, &0, "/test/client/**"); 
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

    
    let result_opt = Tables::route_data(&tables, &sex0, &12, ""); 
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

    
    let result_opt = Tables::route_data(&tables, &sex1, &22, ""); 
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


