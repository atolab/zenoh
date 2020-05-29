use std::error::Error;
use zenoh_protocol::core::{ZError, ZErrorKind};
use zenoh_protocol::zerror;


#[test]
fn error_simple() {
    let err = zerror!(ZErrorKind::Other { descr:"TEST".to_string() });
    let s = format!("{}", err);
    println!("{}", err);
    println!("{:?}", err);
    assert!(if let ZErrorKind::Other { descr:_ } = err.kind() { true } else { false });
    assert!(s.contains("TEST"));
    assert!(s.contains(file!()));
    assert!(err.source().is_none());

    let err = zerror!(ZErrorKind::BufferOverflow{missing:3});
    let s = format!("{}", err);
    println!("{}", err);
    println!("{:?}", err);
    match err.kind() {
        ZErrorKind::BufferOverflow{missing:x} => assert_eq!(3 as usize, *x),
        _ => assert!(false)
    }
    assert!(s.contains(file!()));
    assert!(err.source().is_none());
}

#[test]
fn error_with_source() {
    let err1 = zerror!( ZErrorKind::Other { descr:"ERR1".to_string() });
    let err2 = zerror!( ZErrorKind::Other { descr:"ERR2".to_string() }, err1);
    let s = format!("{}", err2);
    println!("{}", err2);
    println!("{:?}", err2);

    assert!(if let ZErrorKind::Other { descr:_ } = err2.kind() { true } else { false });
    assert!(s.contains(file!()));
    assert!(err2.source().is_some());
    assert_eq!(true, s.contains("ERR1"));
    assert_eq!(true, s.contains("ERR2"));

    let ioerr = std::io::Error::new(std::io::ErrorKind::Other, "IOERR");
    let err2 = zerror!( ZErrorKind::Other { descr:"ERR2".to_string() }, ioerr);
    let s = format!("{}", err2);
    println!("{}", err2);
    println!("{:?}", err2);

    assert!(if let ZErrorKind::Other { descr:_ } = err2.kind() { true } else { false });
    assert!(s.contains(file!()));
    assert!(err2.source().is_some());
    assert_eq!(true, s.contains("IOERR"));
    assert_eq!(true, s.contains("ERR2"));

}



