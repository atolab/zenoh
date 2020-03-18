use zenoh_protocol::proto::SeqNumGenerator;

#[test]
fn sn_gen_test() {
  let sng = SeqNumGenerator::make(0, 14).unwrap();
  let a = sng.next();
  let b = sng.next();
  assert_eq!(sng.precedes(a, b), true);
  assert_eq!(sng.precedes(b, a), false);
}