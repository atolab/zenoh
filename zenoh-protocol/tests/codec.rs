
use zenoh_protocol::core::{ZInt, ZResult};
use zenoh_protocol::io::{WBuf, RBuf};


fn test_zint(v: ZInt) -> ZResult<()> {
  let mut buf = WBuf::new(32, true);
  buf.write_zint(v);
  assert_eq!(v, RBuf::from(&buf).read_zint()?);
  Ok(())
}


#[test]
fn test_zint_codec_limits() -> ZResult<()> {
  test_zint(0)?;
  for i in 1 .. 10 {
    let v: ZInt =  1 << (7*i);
    test_zint(v-1)?;
    test_zint(v)?;
  }
  test_zint(std::u64::MAX)?;
  Ok(())
}
