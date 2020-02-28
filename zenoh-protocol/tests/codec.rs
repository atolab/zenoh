
use zenoh_protocol::core::{ZInt, ZError};
use zenoh_protocol::io::WBuf;


fn test_zint(v: ZInt) -> Result<(), ZError> {
  let mut buf = WBuf::new();
  buf.write_zint(v);
  assert_eq!(v, buf.as_rbuf().read_zint()?);
  Ok(())
}


#[test]
fn test_zint_codec_limits() -> Result<(), ZError> {
  test_zint(0)?;
  for i in 1 .. 10 {
    let v: ZInt =  1 << (7*i);
    test_zint(v-1)?;
    test_zint(v)?;
  }
  test_zint(std::u64::MAX)?;
  Ok(())
}
