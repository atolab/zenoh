
use zenoh_protocol::core::ZError;
use zenoh_protocol::io::RWBuf;
use rand::distributions::{Distribution, Standard};

const N : usize = 1_000_000;



fn test_zint_codec_for<T>(n: usize) -> Result<(), ZError>
  where T: Copy + PartialEq + std::fmt::Debug,
        Standard: Distribution<T> ,
        u64: std::convert::From<T> 
{
  let mut buf = RWBuf::new(512);
  for _ in 0..n {
    let a: u64 = u64::from(rand::random::<T>());    
    buf.write_zint(a)?;
    let b : u64 = buf.read_zint()?;        
    buf.clear();
    assert_eq!(a, b);
  }
  Ok(())
}


#[test]
fn test_zint_codec() -> Result<(), ZError> {  
  test_zint_codec_for::<u8>(N)?;
  test_zint_codec_for::<u16>(N)?;
  test_zint_codec_for::<u32>(N)?;
  test_zint_codec_for::<u64>(N)
}
