
use zenoh_protocol::io::rwbuf::{RWBuf,OutOfBounds};
use rand::distributions::{Distribution, Standard};

const N : usize = 1_000_000;



fn test_zint_codec_for<T>(n: usize) -> Result<(), OutOfBounds>
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


fn test_one_c3_codec_for<T>(n: usize) -> Result<(), OutOfBounds>
  where T: Copy + PartialEq + std::fmt::Debug,
        Standard: Distribution<T> ,
        u64: std::convert::From<T> 
{
  let mut buf = RWBuf::new(512);
  let mut ns : [u64; 4] = [0u64; 4];
  for _ in 0..n {
    let a: u64 = u64::from(rand::random::<T>());    
    buf.write_one_c3(a)?;
    let len = buf.read_c3(&mut ns)?;        
    buf.clear();
    assert_eq!(len, 1);
    assert_eq!(a, ns[0]);
  }
  Ok(())
}

fn test_two_c3_codec_for<T>(n: usize) -> Result<(), OutOfBounds>
  where T: Copy + PartialEq + std::fmt::Debug,
        Standard: Distribution<T> ,
        u64: std::convert::From<T> 
{
  let mut buf = RWBuf::new(512);
  let rs: [u64;2] = [u64::from(rand::random::<T>()), u64::from(rand::random::<T>())];
  let mut ns : [u64; 4] = [0u64; 4];
  for _ in 0..n {    
    buf.write_two_c3(&rs)?;
    let len = buf.read_c3(&mut ns)?;        
    buf.clear();
    assert_eq!(len, 2);
    assert_eq!(rs[0], ns[0]);
    assert_eq!(rs[1], ns[1]);
  }
  Ok(())
}

#[test]
fn test_zint_codec() -> Result<(), OutOfBounds> {  
  test_zint_codec_for::<u8>(N)?;
  test_zint_codec_for::<u16>(N)?;
  test_zint_codec_for::<u32>(N)?;
  test_zint_codec_for::<u64>(N)
}

#[test]
fn test_one_c3_codec() -> Result<(), OutOfBounds> {
  test_one_c3_codec_for::<u8>(N)?;
  test_one_c3_codec_for::<u16>(N)?;
  test_one_c3_codec_for::<u32>(N)?;
  test_one_c3_codec_for::<u64>(N)
}
#[test]
fn test_read_write_u16() -> Result<(), OutOfBounds> {
  let mut buf = RWBuf::new(256);
  let n  = rand::random::<u16>();
  buf.write_u16(n)?;
  let r = buf.read_u16()?;
  assert_eq!(n, r);
  Ok(())
}


#[test]
fn test_read_write_u32() -> Result<(), OutOfBounds> {
  let mut buf = RWBuf::new(256);
  let n  = rand::random::<u32>();
  buf.write_u32(n)?;
  let r = buf.read_u32()?;
  assert_eq!(n, r);
  Ok(())
}


#[test]
fn test_read_write_u64() -> Result<(), OutOfBounds> {
  let mut buf = RWBuf::new(256);
  let n  = rand::random::<u64>();
  buf.write_u64(n)?;
  let r = buf.read_u64()?;
  assert_eq!(n, r);
  Ok(())
}
#[test]
fn test_two_c3_codec() -> Result<(), OutOfBounds> {
  test_two_c3_codec_for::<u8>(N)?;
  test_two_c3_codec_for::<u16>(N)?;
  test_two_c3_codec_for::<u32>(N)?;
  test_two_c3_codec_for::<u64>(N)
}