
use crate::io::rwbuf::{RWBuf, OutOfBounds};
use crate::core;

pub fn compute_size(n: u64) -> u8 {
    // Note that the sizes are already shifted by 2 bits
    if n <= 0xff { 0 } 
    else if n <= 0xffff { 0x04 } 
    else if n <= 0xffff_ffff { 0x08 }
    else { 0x0c }
  }

impl RWBuf {


  /// This the traditional VByte encoding, in which an arbirary integer
  /// is encoded as a sequence  of 
  /// 
  pub fn write_zint(& mut self, v: core::ZInt) -> Result<(), OutOfBounds> {
    let mut c = v;
    let mut b : u8 = (c & 0xff) as u8;
    while c > 0x7f {
      self.write(b | 0x80)?;
      c >>= 7;
      b = (c & 0xff) as u8;
    }
    self.write(b)?;
    Ok(())
  }

  pub fn read_zint(&mut self) -> Result<core::ZInt, OutOfBounds> {
    let mut v : core::ZInt = 0;
    let mut b = self.read()?;
    let mut i = 0;
    let mut k = core::ZINT_MAX_BYTES;
    while b > 0x7f && k > 0 {
      v |= ((b & 0x7f) as core::ZInt)  << i;
      i += 7;
      b = self.read()?;
      k -=1;
    }
    if k > 0 {
      v |= ((b & 0x7f) as core::ZInt)  << i;
      Ok(v)
    } else {
      Err(OutOfBounds {
        msg : "ZInt out of bounds".to_string()
      })
    }
  }

  // Same as write_bytes but with array length before the bytes.
  pub fn write_bytes_array(&mut self, s: &[u8]) -> Result<(), OutOfBounds> {
    self.write_zint(s.len() as core::ZInt)?;
    self.write_bytes(s)
  }

  // Same as write_bytes but with array length before the bytes.
  pub fn read_bytes_array(&mut self) -> Result<Vec<u8>, OutOfBounds> {
    let len = self.read_zint()?;
    let mut buf = Vec::with_capacity(len as usize);
    self.read_bytes(buf.as_mut_slice())?;
    Ok(buf)
  }
  
  pub fn write_string(&mut self, s: &str) -> Result<(), OutOfBounds> { 
    self.write_zint(s.len() as core::ZInt)?;
    self.write_bytes(s.as_bytes())  
  }

  pub fn read_string(&mut self) -> Result<String, OutOfBounds> { 
    let len = self.read_zint()?;
    let bytes = self.read_slice(len as usize)?;
    Ok(String::from(String::from_utf8_lossy(bytes)))
  }

}