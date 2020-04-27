
use super::{ArcSlice, RBuf, WBuf};
use crate::core::{ZError, ZErrorKind, ZInt, ZResult, ZINT_MAX_BYTES};
use crate::zerror;

pub fn encoded_size_of(v: ZInt) -> usize {
    // Note that the sizes are already shifted by 2 bits
    if v <= 0xff { 0 } 
    else if v <= 0xffff { 0x04 } 
    else if v <= 0xffff_ffff { 0x08 }
    else { 0x0c }
  }

impl RBuf {

  pub fn read_zint(&mut self) -> ZResult<ZInt> {
    let mut v : ZInt = 0;
    let mut b = self.read()?;
    let mut i = 0;
    let mut k = ZINT_MAX_BYTES;
    while b > 0x7f && k > 0 {
      v |= ((b & 0x7f) as ZInt)  << i;
      i += 7;
      b = self.read()?;
      k -=1;
    }
    if k > 0 {
      v |= ((b & 0x7f) as ZInt)  << i;
      Ok(v)
    } else {
      Err(zerror!(ZErrorKind::InvalidMessage { descr: "Invalid ZInt (out of 64-bit bound)".to_string() }))  
    }
  }

  // Same as read_bytes but with array length before the bytes.
  pub fn read_bytes_array(&mut self) -> ZResult<Vec<u8>> {
    let len = self.read_zint()?;
    let mut buf = vec![0; len as usize];
    self.read_bytes(buf.as_mut_slice())?;
    Ok(buf)
  }
  
  pub fn read_string(&mut self) -> ZResult<String> { 
    let bytes = self.read_bytes_array()?;
    Ok(String::from(String::from_utf8_lossy(&bytes)))
  }
}

impl WBuf {

  /// This the traditional VByte encoding, in which an arbirary integer
  /// is encoded as a sequence  of 
  /// 
  pub fn write_zint(& mut self, v: ZInt) {
    let mut c = v;
    let mut b : u8 = (c & 0xff) as u8;
    while c > 0x7f {
      self.write(b | 0x80);
      c >>= 7;
      b = (c & 0xff) as u8;
    }
    self.write(b);
  }

  // Same as write_bytes but with array length before the bytes.
  pub fn write_bytes_array(&mut self, s: &[u8]) {
    self.write_zint(s.len() as ZInt);
    self.write_bytes(s);
  }

  pub fn write_string(&mut self, s: &str) {
    self.write_zint(s.len() as ZInt);
    self.write_bytes(s.as_bytes());
  }

  // Similar than write_bytes_array but zero-copy as slice is shared
  pub fn write_bytes_slice(&mut self, slice: &ArcSlice) {
    self.write_zint(slice.len() as ZInt);
    self.add_slice(slice.clone());
  }

  // Similar than write_bytes_array but zero-copy as RBuf contains slices that are shared
  pub fn write_rbuf(&mut self, rbuf: &RBuf) {
    self.write_zint(rbuf.len() as ZInt);
    for slice in rbuf.get_slices() {
      self.add_slice(slice.clone());
    }
  }

}