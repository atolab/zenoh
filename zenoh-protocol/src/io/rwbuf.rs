use std::fmt;
use crate::core::{ZError, ZErrorKind};
use crate::zerror;


/// This is a non owning buffer that maintains read and write indexes.
/// The invariant preserved by this buffer is that the read position will
/// always be smaller or equal to the write position.
/// 
#[derive(Clone)]
pub struct RWBuf {
  r_pos: usize,
  w_pos: usize,
  buf: Vec<u8>
}


impl RWBuf {
  pub fn new(capacity: usize) -> RWBuf {
    RWBuf { r_pos : 0, w_pos : 0, buf: vec![0; capacity] }
  }

  pub fn clear(&mut self) {
    self.r_pos = 0;
    self.w_pos = 0;
  }

  pub fn slice(&self) -> &[u8] {
    &self.buf[..]
  }
  
  /// When writing directly on the buffer slice it is imperative to manually
  /// update the writing position index by calling the "set_write_pos" method
  /// 
  /// # Example: 
  /// ```
  ///   use std::io::prelude::*;
  ///   use std::net::TcpStream;
  ///   
  ///   let mut socket = TcpStream::connect("127.0.0.1:12345")?;
  ///   let mut buff = RWBuf::new(8_192);
  ///   let n = socket.read(&mut buff.writable_slice());
  ///   // n is the number of read (and consequently written) bytes
  ///   buff.set_write_pos(buff.write_pos() + n).unwrap();
  /// ```
  pub fn writable_slice(&mut self) -> &mut [u8] {    
    &mut self.buf[self.w_pos..]
  }

  pub fn read_pos(& self) -> usize {
    self.r_pos
  }
  /*
  pub fn set_read_pos(&mut self, pos: usize) -> Result<(), ZError> {
    if pos <=self.buf.capacity() {
      self.r_pos = pos;
      Ok(())
    } else {
      Err(ZError {
        msg : format!("Read position {} our of range [0, {}].", pos, self.buf.capacity())
      })
    }
  }

  pub fn set_write_pos(&mut self, pos: usize) -> Result<(), ZError> {
    if pos <=self.buf.capacity() {
      self.w_pos = pos;
      Ok(())
    } else {
      Err(ZError {
        msg : format!("Write position {} our of range [0, {}].", pos, self.buf.capacity())
      })
    }
  }
*/
  pub fn write_pos(& self) -> usize {
    self.w_pos
  }
  
  #[inline]
  pub fn readable(&self) -> usize {
    self.w_pos - self.r_pos
  }
  
  #[inline]
  pub fn writable(&self) -> usize {
    self.buf.capacity() - self.w_pos
  }

  pub fn write(&mut self, b: u8) -> Result<(), ZError> {
    if self.w_pos < self.buf.capacity() {
      self.buf[self.w_pos] = b;
      self.w_pos += 1;
      Ok(())
    } else {
      Err(zerror!(ZErrorKind::BufferOverflow { missing: 1 }))  
    }    
  } 

  pub fn read(&mut self) -> Result<u8, ZError> {
    if self.r_pos < self.w_pos {
      let b = self.buf[self.r_pos];
      self.r_pos += 1;
      Ok(b)
    } else {
      Err(zerror!(ZErrorKind::BufferUnderflow { missing: 1 }))  
    }
  }

  // Write all the bytes, without the length.
  pub fn write_bytes(&mut self, s: &[u8]) -> Result<(), ZError> {
    let l = s.len();
    if l <= self.writable() {
      self.buf[self.w_pos..(self.w_pos+l)].copy_from_slice(s);
      self.w_pos += l;
      Ok(())
    } else {
      Err(zerror!(ZErrorKind::BufferOverflow { missing: l - self.writable() }))  
    }
  }

  // Read all the bytes to fill bs, without reading any length.
  pub fn read_bytes(&mut self, bs: &mut [u8]) -> Result<(), ZError> {
    let l = bs.len();
    if self.readable() >= l {
      bs.copy_from_slice(&self.buf[self.r_pos..(self.r_pos+l)]);
      self.r_pos += l;
      Ok(())
    } else {
      Err(zerror!(ZErrorKind::BufferUnderflow { missing: l - self.readable() }))  
    }
  }

  pub fn read_slice(&mut self, len: usize) -> Result<&[u8], ZError> {
    if self.readable() >= len {
      let result = &self.buf[self.r_pos..(self.r_pos+len)];
      self.r_pos += len;
      Ok(result)
    } else {
      Err(zerror!(ZErrorKind::BufferUnderflow { missing: len - self.readable() }))  
    }
  }
}

impl fmt::Display for RWBuf {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
      write!(f, "RWBuf(cap:{}, r_pos:{}, w_pos:{})",
        self.buf.capacity(), self.r_pos, self.w_pos)
  }
}

impl fmt::Debug for RWBuf {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "RWBuf {{ cap: {}, r_pos: {}, w_pos:{}, buf:\n {:02x?} \n}}",
      self.buf.capacity(), self.r_pos, self.w_pos,
      &self.buf[0..self.w_pos]
    )
  }
}
