
/// This is a non owning buffer that maintains read and write indexes.
/// The invariant preserved by this buffer is that the read position will
/// always be smaller or equal to the write position.
/// 
#[derive(Debug, Clone)]
pub struct RWBuf {
  r_pos: usize,
  w_pos: usize,
  buf: Vec<u8>
}

#[derive(Debug, Clone)]
pub struct InvalidFormat { pub msg: String }

#[derive(Debug, Clone)]
pub struct OutOfBounds { pub msg: String }

impl RWBuf {
  pub fn new(capacity: usize) -> RWBuf {
    RWBuf { r_pos : 0, w_pos : 0, buf: vec![0; capacity] }
  }

  pub fn clear(&mut self) {
    self.r_pos = 0;
    self.w_pos = 0;
  }

  pub fn slice(& self) -> &[u8] {
    &self.buf[..]
  }
  
  pub fn mut_slice(&mut self) -> &mut [u8] {    
    &mut self.buf[..]
  }

  pub fn read_pos(& self) -> usize {
    self.r_pos
  }
  
  pub fn set_read_pos(&mut self, pos: usize) -> Result<(), OutOfBounds> {
    if pos <=self.buf.capacity() {
      self.r_pos = pos;
      Ok(())
    } else {
      Err(OutOfBounds {
        msg : format!("Read position {} our of range [0, {}].", pos, self.buf.capacity())
      })
    }
  }

  pub fn set_write_pos(&mut self, pos: usize) -> Result<(), OutOfBounds> {
    if pos <=self.buf.capacity() {
      self.w_pos = pos;
      Ok(())
    } else {
      Err(OutOfBounds {
        msg : format!("Write position {} our of range [0, {}].", pos, self.buf.capacity())
      })
    }
  }

  pub fn write_pos(& self) -> usize {
    self.w_pos
  }
  
  #[inline]
  pub fn readable(& self) -> usize {
    self.w_pos - self.r_pos
  }
  
  #[inline]
  pub fn writable(& self) -> usize {
    self.buf.capacity() - self.w_pos
  }

  pub fn write(&mut self, b: u8) -> Result<(), OutOfBounds> {
    if self.w_pos < self.buf.capacity() {
      self.buf[self.w_pos] = b;
      self.w_pos += 1;
      Ok(())
    } else {
      Err(OutOfBounds {
        msg : format!("Write position {} beyond limits (size is {}).", self.w_pos, self.buf.capacity())})
    }    
  } 
    
  pub fn read(&mut self) -> Result<u8, OutOfBounds> {
    if self.r_pos < self.w_pos {
      let b = self.buf[self.r_pos];
      self.r_pos += 1;
      Ok(b)
    } else {
      Err(OutOfBounds {
        msg : format!("Read position {} beyond limits (w_pos is {}).", self.w_pos, self.w_pos)
      })
    }
  }

  // Write all the bytes, without the length.
  pub fn write_bytes(&mut self, s: &[u8]) -> Result<(), OutOfBounds> {
    let l = s.len();
    if l <= self.writable() {      
      self.buf[self.w_pos..(self.w_pos+l)].copy_from_slice(s);
      self.w_pos += l;
      Ok(())
    } else {
      Err(OutOfBounds {
        msg : format!("Out of bounds write bytes -- slice len = {}, writable: {}).", s.len(), self.writable())
      })  
    }
  }

  // Read all the bytes to fill bs, without reading any length.
  pub fn read_bytes(&mut self, bs: &mut [u8]) -> Result<&mut Self, OutOfBounds> {
    let l = bs.len();
    if self.readable() >= l {
      bs.copy_from_slice(&self.buf[self.r_pos..(self.r_pos+l)]);
      Ok(self)
    } else {
      Err(OutOfBounds {
        msg : format!("Out of bounds read byres -- slice len = {}, readable: {}).", bs.len(), self.readable())
      })  
    }
  }

}