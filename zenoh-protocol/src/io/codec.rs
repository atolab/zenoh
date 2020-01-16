
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

  
  /// Compact-3 (C3) Encoding is a space and time efficient encoding 
  /// that allows to represent up to 3 integer with variable length 
  /// encoding between 1 and 8 bytes. Specifically the lenght of the 
  /// encoded integer can be either 1, 2, 4 or 8 bytes. 
  /// 
  /// The three integers are encoded as follows: 
  ///  
  ///    7   6   5   4   3   2   1   0
  ///  +---+---+---+---+---+---+---+---+
  ///  |   r2  |   r1  |   r0  |   n   |
  ///  +---+---+---+---+---+---+---+---+
  ///  ~              a                ~
  ///  +---+---+---+---+---+---+---+---+
  ///  ~              b                ~
  ///  +---+---+---+---+---+---+---+---+
  ///  ~              c                ~
  ///  +---+---+---+---+---+---+---+---+    
  /// 
  /// Where: 
  /// 
  ///   - n: the number of integer encoded
  ///   - ri: pow(2, ri) is the size of the integer encoded
  /// 
  /// Integers of 2 or more bytes are encoded using little endian. 
  
  
  pub fn write_one_c3(&mut self, a: u64) -> Result<&mut Self, OutOfBounds> {    
    let s = compute_size(a);
    let mask : u8  =  s | 1;
    self.write(mask)?;
    match s {
      0x0u8 => self.write(a as u8),
      0x4u8 => self.write_u16(a as u16),
      0x8u8 => self.write_u32(a as u32),
      0xCu8 => self.write_u64(a),
      _ => panic!("Invalid C3 size")
    }
  }

  pub fn read_c3(&mut self, ns: &mut [u64; 4]) -> Result<u8, OutOfBounds> {
    let mask = self.read()?;
    let n = mask & 0x03u8;    
    let mut k = 1;
    loop {
      let l = (mask >> (2*k)) & 0x3u8;      
      match l {
        0 => ns[k-1] = self.read()? as u64,
        1 => ns[k-1] = self.read_u16()? as u64,
        2 => ns[k-1] = self.read_u32()? as u64,
        3 => ns[k-1] = self.read_u64()? as u64,
        _ => panic!("Invalid C3 size")
      };
      if k == (n as usize) { break }
      k += 1;      
    }
    Ok(n)
  }

  pub fn write_two_c3(&mut self, ns: &[u64;2]) -> Result<&mut Self, OutOfBounds> {      
    let s0 = compute_size(ns[0]);
    let s1 = compute_size(ns[1]);    
    let mask : u8  = (s1 << 2) | s0 | 2;          

    self.write(mask)?;
    // Manual loop unrolling...
    match s0 {
      0x0u8 => self.write(ns[0] as u8),
      0x4u8 => self.write_u16(ns[0] as u16),
      0x8u8 => self.write_u32(ns[0] as u32),
      0xCu8 => self.write_u64(ns[0]),
      _ => panic!("Invalid C3 size")
    }?;
    match s1 {
      0x0u8 => self.write(ns[1] as u8),
      0x4u8 => self.write_u16(ns[1] as u16),
      0x8u8 => self.write_u32(ns[1] as u32),
      0xCu8 => self.write_u64(ns[1]),
      _ => panic!("Invalid C3 size")
    }
  }

  pub fn write_three_c3(&mut self, ns: &[u64;3]) -> Result<&mut Self, OutOfBounds> {
    let s0 = compute_size(ns[0]);
    let s1 = compute_size(ns[1]);
    let s2 = compute_size(ns[2]);    
    let mask : u8  = (s2 << 4) | (s1 << 2) | s0 | 2;      
    self.write(mask)?;
      // Manual loop unrolling...
    match s0 {
      0x0u8 => self.write(ns[0] as u8),
      0x4u8 => self.write_u16(ns[0] as u16),
      0x8u8 => self.write_u32(ns[0] as u32),
      0xCu8 => self.write_u64(ns[0]),
      _ => panic!("Invalid C3 size")
    }?;
    match s1 {
      0x0u8 => self.write(ns[1] as u8),
      0x4u8 => self.write_u16(ns[1] as u16),
      0x8u8 => self.write_u32(ns[1] as u32),
      0xCu8 => self.write_u64(ns[1]),
      _ => panic!("Invalid C3 size")
    }?;
    match s2 {
      0x0u8 => self.write(ns[2] as u8),
      0x4u8 => self.write_u16(ns[2] as u16),
      0x8u8 => self.write_u32(ns[2] as u32),
      0xCu8 => self.write_u64(ns[2]),
      _ => panic!("Invalid C3 size")
    }    
  }

  /// This the traditional VByte encoding, in which an arbirary integer
  /// is encoded as a sequence  of 
  /// 
  pub fn write_zint(& mut self, v: core::ZInt) -> Result<&mut Self, OutOfBounds> {
    let mut c = v;
    let mut b : u8 = (c & 0xff) as u8;
    while c > 0x7f {
      self.write(b | 0x80)?;
      c >>= 7;
      b = (c & 0xff) as u8;
    }
    self.write(b)?;
    Ok(self)
  }

pub fn write_zint2(& mut self, v: core::ZInt) -> Result<&mut Self, OutOfBounds> {
    if v <= 0x7f {
      self.write(v as u8)      
    } else if v <= 0x3fff_ffff {
      self
        .write((v & 0xff) as u8)?
        .write((v >> 7) as u8)      
    } else if v <= 0x1fff_ffff_ffff {
       self
        .write((v & 0xff) as u8)?
        .write(((v >> 7) & 0xff) as u8)?     
        .write(((v >> 14) & 0xff) as u8)
    }
    else { Ok(self) }
    // let mut c = v;
    // let mut b : u8 = (c & 0xff) as u8;
    // while c > 0x7f {
    //   self.write(b | 0x80)?;
    //   c = c >> 7;
    //   b = (c & 0xff) as u8;
    // }
    // self.write(b)?;
    // Ok(self)
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

  pub fn write_string(&mut self, s: &str) -> Result<&mut Self, OutOfBounds> { 
    self
      .write_zint(s.len() as core::ZInt)?
      .write_bytes(s.as_bytes())  
  }
}