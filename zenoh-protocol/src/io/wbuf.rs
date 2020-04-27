use std::fmt;
use async_std::sync::Arc;
use std::io::IoSlice;
use super::{ArcSlice, RBuf};
use crate::core::{ZError, ZResult, ZErrorKind};
use crate::zerror;


#[derive(Clone)]
pub struct WBuf {
    rbuf: RBuf,
    rpos: Option<usize>,   // if None then rpos is in RBuf, else rpos is index in wbuf.
    wbuf: Option<Vec<u8>>,
    wpos: usize,
    capacity: usize
}


impl WBuf {

    // Note: capacity is the size of the initial wbuf (Vec<u8>)
    // When write exceeds capacity, a new Vec<u8> is allocated with this same capacity
    pub fn new(capacity: usize) -> WBuf {
        let rbuf = RBuf::new();
        let wbuf = None;
        WBuf { rbuf, rpos:Some(0), wbuf, wpos:0, capacity }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.rbuf.is_empty() && self.wpos == 0
    }

    pub fn clear(&mut self) {
        self.rbuf = RBuf::new();
        self.rpos = Some(0);
        self.wpos = 0;
    }

    fn wbuf_to_rbuf(&mut self) {
        if self.wbuf.is_some() {
            let rlen = self.rbuf.len();
            let wbuf = self.wbuf.take();
            self.rbuf.add_slice(ArcSlice::new(Arc::new(wbuf.unwrap()), 0, self.wpos));
            // if rpos was in wbuf, set it in rbuf and to None here
            if let Some(rp) = self.rpos {
                self.rbuf.set_pos(rlen + rp).unwrap();
                self.rpos = None;
            }
            self.wpos = 0;
        }
    }

    #[inline]
    fn make_writable(&mut self) {
        if self.wbuf.is_none() {
            self.wbuf.replace(vec![0u8; self.capacity]);
        } else if self.wpos >= self.wbuf.as_ref().unwrap().len() {
            self.wbuf_to_rbuf();
            self.wbuf.replace(vec![0u8; self.capacity]);
        }
    }

    pub fn get_slices(&mut self) -> &[ArcSlice] {
        // if something was written in wbuf, force move of wbuf in rbuf 
        if self.wpos > 0 {
            self.wbuf_to_rbuf();
        }
        // just return the slices from rbuf
        &self.rbuf.get_slices()
    }

    pub fn as_ioslices(&self) -> Vec<IoSlice> {
        let mut result = self.rbuf.as_ioslices();
        if self.wbuf.is_some() && self.wpos > 0 {
            result.push(IoSlice::new(&self.wbuf.as_ref().unwrap()[0..self.wpos]));
        }
        result
    }

    pub fn as_rbuf(&mut self) -> RBuf {
        // if something was written in wbuf, force move of wbuf in rbuf 
        if self.wpos > 0 {
            self.wbuf_to_rbuf();
        }
        // return a clone of rbuf
        self.rbuf.clone()
    }

    pub fn get_rpos(&self) -> usize {
        match self.rpos {
            None => self.rbuf.get_pos(),
            Some(rp) => self.rbuf.len() + rp
        }
    }

    pub fn set_rpos(&mut self, index: usize) -> ZResult<()>  {
        let rlen = self.rbuf.len();
        if index < rlen {
            self.rpos = None;
            self.rbuf.set_pos(index)
        } else {
            let index = index - rlen;
            if index < self.wpos {
                self.rpos = Some(index);
                Ok(())
            } else {
                Err(zerror!(ZErrorKind::BufferUnderflow { missing: index - self.wpos + 1 }))
            }
        }
    }

    pub fn move_rpos(&mut self, n: usize) -> ZResult<()> {
        if n == 0 { return Ok(()) };
        match self.rpos {
            None => {
                let readable = self.rbuf.readable();
                if n < readable {
                    self.rbuf.move_pos(n)
                } else {
                    self.rbuf.move_pos(readable)?;
                    self.rpos = Some(0);
                    self.move_rpos(n - readable)
                }
            }
            Some(rp) => {
                if n <= self.wpos - rp {
                    self.rpos = Some(rp + n);
                    Ok(())
                } else {
                    Err(zerror!(ZErrorKind::BufferUnderflow { missing: n - (self.wpos - rp) }))
                }
            }
        }
    }

    #[inline]
    pub fn can_read(&self) -> bool {
        match self.rpos {
            None => true,
            Some(rp) => rp < self.wpos
        }
    }

    #[inline]
    pub fn readable(&self) -> usize {
        match self.rpos {
            None => self.rbuf.readable() + self.wpos,
            Some(rp) => self.wpos - rp
        }
    }

    pub fn read(&mut self) -> ZResult<u8> {
        match self.rpos {
            None => {
                let b = self.rbuf.read()?;
                if !self.rbuf.can_read() {
                    self.rpos = Some(0);
                }
                Ok(b)
            }
            Some(rp) => {
                if rp < self.wpos {
                    let b = self.wbuf.as_mut().unwrap()[rp];
                    self.rpos = Some(rp+1);
                    Ok(b)
                } else {
                    Err(zerror!(ZErrorKind::BufferUnderflow { missing: 1 }))  
                }
            }
        }
    }

    pub fn read_bytes(&mut self,  bs: &mut [u8]) -> ZResult<()> {
        let to_read = bs.len();
        match self.rpos {
            None => {
                let readable = self.rbuf.readable();
                if to_read < readable {
                    self.rbuf.read_bytes(bs)
                } else {
                    let (left, right) = bs.split_at_mut(readable);
                    self.rbuf.read_bytes(left)?;
                    self.rpos = Some(0);
                    self.read_bytes(right)
                }
            }
            Some(rp) => {
                if to_read <= self.wpos - rp {
                    bs.copy_from_slice(&self.wbuf.as_ref().unwrap()[rp .. rp+to_read]);
                    self.rpos = Some(rp + to_read);
                    Ok(())
                } else {
                    Err(zerror!(ZErrorKind::BufferUnderflow { missing: to_read - (self.wpos - rp) }))
                }
            }
        }
    }

    pub fn write(&mut self, b: u8) {
        self.make_writable();
        self.wbuf.as_mut().unwrap()[self.wpos] = b;
        self.wpos += 1;
    }

    // NOTE: this is different from add_slice() as this makes a copy of bytes into wbuf.
    pub fn write_bytes(&mut self, s: &[u8]) {
        let mut len = s.len();

        let mut offset = 0;
        while len > 0 {
            self.make_writable();
            let writable = self.wbuf.as_ref().unwrap().len() - self.wpos;
            let to_write = std::cmp::min(writable, len);
            let dest = &mut self.wbuf.as_mut().unwrap()[self.wpos .. self.wpos+to_write];
            dest.copy_from_slice(&s[offset .. offset+to_write]);
            self.wpos += to_write;
            offset += to_write;
            len -= to_write;
        }
    }

    pub fn add_slice(&mut self, slice: ArcSlice) {
        // if something was written in wbuf, force move of wbuf in rbuf 
        if self.wpos > 0 {
            self.wbuf_to_rbuf();
        }
        // add the new slice in rbuf
        self.rbuf.add_slice(slice);
    }

}

impl fmt::Display for WBuf {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "WBuf{{ rpos: {:?}, wpos: {}, rbuf len: {}, wbuf len: {} }}",
            self.rpos, self.wpos, self.rbuf.len(), self.wbuf.as_ref().map_or(0, |v| v.len()))
    }
}

impl fmt::Debug for WBuf {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "WBuf{{ rpos: {:?}, wpos: {},\n   rbuf: {:?}\n   wbuf:{:02x?}\n}}",
            self.rpos, self.wpos, self.rbuf, self.wbuf)
    }
}
  


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wbuf() {
        let mut buf = WBuf::new(64);
        println!("\n--- INIT --- \nbuf: {:?}", buf);
        assert_eq!(0, buf.readable());

        for i in 0..10 {
            buf.write(i);
        }
        println!("\n--- After 10 writes --- \nbuf: {:?}", buf);
        assert_eq!(10, buf.readable());

        // Msg containing a "payload" slice to insert in buf
        let mut v: Vec<u8> = vec![0; 64];
        for i in 13..23 {
            v[i] = 0xFF;
        }
        let msg = Arc::new(v);
        let payload = ArcSlice::new(msg.clone(), 13, 23);

        buf.add_slice(payload);
        println!("\n--- After add payload --- \nbuf: {:?}", buf);
        assert_eq!(10+10, buf.readable());

        for i in 0..100 {
            buf.write(i);
        }
        println!("\n--- After 100 writes --- \nbuf: {:?}", buf);
        assert_eq!(10+10+100, buf.readable());

        // test as_ioslices()
        let ioslices = buf.as_ioslices();
        assert_eq!(4, ioslices.len());
        assert_eq!(Some(&[0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9][..]), ioslices[0].get(0..10));
        assert_eq!(Some(&[0xFFu8, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF][..]), ioslices[1].get(0..10));
        assert_eq!(64, ioslices[2].len());
        assert_eq!(36, ioslices[3].len());

        // test read()
        for i in 0..10 {
            assert_eq!(i as u8, buf.read().unwrap());
        }
        println!("\n--- After 10 read --- \nbuf: {:?}", buf);
        assert_eq!(10+100, buf.readable());

        for _i in 0..10 {
            assert_eq!(0xFF as u8, buf.read().unwrap());
        }
        println!("\n--- After 10 read --- \nbuf: {:?}", buf);
        assert_eq!(100, buf.readable());

        for i in 0..100 {
            assert_eq!(i as u8, buf.read().unwrap());
        }
        println!("\n--- After 100 read --- \nbuf: {:?}", buf);
        assert_eq!(0, buf.readable());

        // test set_rpos()
        for index in 0..10+10+100 {
            buf.set_rpos(index).unwrap();
            assert_eq!(120-index , buf.readable());
        }

        // test read_bytes
        let mut bs = [0u8; 5];
        buf.set_rpos(0).unwrap();
        buf.read_bytes(&mut bs).unwrap();
        assert_eq!([0x00, 0x01, 0x02, 0x03, 0x04], bs);
        assert_eq!(10+10+100 - 5, buf.readable());

        buf.set_rpos(8).unwrap();
        buf.read_bytes(&mut bs).unwrap();
        assert_eq!([0x08, 0x09, 0xff, 0xff, 0xff], bs);
        assert_eq!(10+10+100 - 8-5, buf.readable());

        buf.set_rpos(18).unwrap();
        buf.read_bytes(&mut bs).unwrap();
        assert_eq!([0xff, 0xff, 0x00, 0x01, 0x02], bs);
        assert_eq!(10+10+100 - 18-5, buf.readable());

        buf.set_rpos(30).unwrap();
        buf.read_bytes(&mut bs).unwrap();
        assert_eq!([0x0a, 0x0b, 0x0c, 0x0d, 0x0e], bs);
        assert_eq!(10+10+100 - 30-5, buf.readable());

        buf.set_rpos(82).unwrap();
        buf.read_bytes(&mut bs).unwrap();
        assert_eq!([0x3e, 0x3f, 0x40, 0x41, 0x42], bs);
        assert_eq!(10+10+100 - 82-5, buf.readable());

        buf.set_rpos(82).unwrap();
        buf.read_bytes(&mut bs).unwrap();
        assert_eq!([0x3e, 0x3f, 0x40, 0x41, 0x42], bs);
        assert_eq!(10+10+100 - 82-5, buf.readable());

        // create a 2nd WBuf that will share the same "payload" slice
        let mut buf2 = WBuf::new(64);
        for i in 0..50 {
            buf2.write(i);
        }
        let payload = ArcSlice::new(msg.clone(), 13, 23);
        buf2.add_slice(payload);
        assert!(buf2.can_read());
        assert_eq!(50+10, buf2.readable());
        for i in 0..50 {
            assert_eq!(i as u8, buf2.read().unwrap());
        }
        for _i in 0..10 {
            assert_eq!(0xFF as u8, buf2.read().unwrap());
        }

    }


}




