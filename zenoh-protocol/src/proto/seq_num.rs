use crate::zerror;
use crate::core::{
    AtomicZInt,
    ZInt,
    ZResult
}

#[derive(Debug, Clone)]
pub struct InvalidResulition { pub msg: String }

#[derive(Debug, Clone)]
pub struct SeqNumGenerator {
    next: ZInt,
    zero: ZInt,
    resolution: ZInt
}

impl SeqNumGenerator {
    pub fn make(sn0: ZInt, resolution: ZInt) -> ZResult<Self> {
        if sn0 < resolution {
            Ok(Self { 
                next: sn0, 
                zero: resolution >> 1, 
                resolution : resolution 
            })
        }
        else {
            Err(zerror!(ZErrorKind::Other{
                descr: format!("The initial sequence number should be smaller than the resolution")
            }))
        }
    }

    pub fn next(&mut self) {
        self.next_sn = (self.next_sn + 1) % self.resolution;
        self.next_sn
    }

    pub fn preceeds(&self, sna: ZInt, snb: ZInt) -> bool {
        
        false
        
    }
}