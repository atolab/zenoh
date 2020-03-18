use crate::core::{
    AtomicZInt,
    ZInt,
    ZError,
    ZErrorKind,
    ZResult
};

use crate::zerror;
use std::sync::atomic::Ordering;


#[derive(Debug, Clone)]
pub struct InvalidResolution { pub msg: String }

#[derive(Debug)]
pub struct SeqNumGenerator {
    next_sn: AtomicZInt,
    semi_int: ZInt,
    resolution: ZInt
}

/// Sequence Number Generation
/// 
/// Zenoh sequence number have a negotiable resolution. Each session can
/// ideally negotiate its resolution and use it across all conduits. 
/// 
/// The [`SeqNumGenerator`][SeqNumGenerator] encapsulates the generation of sequence numbers
/// along with a [`precede`][SeqNumGenerator::precede] predicate that checks whether two
/// sequence numbers are in the precede relationship.
/// 
impl SeqNumGenerator {
    /// Create a new sequence number generator with a given resolution.
    /// 
    /// # Arguments
    /// * `sn0` - The initial sequence number. It is a good practice to initialize the
    ///           sequence number generator with a random number
    ///
    /// * `resolution` - The resolution, in bits, to be used for the sequence number generator. 
    ///                  As a consequence of wire zenoh's representation of sequence numbers
    ///                  this should be a multiple of 7.
    /// 
    pub fn make(sn0: ZInt, resolution: ZInt) -> ZResult<Self> {
        if sn0 < resolution {
            Ok(SeqNumGenerator { next_sn: AtomicZInt::new(sn0), semi_int: resolution >> 1, resolution : resolution })
        }
        else {
            Err(zerror!(ZErrorKind::InvalidResolution {
                descr: format!("The initial sequence number should be smaller than the resolution")
            }))
        }
    }

    /// Generates the next sequence number
    pub fn next(&self) -> ZInt {
        let n = self.next_sn.fetch_add(1, Ordering::SeqCst);
        let mut a = n + 1;
        if a >= self.resolution {        
            let b = a % self.resolution;
            self.next_sn.compare_and_swap(a, b, Ordering::SeqCst);    
            a = b;
        }
        a
    }

    /// Checks to see if two sequence number are in a precedence relationship, 
    /// while taking into account roll backs. 
    /// 
    /// Two case are considered:
    /// 
    /// ## Case 1: sna < snb
    /// 
    /// In this case *sna* precedes *snb* iff (snb - sna) <= semi_int where 
    /// semi_int is defined as half the sequence number resolution. 
    /// In other terms, sna precedes snb iff there are less than half 
    /// the length for the interval that separates them. 
    /// 
    /// ## Case 2: sna > snb
    /// 
    /// In this case *sna* precedes *snb* iff (sna - snb) > semi_int. 
    /// 
    /// # Arguments
    /// 
    /// * `sna` -  The sequence number which should be checked for precedence relation. 
    /// 
    /// * `snb` -  The sequence number which should be taken as reference to check the 
    ///            precedence of `sna`
    pub fn precedes(&self, sna: ZInt, snb: ZInt) -> bool {
        if snb > sna {
            snb - sna <= self.semi_int
        } else {
            sna - snb > self.semi_int 
        } 
    }
}