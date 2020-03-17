#[derive(Debug, Clone)]
pub struct InvalidResulition { pub msg: String }

#[derive(Debug, Clone)]
pub struct SeqNumGenerator {
    next_sn: ZInt,
    semi_int: ZInt,
    resolution: ZInt
}

impl SeqNumGenerator {
    pub fn make(sn0: ZInt, resolution: ZInt) -> Result<SeqNumGenerator, InvalidResulition> {
        if sn0 < resolution {
            Ok(SeqNumGenerator { next_sn: sn0, semi_int: resolution >> 1, resolution : resolution })
        }
        else {
            Err(InvalidResulition("The initial sequence number should be smaller than resolution"))
        }
    }

    pub fn next(&mut self) {
        self.next_sn = (self.next_sn + 1) % self.resolution;
        self.next_sn
    }

    /// Checks to see if two sequence number are in a precendence relationship, 
    /// while taking into account roll backs. 
    /// 
    /// Two case are considered:
    /// 
    /// Case 1: 
    /// 
    ///        sna     <      snb
    ///  |------^--------------^-------|
    /// 
    /// Case 2: 
    /// 
    ///        snb     <      sna
    ///  |------^--------------^-------|
    /// 
    /// For Case-1 sna preceeds snb iff (snb - sna) <= semi_int where 
    /// semi_int is defined as half the sequence number resolution. 
    /// In other terms, sna preceeds snb iff there are less than half 
    /// the lenght for the interval that separates them.
    /// 
    /// For Case-2 sna preceeds snb iff (sna - snb) > semi_int.
    /// 
    pub fn preceeds(&self, sna: ZInt, snb: ZInt) -> bool {
        if snb < sna {
            snb - sna <= self.semi_int
        } else {
            sna - snv > self.semi_int 
        }       
    }
}