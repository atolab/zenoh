#[derive(Debug, Clone)]
pub struct InvalidResulition { pub msg: String }

#[derive(Debug, Clone)]
pub struct SeqNumGenerator {
    next_sn: ZInt,
    zero: ZInt,
    resolution: ZInt
}

impl SeqNumGenerator {
    pub fn make(sn0: ZInt, resolution: ZInt) -> Result<SeqNumGenerator, InvalidResulition> {
        if sn0 < resolution {
            Ok(SeqNumGenerator { next_sn: sn0, zero: resolution >> 1, resolution : resolution })
        }
        else {
            Err(InvalidResulition("The initial sequence number should be smaller than resolution"))
        }
    }
    pub fn next(&mut self) {
        self.next_sn = (self.next_sn + 1) % self.resolution;
        self.next_sn
    }

    pub fn preceeds(&self, sna: ZInt, snb: ZInt) -> bool {
        //@TODO: finish-up
       false
        
    }
}