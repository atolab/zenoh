
use crate::core::{ZInt, ResKey};

pub mod id {
  use crate::core::ZInt;

  // Declarations
  pub const RESOURCE            :  u8 =  0x01;
  pub const PUBLISHER           :  u8 =  0x02;
  pub const SUBSCRIBER          :  u8 =  0x03;
  pub const STORAGE             :  u8 =  0x04;
  pub const EVAL                :  u8 =  0x05;
  
  pub const FORGET_RESOURCE     :  u8 =  0x11;
  pub const FORGET_PUBLISHER    :  u8 =  0x12;
  pub const FORGET_SUBSCRIBER   :  u8 =  0x13;
  pub const FORGET_STORAGE      :  u8 =  0x14;
  pub const FORGET_EVAL         :  u8 =  0x15;
  
  // SubModes
  pub const MODE_PUSH           : ZInt = 0x00;
  pub const MODE_PULL           : ZInt = 0x01;
  pub const MODE_PERIODIC_PUSH  : ZInt = 0x02;
  pub const MODE_PERIODIC_PULL  : ZInt = 0x03;
}

#[derive(Debug, Clone, PartialEq)]
pub enum SubMode {
    Push,
    Pull,
    PeriodicPush { origin: ZInt, period: ZInt, duration: ZInt },
    PeriodicPull { origin: ZInt, period: ZInt, duration: ZInt }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Declaration {
    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|C| RESOURCE|
    /// +---------------+
    /// ~      RID      ~
    /// +---------------+
    /// ~    ResKey     ~ if  C==1 then only numerical id
    /// +---------------+
    ///    
    /// @Olivier, the idea would be to be able to declare a 
    /// resource using an ID to avoid sending the prefix. 
    /// If we do this however, we open the door to receiving declaration
    /// that may try to redefine an Id... Which BTW may not be so bad, as 
    /// we could use this instead as the rebind. Thoughts?
    Resource {
        rid: ZInt,
        key: ResKey,        
    },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|X|  F_RES  |
    /// +---------------+
    /// ~      RID      ~
    /// +---------------+
    ForgetResource {
        rid: ZInt
    },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|C|   PUB   |
    /// +---------------+
    /// ~    ResKey     ~ if  C==1 then only numerical id
    /// +---------------+
    Publisher {
        key: ResKey
    },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|C|  F_PUB  |
    /// +---------------+
    /// ~    ResKey     ~ if  C==1 then only numerical id
    /// +---------------+
    ForgetPublisher {
        key: ResKey
    },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|S|C|   SUB   |
    /// +---------------+
    /// ~    ResKey     ~ if  C==1 then only numerical id
    /// +---------------+
    /// ~    SubMode    ~ if S==1. Otherwise: SubMode=Push
    /// +---------------+
    Subscriber {
        key: ResKey,
        mode: SubMode
    },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|C|  F_SUB  |
    /// +---------------+
    /// ~    ResKey     ~ if  C==1 then only numerical id
    /// +---------------+
    ForgetSubscriber {
        key: ResKey
    },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|C|  STORE  |
    /// +---------------+
    /// ~     ResKey    ~ if  C==1 then only numerical id
    /// +---------------+
    Storage {
        key: ResKey
    },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|C| F_STORE |
    /// +---------------+
    /// ~    ResKey     ~ if  C==1 then only numerical id
    /// +---------------+
    ForgetStorage {
        key: ResKey
    },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|C|  EVAL   |
    /// +---------------+
    /// ~    ResKey     ~ if  C==1 then only numerical id
    /// +---------------+
    Eval {
        key: ResKey
    },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|X| F_EVAL  |
    /// +---------------+
    /// ~    ResKey     ~ if  C==1 then only numerical id
    /// +---------------+
    ForgetEval {
        key: ResKey
    }
}
