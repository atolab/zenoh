
use crate::core::{ZInt, ResKey };

pub mod id {
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
    /// |X|X|X| RESOURCE|
    /// +---------------+
    /// ~      RID      ~
    /// +---------------+
    /// ~ Resource Key ~
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
    /// |X|X|X|   PUB   |
    /// +---------------+
    /// ~      ResKey   ~
    /// +---------------+
    Publisher {
        key: ResKey
    },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|X|   F_PUB   |
    /// +---------------+
    /// ~      ResKey   ~
    /// +---------------+
    ForgetPublisher {
        key: ResKey
    },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|M|   SUB   |
    /// +---------------+
    /// ~      ResKey   ~
    /// +---------------+
    /// ~    SubMode    ~ if M==1
    /// +---------------+
    Subscriber {
        key: ResKey,
        mode: SubMode
    },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|X|   F_SUB   |
    /// +---------------+
    /// ~      ResKey   ~
    /// +---------------+
    ForgetSubscriber {
        key: ResKey
    },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|X|  STORE  |
    /// +---------------+
    /// ~     ResKey    ~
    /// +---------------+
    Storage {
        key: ResKey
    },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|X| F_STORE |
    /// +---------------+
    /// ~      ResKey   ~
    /// +---------------+
    ForgetStorage {
        key: ResKey
    },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|X|  EVAL   |
    /// +---------------+
    /// ~      ResKey   ~
    /// +---------------+
    Eval {
        key: ResKey
    },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|X| F_EVAL  |
    /// +---------------+
    /// ~      ResKey   ~
    /// +---------------+
    ForgetEval {
        key: ResKey
    }
}
