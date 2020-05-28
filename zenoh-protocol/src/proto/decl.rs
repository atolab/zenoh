
use crate::core::{ZInt, ResKey};

pub mod id {
    // Declarations
    pub const RESOURCE            : u8 = 0x01;
    pub const PUBLISHER           : u8 = 0x02;
    pub const SUBSCRIBER          : u8 = 0x03;
    pub const QUERYABLE           : u8 = 0x04;

    pub const FORGET_RESOURCE     : u8 = 0x11;
    pub const FORGET_PUBLISHER    : u8 = 0x12;
    pub const FORGET_SUBSCRIBER   : u8 = 0x13;
    pub const FORGET_QUERYABLE    : u8 = 0x14;

    // SubModes
    pub const MODE_PUSH           : u8 = 0x00;
    pub const MODE_PULL           : u8 = 0x01;
    pub const PERIOD              : u8 = 0x80;
}


#[derive(Debug, Clone, PartialEq)]
pub enum Reliability { BestEffort, Reliable }

#[derive(Debug, Clone, PartialEq)]
pub enum SubMode { Push, Pull }

#[derive(Debug, Clone, PartialEq)]
pub struct Period { 
    pub origin: ZInt,
    pub period: ZInt,
    pub duration: ZInt
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubInfo {
    pub reliability: Reliability,
    pub mode: SubMode,
    pub period: Option<Period>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Declaration {
    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |K|X|X| RESOURCE|
    /// +---------------+
    /// ~      RID      ~
    /// +---------------+
    /// ~    ResKey     ~ if  K==1 then only numerical id
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
    /// |K|X|X|   PUB   |
    /// +---------------+
    /// ~    ResKey     ~ if  K==1 then only numerical id
    /// +---------------+
    Publisher {
        key: ResKey
    },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |K|X|X|  F_PUB  |
    /// +---------------+
    /// ~    ResKey     ~ if  K==1 then only numerical id
    /// +---------------+
    ForgetPublisher {
        key: ResKey
    },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |K|S|R|   SUB   |  R for Reliable
    /// +---------------+
    /// ~    ResKey     ~ if K==1 then only numerical id
    /// +---------------+
    /// |    SubMode    | if S==1. Otherwise: SubMode=Push
    /// +---------------+
    /// ~    Period     ~ if SubMode && PERIOD. Otherwise: None
    /// +---------------+
    Subscriber {
        key: ResKey,
        info: SubInfo
    },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |K|X|X|  F_SUB  |
    /// +---------------+
    /// ~    ResKey     ~ if  K==1 then only numerical id
    /// +---------------+
    ForgetSubscriber {
        key: ResKey
    },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |K|X|X|  QABLE  |
    /// +---------------+
    /// ~     ResKey    ~ if  K==1 then only numerical id
    /// +---------------+
    Queryable {
        key: ResKey
    },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |K|X|X| F_QABLE |
    /// +---------------+
    /// ~    ResKey     ~ if  K==1 then only numerical id
    /// +---------------+
    ForgetQueryable {
        key: ResKey
    },
}
