
use crate::core::{ZInt, Property};

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


#[derive(Debug, Clone)]
pub enum Declaration {
    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |C|P|X| RESOURCE|
    /// +---------------+
    /// ~      RID      ~
    /// +---------------+
    /// ~    Resource   ~
    /// +---------------+
    /// ~  [Property]   ~
    /// +---------------+
    ///
    /// If C=1 then the textual representation of the resource can be removed
    /// RID represents the compact ID associated with the resource and is always
    /// included
    Resource {
        rid: ZInt,
        resource: Option<String>,
        properties: Vec<Property>
    },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |C|S|P|  PUBSUB |
    /// +---------------+
    /// ~      RID      |
    /// +---------------+
    /// ~    Resource   ~
    /// +---------------+
    PubSub {
        ps_mask : u8,
        rid: ZInt,
        resource: Option<String>
    }
}
