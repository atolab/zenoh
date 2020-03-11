use super::ZInt;

// Properties accepted in open()
pub const ZN_USER_KEY: ZInt = 0x50;
pub const ZN_PASSWD_KEY: ZInt = 0x51;

// Properties returned by info()
pub const ZN_INFO_PID_KEY: ZInt      = 0x00;
pub const ZN_INFO_PEER_KEY: ZInt     = 0x01;
pub const ZN_INFO_PEER_PID_KEY: ZInt = 0x02;
