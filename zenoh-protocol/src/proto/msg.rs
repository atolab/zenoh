use crate::core::*;
use super::decl;
use std::sync::Arc;
//
// @TODO: Update declare messages to include rid and postfix.
//
//
// zenoh's protocol messages IDs
//
pub mod id {
    pub const SCOUT         :   u8 	= 0x01;
    pub const HELLO         :   u8 	= 0x02;

    pub const OPEN          :   u8 	= 0x03;
    pub const ACCEPT        :   u8 	= 0x04;
    pub const CLOSE         :   u8 	= 0x05;

    pub const DECLARE       :   u8 	= 0x06;

    pub const DATA  	    : 	u8 	= 0x07;

    pub const QUERY         :   u8  = 0x08;
    pub const PULL          :   u8  = 0x09;

    pub const SYNC  	    :	u8	= 0x10;
    pub const ACK_NACK      :   u8  = 0x11;

    pub const KEEP_ALIVE    :   u8  = 0x12;

    pub const PING_PONG     :   u8 	= 0x13;

    // Decorators
    pub const REPLY         :   u8 =  0x14;
    pub const BATCH         :   u8  = 0x15;
    pub const FRAGMENT      :   u8  = 0x16;
    pub const CONDUIT	    :   u8	= 0x17;
    pub const MIGRATE	    :   u8	= 0x18;
    pub const R_SPACE       :   u8  = 0x19;

    pub const MID_MAX	    :   u8	= 0x1f;
}

pub mod flag {
    pub const C        :   u8  = 0x20;
    pub const M        :   u8  = 0x20;
    pub const P        :   u8  = 0x20;
    pub const W        :   u8  = 0x20;

    pub const E        :   u8  = 0x40;
    pub const L        :   u8  = 0x40;
    pub const N        :   u8  = 0x40;
    pub const R        :   u8  = 0x40;

    pub const F        :   u8  = 0x80;
    pub const I        :   u8  = 0x80;
    pub const Z        :   u8  = 0x80;

    pub const MID_MASK      :   u8  = 0x1f;
    pub const HEADER_MASK   :   u8  = 0xe0;

    pub const PUSH_READ				: u8 	=  0x01;
    pub const PULL_READ				: u8	=  0x02;
    pub const PERIODIC_PUSH_READ	: u8 	=  0x03;
    pub const PERIODIC_PULL_READ	: u8 	=  0x04;
    pub const PUSH_PULL_READ		: u8 	=  0x05;

    pub const BROKER                : u8    =  0x01;
    pub const PEER                  : u8    =  0x02;
    pub const CLIENT                : u8    =  0x04;



    pub fn is_r_set(rsb: u8) -> bool { rsb & R != 0}
    pub fn has_flag(header: u8, flag: u8) -> bool { header & flag != 0 }
    pub fn mid(header: u8) -> u8 { header & MID_MASK}
    pub fn flags(header: u8) -> u8 {header & HEADER_MASK}
}

pub enum Whatami {
}

#[derive(Debug, Clone)]
pub enum SessionMode {
    Push,
    Pull,
    PeriodicPush(u32),
    PeriodicPull(u32),
    PushPull
}

#[derive(Debug, Clone)]
pub struct SourceInfo {
    id: PeerId,
    sn: ZInt
}

#[derive(Debug, Clone)]
pub struct DataInfo {
    source: Option<SourceInfo>,
    fist_broker: Option<SourceInfo>,
    timestamp: Option<TimeStamp>,
    kind: Option<ZInt>,
    encoding: Option<ZInt>,
}

#[derive(Debug, Clone)]
pub enum ReplySource {
    Eval,
    Storage
}

#[derive(Debug, Clone)]
pub enum QueryConsolidation {
    None,
    LastBroker,
    Incremental
    // @TODO: add more if necessary
}

// @TODO: The query target is incomplete
#[derive(Debug, Clone)]
pub enum Target { None, All, BestMatching, Complete {n: ZInt} }

#[derive(Debug, Clone)]
pub struct QueryTarget {
    storage: Target,
    eval: Target,
    // @TODO: finalise
}
#[derive(Debug, Clone)]
pub enum Body {

    /// The SCOUT message can be sent at any point in time to solicit HELLO messages from
    /// matching parties
    ///
    ///  7 6 5 4 3 2 1 0
	/// +-+-+-+-+-+-+-+-+
	/// |X|X|W|   SCOUT |
	/// +-+-+-+-+-+-+-+-+
	/// ~      what     ~ if (W==1) -- otherwise implicitely scouting for Routers
	/// +-+-+-+-+-+-+-+-+
	///
    /// b0:     Broker
    /// b1:     Router
    /// b2:     Peer
    /// b3:     Client
    /// b4-b13: Reserved
    Scout { what: Option<ZInt> },

    /// The hello message advertise a node and its locators. Locators are expressed as:
  	/// <code>
	///  udp/192.168.0.2:1234
	///  tcp/192.168.0.2.1234
	///  udp/239.255.255.123:5555
	/// <code>
	///
	///  7 6 5 4 3 2 1 0
	/// +-+-+-+-+-+-+-+-+
	/// |X|L|W|  HELLO  |
	/// +---------------+
    /// ~    whatami    ~ if (W==1) -- otherwise the answer if from a broker
	/// +---------------+
    /// ~    Locators   ~ if (L==1) -- otherwise src-address is the locator
    /// +-+-+-+-+-+-+-+-+
    Hello { whatami: Option<ZInt>,  locators: Option<Vec<String>>},


    ///  7 6 5 4 3 2 1 0
	/// +-+-+-+-+-+-+-+-+
	/// |X|L|W|  OPEN   |
	/// +-------+-------+
	/// | VMaj  | VMin  | Protocol Version VMaj.VMin
	/// +-------+-------+
    /// ~    whatami    ~ if (W==1) -- otherwise the answer if from a broker
	/// +---------------+
	/// ~      PID      ~
	/// +---------------+
	/// ~ lease_period  ~
	/// +---------------+
    /// ~    Locators   ~ if (L==1)
	/// +---------------+
    Open { version: u8, pid: PeerId, lease: ZInt, locators: Vec<String> },


    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|X|  ACCEPT |
    /// +---------------+
    /// ~     OPID      ~  -- PID of the sender of the OPEN
    /// +---------------+  -- PID of the "responder" to the OPEN, i.e. accepting entity.
    /// ~     APID      ~
	/// +---------------+
	/// ~ lease_period  ~
    /// +---------------+
    Accept { opid: PeerId, apid: PeerId, lease: ZInt },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|P|  CLOSE  |
    /// +---------------+
    /// ~      PID      ~  if P==1
    /// +---------------+
    /// |     Reason    |
    /// +---------------+
    ///
    Close{ pid: PeerId, reason: u8 },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|P| K_ALIVE |
    /// +---------------+
    /// ~      PID      ~ -- If P == 1
    /// +---------------+

    KeepAlive { pid: PeerId },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|X| DECLARE |
    /// +---------------+
    /// ~      SN       ~
    /// +---------------+
    /// ~ [Declaration] ~
    /// +---------------+
    Declare {sn: ZInt, declarations: Vec<decl::Declaration> },


    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |I|R|C|  DATA   |
    /// +-+-+-+-+-+-+-+-+
    /// ~      sn       ~
    /// +---------------+
    /// ~    ResKey     ~ if  C==1 then only numerical id
    /// +---------------+
    /// ~     Info      ~ if I==1
    /// +---------------+
    /// ~    Payload    ~
    /// +---------------+
    ///
    /// The message is sent on the reliable channel if R==1 best-effort otherwise.
    Data { reliable: bool, sn: ZInt, key: ResKey, info: Option<DataInfo>, payload: Arc<Vec<u8>> },

    /// +-+-+-+---------+
    /// |F|N|C|  PULL   |
    /// +-+-+-+---------+
    /// ~      sn       ~
    /// +---------------+
    /// ~    ResKey     ~ if  C==1 then only numerical id
    /// +---------------+
    /// ~    pullid     ~
    /// +---------------+
    /// ~  maxSamples   ~ if N
    /// +---------------+
    Pull { sn: ZInt, key: ResKey, pull_id: ZInt, max_samples: ZInt},

    /// +-+-+-+---------+
    /// |X|X|C|  QUERY  |
    /// +-+-+-+---------+
    /// ~      sn       ~
    /// +---------------+
    /// ~    ResKey     ~ if  C==1 then only numerical id
    /// +---------------+
    /// ~   predicate   ~
    /// +---------------+
    /// ~      qid      ~
    /// +---------------+
    /// ~     target    ~
    /// +---------------+
    /// ~ consolidation ~
    /// +---------------+
    Query {sn: ZInt, key: ResKey, predicate: String, qid: ZInt, target: QueryTarget, consolidation: QueryConsolidation },

    /// +-+-+-+---------+
    /// |X|X|P| PingPong|
    /// +-+-+-+---------+
    /// ~     Hash      ~
    /// +---------------+
    ///
    /// If P==1 the message is Ping otherwise is Pong
    Ping { hash: ZInt },
    Pong { hash: ZInt },

    /// +-+-+-+---------+
    /// |X|R|C|   SYNC  |
    /// +-+-+-+---------+
    /// ~      SN       ~
    /// +---------------+
    /// ~     Count     ~ if C==1
    /// +---------------+
    ///
    /// Where the message attributes have the following meaning:
    ///
    /// - if R==1 this concerns the reliable channel, otherwise it concerns the unreliable channel.
    ///
    /// - sn is the sequence number of the next message to be transmitted on this channel.
    ///
    /// - count is optionally the  number of unacknowledged messages.
    Sync { sn: ZInt, count: Option<ZInt> },

    /// +-+-+-+---------+
    /// |X|X|M| ACKNACK |
    /// +-+-+-+---------+
    /// ~     SN        ~
    /// +---------------+
    /// ~     Mask      ~ if M==1
    /// +---------------+
    AckNack { sn: ZInt, mask: Option<ZInt> }
}

    /// -- Message Decorators
    ///
    /// The wire format of message decorators is described below. That said,
    /// they are represented as fields in the message.

    /// +-+-+-+---------+
    /// |Z|H|L| CONDUIT |
    /// +-+-+-+---------+
    /// ~      Id       ~ if Z==0
    /// +---------------+
    ///
    /// When Z==1 the compact conduit identifier is used and
    /// the two bits encode the value of the conduit minus 1.
    /// in other terms CID = bHL + 1
    // Conduit { id: ZInt},

    /// +-+-+-+---------+
    /// |F|L|C|  FRAG   |
    /// +-+-+-+---------+
    /// ~    Count        ~ if F==1 and C==1
    /// +---------------+
    ///
    /// - F==1 indicates that this is the first fragment
    /// - L==1 indicates that this is the last fragment
    /// - C==1 indicates optionally the number of remaining fragments
    // Fragment { kind: FragmentKind, count: Option<ZInt> },

    /// +-+-+-+---------+
    /// |X|X|X|  PROPS  |
    /// +-+-+-+---------+
    /// ~   Properties  ~
    /// +---------------+
    ///
    // Properties { properties: Arc<Vec<Property>> },

    /// +-+-+-+---------+
    /// |F|E|X|  REPLY  |
    /// +-+-+-+---------+
    /// ~      qid      ~
    /// +---------------+
    /// ~   replier-id  ~ if not F
    /// +---------------+
    ///
    /// E -> end of the query
    /// F -> storage has finished to send data
    ///
    /// The **Reply** is a message decorator for the **Data** messages that result
    /// from a query. The **replier-id** (eval or storage id) is represented as a byte-array.
    // Reply { qid: ZInt, storage_id: ZInt }


// The MessageKind is used to provide additional information concerning the message
// that is often provided through message decorators.
#[derive(Debug, Clone, PartialEq)]
pub enum MessageKind {
    FullMessage,
    FirstFragment {n: Option<ZInt>},
    InbetweenFragment,
    LastFragment,
}

#[derive(Debug, Clone)]
pub struct ReplyContext {
    pub(in super) qid: ZInt,
    pub(in super) replier: Option<PeerId>
}

#[derive(Debug, Clone)]
pub struct Message {
    pub(in super) cid: ZInt,
    pub(in super) header: u8,
    pub(in super) body: Body,
    pub(in super) kind: MessageKind,
    pub(in super) reply_context: Option<ReplyContext>,
    pub(in super) properties: Option<Arc<Vec<Property>>>
}

impl Message {
    pub fn make_scout(what: Option<ZInt>, ps: Option<Arc<Vec<Property>>>) -> Message {
        let header = match what {
            Some(_) => id::SCOUT | flag::W,
            None => id::SCOUT
        };
        Message { cid: 0, header, body: Body::Scout { what }, kind: MessageKind::FullMessage, reply_context: None, properties: ps }
    }

    pub fn make_data(reliable: bool, cid: Option<ZInt>, sn: ZInt, key: ResKey, info: Option<DataInfo>, payload: Arc<Vec<u8>>, properties: Option<Arc<Vec<Property>>> ) -> Message {
        let id = match cid {
            Some(id) => id,
            None => 0
        };
        let cflag = if key.is_numerical() { flag::C } else { 0 };
        let iflag = if info.is_some() { flag::I } else { 0 };
        let header = id::DATA | cflag | iflag;
        let body = Body::Data { reliable, sn, key, info, payload};
        Message {cid: id, header, body, kind: MessageKind::FullMessage, reply_context: None, properties }
    }

    // Replies are always reliable.
    pub fn make_reply(cid: Option<ZInt>, sn: ZInt, key: ResKey, info: Option<DataInfo>, payload: Arc<Vec<u8>>, reply_context: ReplyContext, properties: Option<Arc<Vec<Property>>> ) -> Message {
        let id = match cid {
            Some(id) => id,
            None => 0
        };
        let cflag = if key.is_numerical() { flag::C } else { 0 };
        let iflag = if info.is_some() { flag::I } else { 0 };
        let header = id::DATA | cflag | iflag;
        let body = Body::Data { reliable: true, sn, key, info, payload};
        Message {cid: id, header, body, kind: MessageKind::FullMessage, reply_context: Some(reply_context), properties }
    }

    //@TODO: Add other constructors that impose message invariants

    // -- Message Predicates
    fn is_reliable(&self) -> bool {
        match self.body {
            Body::Data { reliable: _, sn: _, key: _, info: _, payload: _ } =>
                if self.header & flag::R == 0 { false } else { true },
            Body::Declare { sn: _, declarations: _ } => true,
            Body::Pull { sn: _, key: _, pull_id: _, max_samples: _} => true,
            Body::Query {sn: _, key: _, predicate: _, qid: _, target: _, consolidation: _} => true,
            _ => false
        }
    }
    fn is_fragment(&self) -> bool {
        match self.kind {
            MessageKind::FullMessage => false,
            _ => true
        }
    }

    fn is_reply(&self) -> bool {
        self.reply_context.is_some()
    }

    // -- Accessor
    fn get_properties(&self) -> Option<Arc<Vec<Property>>> {
        match self.properties {
            Some(ref ps) => Some(ps.clone()),
            None => None
        }
    }
}
#[derive(Debug, Clone)]
pub struct MalformedMessage { pub msg: String }

#[derive(Debug, Clone)]
pub struct InvalidMessage { pub msg: String }