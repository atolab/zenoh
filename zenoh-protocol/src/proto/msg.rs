use crate::zerror;
use crate::core::{ZError, ZErrorKind, ZInt, PeerId, Property, ResKey, TimeStamp};
use crate::io::ArcSlice;
use crate::link::Locator;
use super::decl::Declaration;
use std::sync::Arc;

//
// @TODO: Update declare messages to include rid and postfix.
//
//
// zenoh's protocol messages IDs
//
#[allow(dead_code)]
pub mod id {
    // Messages
    pub const SCOUT         :   u8 	= 0x01;
    pub const HELLO         :   u8 	= 0x02;

    pub const OPEN          :   u8 	= 0x03;
    pub const ACCEPT        :   u8 	= 0x04;
    pub const CLOSE         :   u8 	= 0x05;

    pub const DECLARE       :   u8 	= 0x06;

    pub const DATA  	    : 	u8 	= 0x07;

    pub const QUERY         :   u8  = 0x08;
    pub const PULL          :   u8  = 0x09;

    pub const SYNC  	    :	u8	= 0x0a;
    pub const ACK_NACK      :   u8  = 0x0b;

    pub const KEEP_ALIVE    :   u8  = 0x0c;

    pub const PING_PONG     :   u8 	= 0x0d;

    // Decorators
    pub const REPLY         :   u8 =  0x10;
    pub const BATCH         :   u8  = 0x11;
    pub const FRAGMENT      :   u8  = 0x12;
    pub const CONDUIT	    :   u8	= 0x13;
    pub const MIGRATE	    :   u8	= 0x14;
    pub const R_SPACE       :   u8  = 0x15;
    pub const PROPERTIES    :   u8  = 0x16;

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
    pub const T        :   u8  = 0x40;

    pub const F        :   u8  = 0x80;
    pub const I        :   u8  = 0x80;
    pub const S        :   u8  = 0x80;
    pub const Z        :   u8  = 0x80;

    pub const MID_MASK      :   u8  = 0x1f;
    pub const HEADER_MASK   :   u8  = 0xe0;

    pub const BROKER                : u8    =  0x01;
    pub const ROUTER                : u8    =  0x02;
    pub const PEER                  : u8    =  0x04;
    pub const CLIENT                : u8    =  0x08;

    pub fn is_r_set(rsb: u8) -> bool { rsb & R != 0}
    pub fn has_flag(header: u8, flag: u8) -> bool { header & flag != 0 }
    pub fn mid(header: u8) -> u8 { header & MID_MASK}
    pub fn flags(header: u8) -> u8 {header & HEADER_MASK}
}

// Flags used for DataInfo
pub mod info_flag {
    pub const SRCID : u8 = 0x01;
    pub const SRCSN : u8 = 0x02;
    pub const BKRID : u8 = 0x04;
    pub const BKRSN : u8 = 0x08;
    pub const TS    : u8 = 0x10;
    pub const KIND  : u8 = 0x20;
    pub const ENC   : u8 = 0x40;
}

#[repr(u8)]
#[derive(Debug, Clone, PartialEq)]
pub enum WhatAmI {
    Broker = flag::BROKER,
    Router = flag::ROUTER,
    Peer = flag::PEER,
    Client = flag::CLIENT
}

impl WhatAmI {
    pub fn from_zint(value: ZInt) -> Result<WhatAmI, ZError> {
        if value == WhatAmI::to_zint(&WhatAmI::Broker) { 
            Ok(WhatAmI::Broker)
        } else if value == WhatAmI::to_zint(&WhatAmI::Router) {
            Ok(WhatAmI::Router)
        } else if value == WhatAmI::to_zint(&WhatAmI::Peer) {
            Ok(WhatAmI::Peer)
        } else if value == WhatAmI::to_zint(&WhatAmI::Client) {   
            Ok(WhatAmI::Client) 
        } else {
            Err(zerror!(ZErrorKind::Other{
                descr: format!("Invalid WhatAmI field ({})", value)
            }))
        }
    }

    pub fn to_zint(value: &WhatAmI) -> ZInt {
        value.clone() as ZInt
    }
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
pub struct DataInfo {
    pub(in super) header: u8,
    pub(in super) source_id: Option<PeerId>,
    pub(in super) source_sn: Option<ZInt>,
    pub(in super) fist_broker_id: Option<PeerId>,
    pub(in super) fist_broker_sn: Option<ZInt>,
    pub(in super) timestamp: Option<TimeStamp>,
    pub(in super) kind: Option<ZInt>,
    pub(in super) encoding: Option<ZInt>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReplySource {
    Eval,
    Storage
}

#[derive(Debug, Clone, PartialEq)]
pub enum QueryConsolidation {
    None,
    LastBroker,
    Incremental
    // @TODO: add more if necessary
}

impl Default for QueryConsolidation {
    fn default() -> Self { QueryConsolidation::Incremental }
}

// @TODO: The query target is incomplete
#[derive(Debug, Clone, PartialEq)]
pub enum Target {
    BestMatching,
    Complete {n: ZInt},
    All,
    None,
}

impl Default for Target {
    fn default() -> Self { Target::BestMatching }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct QueryTarget {
    pub storage: Target,
    pub eval: Target,
    // @TODO: finalise
}

#[derive(Debug, Clone, PartialEq)]
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
    Hello { whatami: WhatAmI, locators: Option<Vec<Locator>>},


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
    Open { version: u8, whatami: WhatAmI, pid: PeerId, lease: ZInt, locators: Option<Vec<Locator>> },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|W|  ACCEPT |
    /// +---------------+
    /// ~    whatami    ~ if (W==1) -- otherwise the answer if from a broker
	/// +---------------+
    /// ~     OPID      ~  -- PID of the sender of the OPEN
    /// +---------------+  -- PID of the "responder" to the OPEN, i.e. accepting entity.
    /// ~     APID      ~
	/// +---------------+
	/// ~ lease_period  ~
    /// +---------------+
    Accept { whatami: WhatAmI, opid: PeerId, apid: PeerId, lease: ZInt },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|P|  CLOSE  |
    /// +---------------+
    /// ~      PID      ~  if P==1
    /// +---------------+
    /// |     Reason    |
    /// +---------------+
    ///
    Close{ pid: Option<PeerId>, reason: u8 },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|P| K_ALIVE |
    /// +---------------+
    /// ~      PID      ~ -- If P == 1
    /// +---------------+
    KeepAlive { pid: Option<PeerId> },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|X| DECLARE |
    /// +---------------+
    /// ~      SN       ~
    /// +---------------+
    /// ~ [Declaration] ~
    /// +---------------+
    Declare {sn: ZInt, declarations: Vec<Declaration> },

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
    Data { reliable: bool, sn: ZInt, key: ResKey, info: Option<ArcSlice>, payload: ArcSlice },

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
    Pull { sn: ZInt, key: ResKey, pull_id: ZInt, max_samples: Option<ZInt>},

    /// +-+-+-+---------+
    /// |X|T|C|  QUERY  |
    /// +-+-+-+---------+
    /// ~      sn       ~
    /// +---------------+
    /// ~    ResKey     ~ if  C==1 then only numerical id
    /// +---------------+
    /// ~   predicate   ~
    /// +---------------+
    /// ~      qid      ~
    /// +---------------+
    /// ~     target    ~ if T==1
    /// +---------------+
    /// ~ consolidation ~
    /// +---------------+
    Query {sn: ZInt, key: ResKey, predicate: String, qid: ZInt, target: Option<QueryTarget>, consolidation: QueryConsolidation },

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
    /// E -> the message comes from an eval
    /// F -> the message is a REPLY_FINAL 
    ///
    /// The **Reply** is a message decorator for eithr:
    ///   - the **Data** messages that result from a query
    ///   - or a **KeepAlive** message in case the message is a
    ///     STORAGE_FINAL, EVAL_FINAL or REPLY_FINAL.
    ///  The **replier-id** (eval or storage id) is represented as a byte-array.
    // Reply { is_final: bool, qid: ZInt, source: ReplySource, replier_id: Option<PeerId> }


// The MessageKind is used to provide additional information concerning the message
// that is often provided through message decorators.
#[derive(Debug, Clone, PartialEq)]
pub enum MessageKind {
    FullMessage,
    FirstFragment {n: Option<ZInt>},
    InbetweenFragment,
    LastFragment,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReplyContext {
    pub(in super) is_final: bool,
    pub(in super) qid: ZInt,
    pub(in super) source: ReplySource,
    pub(in super) replier_id: Option<PeerId>
}

impl ReplyContext {
    // Note: id replier_id=None flag F is set, meaning it's a REPLY_FINAL
    pub fn make(qid: ZInt, source: ReplySource, replier_id: Option<PeerId>) -> ReplyContext {
        ReplyContext {
            is_final: replier_id.is_none(),
            qid,
            source,
            replier_id
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    pub(crate) has_decorators: bool,
    pub(crate) cid: ZInt,
    pub(crate) header: u8,
    pub(crate) body: Body,
    pub(crate) kind: MessageKind,
    pub(crate) reply_context: Option<ReplyContext>,
    pub(crate) properties: Option<Arc<Vec<Property>>>
}

impl Message {
    pub fn make_scout(what: Option<ZInt>, cid: Option<ZInt>, ps: Option<Arc<Vec<Property>>>) -> Message {
        let header = match what {
            Some(_) => id::SCOUT | flag::W,
            None => id::SCOUT
        };
        Message {
            has_decorators: cid.is_some() || ps.is_some(),
            cid: cid.unwrap_or(0),
            header,
            body: Body::Scout { what },
            kind: MessageKind::FullMessage,
            reply_context: None,
            properties: ps            
        }
    }

    pub fn make_hello(whatami: WhatAmI, locators: Option<Vec<Locator>>, cid: Option<ZInt>, ps: Option<Arc<Vec<Property>>>) -> Message {
        let wflag = match whatami {
            WhatAmI::Broker=> 0,
            _ => flag::W
        };
        let lflag = if locators.is_some() { flag::L } else { 0 };
        let header = id::HELLO | wflag | lflag;
        Message {
            has_decorators: cid.is_some() || ps.is_some(),
            cid: cid.unwrap_or(0),
            header,
            body: Body::Hello { whatami, locators },
            kind: MessageKind::FullMessage,
            reply_context: None,
            properties: ps        }
    }

    pub fn make_open(version: u8, whatami: WhatAmI, pid: PeerId, lease: ZInt, locators: Option<Vec<Locator>>, cid: Option<ZInt>, ps: Option<Arc<Vec<Property>>>) -> Message {
        let wflag = match whatami {
            WhatAmI::Broker=> 0,
            _ => flag::W
        };
        let lflag = if locators.is_some() { flag::L } else { 0 };
        let header = id::OPEN | wflag | lflag;
        Message {
            has_decorators: cid.is_some() || ps.is_some(),
            cid: cid.unwrap_or(0),
            header,
            body: Body::Open { version, whatami, pid, lease, locators },
            kind: MessageKind::FullMessage,
            reply_context: None,
            properties: ps            
        }
    }
    
    pub fn make_accept(whatami: WhatAmI, opid: PeerId, apid: PeerId, lease: ZInt, cid: Option<ZInt>, ps: Option<Arc<Vec<Property>>>) -> Message {
        let wflag = match whatami {
            WhatAmI::Broker=> 0,
            _ => flag::W
        };
        let header = id::ACCEPT | wflag;
        Message {
            has_decorators: cid.is_some() || ps.is_some(),
            cid: cid.unwrap_or(0),
            header,
            body: Body::Accept { whatami, opid, apid, lease },
            kind: MessageKind::FullMessage,
            reply_context: None,
            properties: ps
         }
    }

    pub fn make_close(pid: Option<PeerId>, reason: u8, cid: Option<ZInt>, ps: Option<Arc<Vec<Property>>>) -> Message {
        let header = match pid {
            Some(_) => id::CLOSE | flag::P,
            None    => id::CLOSE,
        };
        Message {
            has_decorators: cid.is_some() || ps.is_some(),
            cid: cid.unwrap_or(0),
            header,
            body: Body::Close { pid, reason },
            kind: MessageKind::FullMessage,
            reply_context: None,
            properties: ps    
        }
    }
    pub fn make_keep_alive(pid: Option<PeerId>, reply_context: Option<ReplyContext>, cid: Option<ZInt>, ps: Option<Arc<Vec<Property>>>) -> Message {
        let header = match pid {
            Some(_) => id::KEEP_ALIVE | flag::P,
            None    => id::KEEP_ALIVE,
        };
        Message {
            has_decorators: cid.is_some() || ps.is_some() || reply_context.is_some(),
            cid: cid.unwrap_or(0),
            header,
            body: Body::KeepAlive { pid },
            kind: MessageKind::FullMessage,
            reply_context,
            properties: ps
        }
    }

    pub fn make_declare(sn: ZInt, declarations: Vec<Declaration>, cid: Option<ZInt>, ps: Option<Arc<Vec<Property>>>) -> Message {
        let header = id::DECLARE;
        Message {
            has_decorators: cid.is_some() || ps.is_some(),
            cid: cid.unwrap_or(0),
            header,
            body: Body::Declare { sn, declarations },
            kind: MessageKind::FullMessage,
            reply_context: None,
            properties: ps
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn make_data(
        kind: MessageKind,
        reliable: bool,
        sn: ZInt,
        key: ResKey,
        info: Option<ArcSlice>,
        payload: ArcSlice,
        reply_context: Option<ReplyContext>,
        cid: Option<ZInt>,
        ps: Option<Arc<Vec<Property>>> ) -> Message
    {
        let iflag = if info.is_some() { flag::I } else { 0 };
        let rflag = if reliable { flag::R } else { 0 };
        let cflag = if key.is_numerical() { flag::C } else { 0 };
        let header = id::DATA | iflag | rflag | cflag;
        Message {
            has_decorators: cid.is_some() || ps.is_some() || reply_context.is_some(),
            cid: cid.unwrap_or(0),
            header,
            body: Body::Data { reliable, sn, key, info, payload },
            kind,
            reply_context,
            properties: ps
        }
    }

    pub fn make_pull(is_final: bool, sn: ZInt, key: ResKey, pull_id: ZInt, max_samples: Option<ZInt>, cid: Option<ZInt>, ps: Option<Arc<Vec<Property>>>) -> Message {
        let fflag = if is_final { flag::F } else { 0 };
        let nflag = if max_samples.is_some() { flag::N } else { 0 };
        let cflag = if key.is_numerical() { flag::C } else { 0 };
        let header = id::PULL | fflag | nflag | cflag;
        Message {
            has_decorators: cid.is_some() || ps.is_some(),
            cid: cid.unwrap_or(0),
            header,
            body: Body::Pull { sn, key, pull_id, max_samples },
            kind: MessageKind::FullMessage,
            reply_context: None,
            properties: ps    
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn make_query(sn: ZInt, key: ResKey, predicate: String, qid: ZInt, target: Option<QueryTarget>, consolidation: QueryConsolidation, cid: Option<ZInt>, ps: Option<Arc<Vec<Property>>>) -> Message {
        let tflag = if target.is_some() { flag::T } else { 0 };
        let cflag = if key.is_numerical() { flag::C } else { 0 };
        let header = id::QUERY | tflag | cflag;
        Message {
            has_decorators: cid.is_some() || ps.is_some(),
            cid: cid.unwrap_or(0),
            header,
            body: Body::Query { sn, key, predicate, qid, target, consolidation },
            kind: MessageKind::FullMessage,
            reply_context: None,
            properties: ps
        }
    }

    pub fn make_ping(hash: ZInt, cid: Option<ZInt>, ps: Option<Arc<Vec<Property>>>) -> Message {
        let header = id::PING_PONG | flag::P;
        Message {
            has_decorators: cid.is_some() || ps.is_some(),
            cid: cid.unwrap_or(0),
            header,
            body: Body::Ping { hash },
            kind: MessageKind::FullMessage,
            reply_context: None,
            properties: ps
        }
    }

    pub fn make_pong(hash: ZInt, cid: Option<ZInt>, ps: Option<Arc<Vec<Property>>>) -> Message {
        let header = id::PING_PONG;
        Message {
            has_decorators: cid.is_some() || ps.is_some(),
            cid: cid.unwrap_or(0),
            header,
            body: Body::Pong { hash },
            kind: MessageKind::FullMessage,
            reply_context: None,
            properties: ps
        }
    }

    pub fn make_sync(reliable: bool, sn: ZInt, count: Option<ZInt>, cid: Option<ZInt>, ps: Option<Arc<Vec<Property>>>) -> Message {
        let rflag = if reliable { flag::R } else { 0 };
        let cflag = if count.is_some() { flag::C } else { 0 };
        let header = id::SYNC | rflag | cflag;
        Message {
            has_decorators: cid.is_some() || ps.is_some(),
            cid: cid.unwrap_or(0),
            header,
            body: Body::Sync { sn, count },
            kind: MessageKind::FullMessage,
            reply_context: None,
            properties: ps
        }
    }

    pub fn make_ack_nack(sn: ZInt, mask: Option<ZInt>, cid: Option<ZInt>, ps: Option<Arc<Vec<Property>>>) -> Message {
        let header = match mask {
            Some(_) => id::ACK_NACK | flag::M,
            None    => id::ACK_NACK,
        };
        Message {
            has_decorators: cid.is_some() || ps.is_some(),
            cid: cid.unwrap_or(0),
            header,
            body: Body::AckNack { sn, mask },
            kind: MessageKind::FullMessage,
            reply_context: None,
            properties: ps
        }
    }

    //@TODO: Add other constructors that impose message invariants

    pub fn has_decorators(&self) -> bool {
        self.has_decorators
    }
    
    // -- Message Predicates
    pub fn is_reliable(&self) -> bool {
        match self.body {
            Body::Data { .. } => self.header & flag::R != 0,
            Body::Declare { .. } => true,
            Body::Pull { .. } => true,
            Body::Query { .. } => true,
            _ => false
        }
    }
    pub fn is_fragment(&self) -> bool {
        match self.kind {
            MessageKind::FullMessage => false,
            _ => true
        }
    }

    pub fn is_reply(&self) -> bool {
        self.reply_context.is_some()
    }

    // -- Accessor
    pub fn get_body(&self) -> &Body {
        &self.body
    }

    pub fn get_properties(&self) -> Option<Arc<Vec<Property>>> {
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