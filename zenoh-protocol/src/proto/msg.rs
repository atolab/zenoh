use crate::zerror;
use crate::core::{ZError, ZErrorKind, ZInt, ZResult, PeerId, Property, ResKey, TimeStamp};
use crate::io::RBuf;
use crate::link::Locator;
use super::decl::Declaration;
use std::sync::Arc;

// Session protocol IDs
pub mod sid {
    // Messages
    pub const SCOUT         : u8 = 0x01;
    pub const HELLO         : u8 = 0x02;

    pub const OPEN          : u8 = 0x03;
    pub const ACCEPT        : u8 = 0x04;
    pub const CLOSE         : u8 = 0x05;

    pub const SYNC  	    : u8 = 0x06;
    pub const ACK_NACK      : u8 = 0x07;

    pub const KEEP_ALIVE    : u8 = 0x08;

    pub const PING_PONG     : u8 = 0x09;

    pub const FRAME         : u8 = 0x0a; 

    // Header mask
    pub const HEADER_MASK   : u8 = 0x0f;
    pub fn mid(header: u8) -> u8 { header & HEADER_MASK }
    pub fn flags(header: u8) -> u8 { header & !HEADER_MASK }
}

// Zenoh protocol IDs
pub mod zid {
    // Messages
    pub const DECLARE       : u8 = 0x01;
    pub const DATA  	    : u8 = 0x02;
    pub const QUERY         : u8 = 0x03;
    pub const PULL          : u8 = 0x04;
    pub const UNIT          : u8 = 0x05;

    // Decorators
    pub const REPLY         : u8 = 0x1e;
    pub const PROPERTIES    : u8 = 0x1f;

    // Header mask
    pub const HEADER_MASK	: u8 = 0x1f;
    pub fn mid(header: u8) -> u8 { header & HEADER_MASK }
    pub fn flags(header: u8) -> u8 { header & !HEADER_MASK }
}

pub mod flag {
    pub const A: u8 = 0x1 << 5; // PeerID       -- if (A==1) then the PeerID is present
    pub const B: u8 = 0x1 << 5; // PingOrPong   -- if (B==1) then the message is Ping, otherwise is Pong
    pub const C: u8 = 0x1 << 4; // Fragment     -- if (C==1) then the Frame is a fragment
    pub const D: u8 = 0x1 << 2; // LeasePeriod  -- if (D==1) then the Lease Period is present
    pub const E: u8 = 0x1 << 6; // FromEval     -- if (E==1) then the Reply comes from an eval
    pub const F: u8 = 0x1 << 5; // Final        -- if (F==1) then this is the final message (e.g., Frame, Reply, Pull)
    pub const G: u8 = 0x1 << 5; // Count        -- if (G==1) then Number of unacknowledged messages is present
    // pub const H: u8;
    pub const I: u8 = 0x1 << 5; // Info         -- if (I==1) then Info struct is present
    // pub const J: u8;
    pub const K: u8 = 0x1 << 7; // ResourceKey  -- if (K==1) then only numerical ID
    pub const L: u8 = 0x1 << 5; // Locators     -- if (L==1) then Locators are present
    pub const M: u8 = 0x1 << 5; // Mask         -- if (M==1) then a Mask is present
    pub const N: u8 = 0x1 << 6; // MaxSamples   -- if (N==1) then the MaxSamples is indicated
    pub const O: u8 = 0x1 << 5; // Options      -- if (O==1) then options are present
    pub const P: u8 = 0x1 << 7; // Properties   -- if (P==1) then properties are present
    // pub const Q: u8;
    pub const R: u8 = 0x1 << 6; // Reliable     -- if (R==1) then it concerns the reliable channel, best-effort otherwise
    pub const S: u8 = 0x1 << 0; // SN Resolution-- if (S==1) then the SN Resolution is present
    pub const T: u8 = 0x1 << 5; // QueryTarget  -- if (T==1) then the query target is present
    // pub const U: u8;
    // pub const V: u8;
    pub const W: u8 = 0x1 << 6; // WhatAmI      -- if (W==1) then WhatAmI is indicated
    // pub const X: u8;
    // pub const Y: u8;
    pub const Z: u8 = 0x1 << 6; // CloseLink    -- if (Z==1) the Close the transport link only

    pub fn has_flag(byte: u8, flag: u8) -> bool { byte & flag != 0 }
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

// Reason for the Close message 
pub mod close_reason {
    pub const GENERIC       : u8 = 0x00;
    pub const UNSUPPORTED   : u8 = 0x01;
    pub const MAX_SESSIONS  : u8 = 0x02;
    pub const MAX_LINKS     : u8 = 0x03;
}

pub type Channel = bool;
pub mod channel {
    use super::Channel;

    pub const BEST_EFFORT   : Channel = false;
    pub const RELIABLE      : Channel = true;
}

pub type WhatAmI = ZInt;
pub mod whatami {
    use super::WhatAmI;
    // b0:     Broker
    // b1:     Router
    // b2:     Peer
    // b3:     Client
    // b4-b13: Reserved
    pub const BROKER: WhatAmI = 0x1 << 0;
    pub const ROUTER: WhatAmI = 0x1 << 1;
    pub const PEER  : WhatAmI = 0x1 << 2;
    pub const CLIENT: WhatAmI = 0x1 << 3;
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

impl DataInfo {
    pub fn make(
        source_id: Option<PeerId>,
        source_sn: Option<ZInt>,
        fist_broker_id: Option<PeerId>,
        fist_broker_sn: Option<ZInt>,
        timestamp: Option<TimeStamp>,
        kind: Option<ZInt>,
        encoding: Option<ZInt>) -> DataInfo
    {
        let mut header = 0u8;
        if source_id.is_some() { header |= info_flag::SRCID }
        if source_sn.is_some() { header |= info_flag::SRCSN }
        if fist_broker_id.is_some() { header |= info_flag::BKRID }
        if fist_broker_sn.is_some() { header |= info_flag::BKRSN }
        if timestamp.is_some() { header |= info_flag::TS }
        if kind.is_some() { header |= info_flag::KIND }
        if encoding.is_some() { header |= info_flag::ENC }
        
        DataInfo { header, source_id, source_sn, fist_broker_id, fist_broker_sn, timestamp, kind, encoding }
    }
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

// Zenoh messages at zenoh level
#[derive(Debug, Clone, PartialEq)]
pub enum ZenohBody {
    /// -- Message Decorators at zenoh level
    ///    Message decorators are used to represent on the wire certain message properties.
    ///    The wire format of message decorators is described below. 

    /// NOTE: 16 bits (2 bytes) may be prepended to the serialized message indicating the total lenght 
    ///       in bytes of the message, resulting in the maximum lenght of a message being 65_536 bytes.
    ///       This is necessary in those stream-oriented transports (e.g., TCP) that do not preserve 
    ///       the boundary of the serialized messages. The length is encoded as little-endian.
    ///       In any case, the lenght of a message must not exceed 65_536 bytes.    
    /// 
    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |-|-|-|  PROPS  |
    /// +-+-+-+---------+
    /// ~    Message    ~
    /// +---------------+
    /// ~   Properties  ~
    /// +---------------+
    ///
    Properties { properties: Arc<Vec<Property>> },

    /// -- Message Decorators at zenoh-transport level
    ///
    /// The wire format of message decorators is described below. That said,
    /// they are represented as fields in the message.

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |-|E|F|  REPLY  |
    /// +-+-+-+---------+
    /// ~      qid      ~
    /// +---------------+
    /// ~   replier_id  ~ if (F==0)
    /// +---------------+
    ///
    /// -- if (E==1) then the message comes from an eval
    /// -- if (F==1) then the message is a REPLY_FINAL 
    ///
    /// The **Reply** is a message decorator for either:
    ///   - the **Data** messages that result from a query
    ///   - or a **Unit** message in case the message is a
    ///     SOURCE_FINAL or REPLY_FINAL.
    ///  The **replier-id** (eval or storage id) is represented as a byte-array.
    /// 
    Reply { is_final: bool, qid: ZInt, source: ReplySource, replier_id: Option<PeerId> },

    /// -- Message at zenoh level
    
    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |-|-|-| DECLARE |
    /// +-+-+-+---------+
    /// ~ [Declaration] ~
    /// +---------------+
    Declare { declarations: Vec<Declaration> },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |K|R|I|  DATA   |
    /// +-+-+-+---------+
    /// ~    ResKey     ~ if (K==1) then only numerical id
    /// +---------------+
    /// ~     Info      ~ if (I==1)
    /// +---------------+
    /// ~    Payload    ~
    /// +---------------+
    ///
    /// - if (R==1) then the message is sent on the reliable channel, best-effort otherwise.
    /// 
    Data { key: ResKey, info: Option<RBuf>, payload: RBuf },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |-|R|-|  UNIT   |
    /// +-+-+-+---------+
    ///
    /// - if (R==1) then the message is sent on the reliable channel, best-effort otherwise.
    /// 
    Unit { },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |K|N|F|  PULL   |
    /// +-+-+-+---------+
    /// ~    ResKey     ~ if (K==1) then only numerical id
    /// +---------------+
    /// ~    pullid     ~
    /// +---------------+
    /// ~  max_samples  ~ if (N==1)
    /// +---------------+
    Pull { key: ResKey, pull_id: ZInt, max_samples: Option<ZInt>},

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |K|-|T|  QUERY  |
    /// +-+-+-+---------+
    /// ~    ResKey     ~ if (K==1) then only numerical id
    /// +---------------+
    /// ~   predicate   ~
    /// +---------------+
    /// ~      qid      ~
    /// +---------------+
    /// ~     target    ~ if (T==1)
    /// +---------------+
    /// ~ consolidation ~
    /// +---------------+
    Query { key: ResKey, predicate: String, qid: ZInt, target: Option<QueryTarget>, consolidation: QueryConsolidation },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ZenohMessage {
    pub(crate) has_decorators: bool,
    pub(crate) header: u8,
    pub(crate) len: u16,
    pub(crate) ch: Channel,
    pub(crate) body: ZenohBody,
    pub(crate) reply_context: Option<ReplyContext>,
    pub(crate) properties: Option<Arc<Vec<Property>>>
}

impl ZenohMessage {
    pub fn make_declare(
        declarations: Vec<Declaration>, 
        ps: Option<Arc<Vec<Property>>>
    ) -> ZenohMessage {
        let header = zid::DECLARE;
        let len: u16 = 0;
        ZenohMessage {
            has_decorators: ps.is_some(),
            header,
            len,
            ch: channel::RELIABLE,
            body: ZenohBody::Declare { declarations },
            reply_context: None,
            properties: ps
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn make_data(
        ch: Channel,
        key: ResKey,
        info: Option<RBuf>,
        payload: RBuf,
        reply_context: Option<ReplyContext>,
        ps: Option<Arc<Vec<Property>>> 
    ) -> ZenohMessage {   
        let kflag = if key.is_numerical() { flag::K } else { 0 };
        let rflag = if ch { flag::R } else { 0 };
        let iflag = if info.is_some() { flag::I } else { 0 };  
        let header = zid::DATA | iflag | rflag | kflag;

        let len: u16 = 0;
        ZenohMessage {
            has_decorators: ps.is_some() || reply_context.is_some(),
            header,
            len,
            ch, 
            body: ZenohBody::Data { key, info, payload },
            reply_context,
            properties: ps
        }
    }

    pub fn make_unit(
        ch: Channel,
        reply_context: Option<ReplyContext>,
        ps: Option<Arc<Vec<Property>>>
    ) -> ZenohMessage {
        let rflag = if ch { flag::R } else { 0 };
        let header = zid::UNIT | rflag;

        let len: u16 = 0;
        ZenohMessage {
            has_decorators: ps.is_some() || reply_context.is_some(),
            header,
            len,
            ch,
            body: ZenohBody::Unit { },
            reply_context,
            properties: ps
        }
    }

    pub fn make_pull(
        is_final: bool, 
        key: ResKey, 
        pull_id: ZInt, 
        max_samples: Option<ZInt>, 
        ps: Option<Arc<Vec<Property>>>
    ) -> ZenohMessage {
        let kflag = if key.is_numerical() { flag::K } else { 0 };
        let nflag = if max_samples.is_some() { flag::N } else { 0 };
        let fflag = if is_final { flag::F } else { 0 };
        let header = zid::PULL | fflag | nflag | kflag;

        let len: u16 = 0;
        ZenohMessage {
            has_decorators: ps.is_some(),
            header,
            len,
            ch: channel::RELIABLE,
            body: ZenohBody::Pull { key, pull_id, max_samples },
            reply_context: None,
            properties: ps    
        }
    }

    // #[allow(clippy::too_many_arguments)]
    pub fn make_query(
        key: ResKey, 
        predicate: String, 
        qid: ZInt,
        target: Option<QueryTarget>, 
        consolidation: QueryConsolidation, 
        ps: Option<Arc<Vec<Property>>>
    ) -> ZenohMessage {
        let kflag = if key.is_numerical() { flag::K } else { 0 };
        let tflag = if target.is_some() { flag::T } else { 0 };
        let header = zid::QUERY | tflag | kflag;

        let len: u16 = 0;
        ZenohMessage {
            has_decorators: ps.is_some(),
            header,
            len,
            ch: channel::RELIABLE,
            body: ZenohBody::Query { key, predicate, qid, target, consolidation },
            reply_context: None,
            properties: ps
        }
    }

    #[inline]
    pub fn has_decorators(&self) -> bool {
        self.has_decorators
    }
    
    // -- Message Predicates
    #[inline]
    pub fn is_reliable(&self) -> bool {
        self.ch
    }

    #[inline]
    pub fn is_reply(&self) -> bool {
        self.reply_context.is_some()
    }

    // -- Accessor
    #[inline]
    pub fn get_body(&self) -> &ZenohBody {
        &self.body
    }

    #[inline]
    pub fn get_properties(&self) -> Option<Arc<Vec<Property>>> {
        match self.properties.as_ref() {
            Some(ps) => Some(ps.clone()),
            None => None
        }
    }
}


// The Payload of the Frame message
#[derive(Debug, Clone, PartialEq)]
pub enum FramePayload {
    Fragment { buffer: RBuf, is_final: bool },
    Messages { messages: Vec<ZenohMessage> }
}

// Zenoh messages at zenoh-session level
#[derive(Debug, Clone, PartialEq)]
pub enum SessionBody {
    /// The SCOUT message can be sent at any point in time to solicit HELLO messages from
    /// matching parties
    ///
    ///  7 6 5 4 3 2 1 0
	/// +-+-+-+-+-+-+-+-+
	/// |P|W|-|-| SCOUT |
	/// +-+-+-+-+-------+
	/// ~    whatmai    ~ if (W==1) -- Otherwise implicitly scouting for Brokers
	/// +---------------+
    /// ~   Properties  ~ if (P==1)
    /// +---------------+
    /// 
    Scout { what: Option<WhatAmI> },

    /// The hello message advertise a node and its locators. Locators are expressed as:
  	/// <code>
	///  udp/192.168.0.2:1234
	///  tcp/192.168.0.2:1234
	///  udp/239.255.255.123:5555
	/// <code>
	///
	///  7 6 5 4 3 2 1 0
	/// +-+-+-+-+-+-+-+-+
	/// |P|W|L|-| HELLO |
	/// +-+-+-+-+-------+
    /// ~    whatmai    ~ if (W==1) -- Otherwise it is from a Broker
	/// +---------------+
    /// ~    Locators   ~ if (L==1) -- Otherwise src-address is the locator
    /// +---------------+
    /// ~   Properties  ~ if (P==1)
    /// +---------------+
    /// 
    Hello { whatami: Option<WhatAmI>, locators: Option<Vec<Locator>> },

    /// -- Messages at zenoh-session level
    ///    The wire format of messages is described below.

    /// NOTE: 16 bits (2 bytes) may be prepended to the serialized message indicating the total lenght 
    ///       in bytes of the message, resulting in the maximum lenght of a message being 65_536 bytes.
    ///       This is necessary in those stream-oriented transports (e.g., TCP) that do not preserve 
    ///       the boundary of the serialized messages. The length is encoded as little-endian.
    ///       In any case, the lenght of a message must not exceed 65_536 bytes.    
    /// 
    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |P|-|O|-|  OPEN |
    /// +-+-+-+-+-------+
    /// | v_maj | v_min | -- Protocol Version VMaj.VMin
    /// +-------+-------+
    /// ~    whatami    ~ -- E.g., client, broker, router, peer or a combination of them
    /// +---------------+
    /// ~   o_peer_id   ~ -- PID of the sender of the OPEN
    /// +---------------+
    /// ~ lease_period  ~ -- Lease period of the session
    /// +---------------+
    /// ~  initial_sn   ~ -- Initial SN proposed by the sender of the OPEN(*)
    /// +-+-+-+-+-+-+-+-+
    /// |-|-|L|-|-|-|-|S| if (O==1)
    /// +-+-+-+-+-+-+-+-+
    /// ~ sn_resolution ~ if (O==1 && S==1) -- Otherwise 2^28 is assumed(**)
    /// +---------------+
    /// ~    Locators   ~ if (O==1 && L==1) -- List of locators the sender of the OPEN is reachable at
    /// +---------------+
    /// ~   Properties  ~ if (P==1)
    /// +---------------+
    /// 
    /// (*)  The Initial SN must be bound to the proposed SN Resolution. Otherwise the OPEN message is considered 
    ///      invalid and it should be discarded by the recipient of the OPEN message.
    /// (**) In case of the Accepter Peer negotiates a smaller SN Resolution (see ACCEPT message) and the proposed 
    ///      Initial SN results to be out-of-bound, the new Agreed Initial SN is calculated according to the
    ///      following modulo operation:
    ///         Agreed Initial SN := (Initial SN_Open) mod (SN Resolution_Accept)
    /// 
    Open { version: u8, whatami: WhatAmI, pid: PeerId, lease: ZInt, initial_sn: ZInt,
           sn_resolution: Option<ZInt>, locators: Option<Vec<Locator>> },

    /// NOTE: 16 bits (2 bytes) may be prepended to the serialized message indicating the total lenght 
    ///       in bytes of the message, resulting in the maximum lenght of a message being 65_536 bytes.
    ///       This is necessary in those stream-oriented transports (e.g., TCP) that do not preserve 
    ///       the boundary of the serialized messages. The length is encoded as little-endian.
    ///       In any case, the lenght of a message must not exceed 65_536 bytes.    
    /// 
    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |P|-|O|-|ACCEPT |
    /// +-+-+-+-+-------+
    /// ~    whatami    ~ -- Client, Broker, Router, Peer or a combination of them
    /// +---------------+
    /// ~   o_peer_id   ~ -- PID of the sender of the OPEN this ACCEPT is for
    /// +---------------+  
    /// ~   a_peer_id   ~ -- PID of the sender of the ACCEPT
    /// +---------------+
    /// ~  initial_sn   ~ -- Initial SN proposed by the sender of the ACCEPT(*)
    /// +-+-+-+-+-+-+-+-+
    /// |-|-|L|-|-|-|D|S| if (O==1)
    /// +-+-+-+-+-+-+---+
    /// ~ sn_resolution + if (O==1 && S==1) -- Agreed SN Resolution(**)
    /// +---------------+
    /// ~ lease_period  ~ if (O==1 && D==1)
    /// +---------------+
    /// ~    Locators   ~ if (O==1 && L==1)
    /// +---------------+
    /// ~   Properties  ~ if (P==1)
    /// +---------------+
    /// 
    /// 
    /// - if S==0 the agreed sequence number resolution is the one indicated in the OPEN message.
    /// - if S==1 the agreed sequence number resolution is the one indicated in this ACCEPT message. 
    ///           The resolution in the ACCEPT must be less or equal than the resolution in the OPEN, 
    ///           otherwise the ACCEPT message is considered invalid and it should be treated as a
    ///           CLOSE message with L==0 by the Opener Peer -- the recipient of the ACCEPT message.
    /// 
    /// - if D==0 the agreed lease period is the one indicated in the OPEN message.
    /// - if D==1 the agreed lease period is the one indicated in this ACCEPT message. 
    ///           The lease period in the ACCEPT must be less or equal than the lease period in the OPEN,
    ///           otherwise the ACCEPT message is considered invalid and it should be treated as a
    ///           CLOSE message with L==0 by the Opener Peer -- the recipient of the ACCEPT message.
    /// 
    /// (*)  The Initial SN is bound to the proposed SN Resolution. 
    /// (**) In case of the SN Resolution proposed in this ACCEPT message is smaller than the SN Resolution 
    ///      proposed in the OPEN message AND the Initial SN contained in the OPEN messages results to be 
    ///      out-of-bound, the new Agreed Initial SN for the Opener Peer is calculated according to the 
    ///      following modulo operation:
    ///         Agreed Initial SN := (Initial SN_Open) mod (SN Resolution_Accept) 
    /// 
    Accept { whatami: WhatAmI, opid: PeerId, apid: PeerId, initial_sn: ZInt, 
             sn_resolution: Option<ZInt>, lease: Option<ZInt>, locators: Option<Vec<Locator>> },

    /// NOTE: 16 bits (2 bytes) may be prepended to the serialized message indicating the total lenght 
    ///       in bytes of the message, resulting in the maximum lenght of a message being 65_536 bytes.
    ///       This is necessary in those stream-oriented transports (e.g., TCP) that do not preserve 
    ///       the boundary of the serialized messages. The length is encoded as little-endian.
    ///       In any case, the lenght of a message must not exceed 65_536 bytes.
    /// 
    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |P|Z|A|-| CLOSE |
    /// +-+-+-+-+-------+
    /// ~    peer_id    ~  if (A==1) -- PID of the target peer.
    /// +---------------+
    /// |     reason    |
    /// +---------------+
    /// ~   Properties  ~ if (P==1)
    /// +---------------+
    /// 
    /// - if Z==0 then close the whole zenoh session.
    /// - if Z==1 then close the transport link the CLOSE message was sent on(e.g., TCP socket) but 
    ///           keep the whole session open. Nevertheless, the session will be automatically closed 
    ///           when the session lease period expires.
    /// 
    Close { pid: Option<PeerId>, reason: u8, link_only: bool },

    /// NOTE: 16 bits (2 bytes) may be prepended to the serialized message indicating the total lenght 
    ///       in bytes of the message, resulting in the maximum lenght of a message being 65_536 bytes.
    ///       This is necessary in those stream-oriented transports (e.g., TCP) that do not preserve 
    ///       the boundary of the serialized messages. The length is encoded as little-endian.
    ///       In any case, the lenght of a message must not exceed 65_536 bytes.
    /// 
    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |P|R|G|-| SYNC  |
    /// +-+-+-+-+-------+
    /// ~      sn       ~ -- Sequence number of the next message to be transmitted on this channel.
    /// +---------------+
    /// ~     count     ~ if (G==1) -- Number of unacknowledged messages. 
    /// +---------------+
    /// ~   Properties  ~ if (P==1)
    /// +---------------+
    ///
    /// - if (R==1) this concerns the reliable channel, otherwise it concerns the unreliable channel.
    /// 
    Sync { ch: Channel, sn: ZInt, count: Option<ZInt> },

    /// NOTE: 16 bits (2 bytes) may be prepended to the serialized message indicating the total lenght 
    ///       in bytes of the message, resulting in the maximum lenght of a message being 65_536 bytes.
    ///       This is necessary in those stream-oriented transports (e.g., TCP) that do not preserve 
    ///       the boundary of the serialized messages. The length is encoded as little-endian.
    ///       In any case, the lenght of a message must not exceed 65_536 bytes.
    /// 
    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |P|R|M|-|ACKNACK|
    /// +-+-+-+-+-------+
    /// ~      sn       ~ 
    /// +---------------+
    /// ~     mask      ~ if (M==1)
    /// +---------------+
    /// ~   Properties  ~ if (P==1)
    /// +---------------+
    /// 
    /// - if (R==1) this concerns the reliable channel, otherwise it concerns the unreliable channel.
    /// 
    AckNack { ch: Channel, sn: ZInt, mask: Option<ZInt> },

    /// NOTE: 16 bits (2 bytes) may be prepended to the serialized message indicating the total lenght 
    ///       in bytes of the message, resulting in the maximum lenght of a message being 65_536 bytes.
    ///       This is necessary in those stream-oriented transports (e.g., TCP) that do not preserve 
    ///       the boundary of the serialized messages. The length is encoded as little-endian.
    ///       In any case, the lenght of a message must not exceed 65_536 bytes. 
    /// 
    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |P|-|A|-|K_ALIVE|
    /// +-+-+-+-+-------+
    /// ~    peer_id    ~ if (A==1) -- Peer ID of the KEEP_ALIVE sender.
    /// +---------------+
    /// ~   Properties  ~ if (P==1)
    /// +---------------+
    /// 
    KeepAlive { pid: Option<PeerId> },

    /// NOTE: 16 bits (2 bytes) may be prepended to the serialized message indicating the total lenght 
    ///       in bytes of the message, resulting in the maximum lenght of a message being 65_536 bytes.
    ///       This is necessary in those stream-oriented transports (e.g., TCP) that do not preserve 
    ///       the boundary of the serialized messages. The length is encoded as little-endian.
    ///       In any case, the lenght of a message must not exceed 65_536 bytes. 
    /// 
    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |P|-|B|-| P_PONG|
    /// +-+-+-+-+-------+
    /// ~     hash      ~
    /// +---------------+
    /// ~   Properties  ~ if (P==1)
    /// +---------------+
    /// 
    /// - if (B==1) the message is Ping otherwise is Pong.
    /// 
    Ping { hash: ZInt },
    Pong { hash: ZInt },

    /// NOTE: 16 bits (2 bytes) may be prepended to the serialized message indicating the total lenght 
    ///       in bytes of the message, resulting in the maximum lenght of a message being 65_536 bytes.
    ///       This is necessary in those stream-oriented transports (e.g., TCP) that do not preserve 
    ///       the boundary of the serialized messages. The length is encoded as little-endian.
    ///       In any case, the lenght of a message must not exceed 65_536 bytes.
    /// 
    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |P|R|F|C| FRAME |
    /// +-+-+-+-+-------+
    /// ~      SN       ~
    /// +---------------+
    /// ~   Properties  ~ if (P==1)
    /// +---------------+
    /// ~  FramePayload ~ -- Either a list of complete Zenoh Messages or a fragment of a single Zenoh Message
    /// +---------------+
    /// 
    /// - if (R==1) then the message is sent on the reliable channel, best-effort otherwise.
    /// - if (C==1) then this FRAME is a fragment.
    /// - if (F==1) then this FRAME message is the last fragment. F==1 is valid iff C==1.
    /// 
    /// NOTE: Only one bit would be sufficient to signal fragmentation in a IP-like fashion as follows:
    ///         - if C==1 then this FRAME is a fragment and more fragment will follow;
    ///         - if C==0 then the message is the last fragment if SN-1 had C==1, 
    ///           otherwise it's a non-fragmented message.
    ///       However, this would require to always perform a two-steps de-serialization: first
    ///       de-serialize the FRAME and then the Payload. This is due to the fact the C==0 is ambigous
    ///       w.r.t. detecting if the FRAME is a fragment or not before SN re-ordering has occured. 
    ///       By using the F bit to only signal whether the FRAME is fragmented or not, it allows to 
    ///       de-serialize the payload in one single pass when C==0 since no re-ordering needs to take
    ///       place at this stage. Then, the F bit is used to detect the last fragment during re-ordering.
    /// 
    Frame { ch: Channel, sn: ZInt, payload: FramePayload }
}


#[derive(Debug, Clone, PartialEq)]
pub struct SessionMessage {
    pub(crate) header: u8,
    pub(crate) len: u16,
    pub(crate) body: SessionBody,
    pub(crate) properties: Option<Arc<Vec<Property>>>
}

impl SessionMessage {
    pub fn make_open(
        version: u8, 
        whatami: WhatAmI, 
        pid: PeerId, 
        lease: ZInt, 
        initial_sn: ZInt,
        sn_resolution: Option<ZInt>,
        locators: Option<Vec<Locator>>, 
        ps: Option<Arc<Vec<Property>>>
    ) -> SessionMessage {
        let pflag = if ps.is_some() { flag::P } else { 0 };
        let oflag = if sn_resolution.is_some() || locators.is_some() { flag::O } else { 0 };
        let header = sid::OPEN | oflag | pflag;

        let len: u16 = 0;
        SessionMessage {
            header,
            len,
            body: SessionBody::Open { version, whatami, pid, lease, initial_sn, sn_resolution, locators },
            properties: ps            
        }
    }
    
    pub fn make_accept(
        whatami: WhatAmI, 
        opid: PeerId, 
        apid: PeerId,
        initial_sn: ZInt,
        sn_resolution: Option<ZInt>, 
        lease: Option<ZInt>,
        locators: Option<Vec<Locator>>,
        ps: Option<Arc<Vec<Property>>>
    ) -> SessionMessage {
        let pflag = if ps.is_some() { flag::P } else { 0 };
        let oflag = if sn_resolution.is_some() || lease.is_some() 
                    || locators.is_some() { flag::O } else { 0 };
        let header = sid::ACCEPT | oflag| pflag;

        let len: u16 = 0;
        SessionMessage {
            header,
            len,
            body: SessionBody::Accept { whatami, opid, apid, initial_sn, sn_resolution, lease, locators },
            properties: ps
         }
    }

    pub fn make_close(
        pid: Option<PeerId>, 
        reason: u8,
        link_only: bool,
        ps: Option<Arc<Vec<Property>>>
    ) -> SessionMessage {
        let pflag = if ps.is_some() { flag::P } else { 0 };
        let zflag = if link_only { flag::Z } else { 0 };
        let aflag = if pid.is_some() { flag::A } else { 0 };
        let header = sid::CLOSE | aflag | zflag | pflag;

        let len: u16 = 0;
        SessionMessage {
            header,
            len,
            body: SessionBody::Close { pid, reason, link_only },
            properties: ps    
        }
    }

    pub fn make_sync(
        ch: Channel, 
        sn: ZInt, 
        count: Option<ZInt>, 
        ps: Option<Arc<Vec<Property>>>
    ) -> SessionMessage {
        let pflag = if ps.is_some() { flag::P } else { 0 };
        let rflag = if ch { flag::R } else { 0 };
        let gflag = if count.is_some() { flag::G } else { 0 };
        let header = sid::SYNC | gflag | rflag | pflag;
        
        let len: u16 = 0;
        SessionMessage {
            header,
            len,
            body: SessionBody::Sync { ch, sn, count },
            properties: ps
        }
    }

    pub fn make_ack_nack(
        ch: Channel, 
        sn: ZInt, 
        mask: Option<ZInt>, 
        ps: Option<Arc<Vec<Property>>>
    ) -> SessionMessage {
        let pflag = if ps.is_some() { flag::P } else { 0 };
        let rflag = if ch { flag::R } else { 0 };
        let mflag = if mask.is_some() { flag::M } else { 0 };
        let header = sid::ACK_NACK | mflag | rflag | pflag;

        let len: u16 = 0;
        SessionMessage {
            header,
            len,
            body: SessionBody::AckNack { ch, sn, mask },
            properties: ps
        }
    }

    pub fn make_keep_alive(
        pid: Option<PeerId>, 
        ps: Option<Arc<Vec<Property>>>
    ) -> SessionMessage {
        let pflag = if ps.is_some() { flag::P } else { 0 };
        let aflag = if pid.is_some() { flag::A } else { 0 };
        let header = sid::KEEP_ALIVE | aflag | pflag;

        let len: u16 = 0;
        SessionMessage {
            header,
            len,
            body: SessionBody::KeepAlive { pid },
            properties: ps
        }
    }

    pub fn make_ping(
        hash: ZInt, 
        ps: Option<Arc<Vec<Property>>>
    ) -> SessionMessage {
        let pflag = if ps.is_some() { flag::P } else { 0 };
        let bflag = flag::B;
        let header = sid::PING_PONG | bflag | pflag;

        let len: u16 = 0;
        SessionMessage {
            header,
            len,
            body: SessionBody::Ping { hash },
            properties: ps
        }
    }

    pub fn make_pong(
        hash: ZInt, 
        ps: Option<Arc<Vec<Property>>>
    ) -> SessionMessage {
        let pflag = if ps.is_some() { flag::P } else { 0 };
        let bflag = 0;
        let header = sid::PING_PONG | bflag | pflag;

        let len: u16 = 0;
        SessionMessage {
            header,
            len,
            body: SessionBody::Pong { hash },
            properties: ps
        }
    }

    pub fn make_frame(
        ch: Channel,
        sn: ZInt,
        payload: FramePayload,
        ps: Option<Arc<Vec<Property>>>
    ) -> SessionMessage {
        let pflag = if ps.is_some() { flag::P } else { 0 };
        let rflag = if ch { flag::R } else { 0 };
        let (fflag, cflag) = match payload {
            FramePayload::Fragment { buffer, is_final} => {
                if is_final {
                    (flag::F, flag::C)
                } else {
                    (0, flag::C)
                }
            },
            FramePayload::Messages { messages } => {
                (0, 0)
            }
        };
        let header = sid::PING_PONG | rflag | fflag | cflag;

        let len: u16 = 0;
        SessionMessage {
            header,
            len,
            body: SessionBody::Frame { ch, sn, payload },
            properties: ps
        }
    }

    // -- Accessor
    pub fn get_body(&self) -> &SessionBody {
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