use crate::zerror;
use crate::core::{ZError, ZErrorKind, ZInt, ZResult, PeerId, Property, ResKey, TimeStamp};
use crate::io::RBuf;
use crate::link::Locator;
use super::decl::Declaration;
use std::sync::Arc;

const DECORATOR_ATTACHMENT_ID: u8 = 0x1f;

/*************************************/
/*         ZENOH MESSAGES            */
/*************************************/
pub mod zmsg {
    use super::DECORATOR_ATTACHMENT_ID;

    // Zenoh message IDs
    pub mod id {
        use super::DECORATOR_ATTACHMENT_ID;

        // Messages
        pub const DECLARE       : u8 = 0x01;
        pub const DATA  	    : u8 = 0x02;
        pub const QUERY         : u8 = 0x03;
        pub const PULL          : u8 = 0x04;
        pub const UNIT          : u8 = 0x05;

        // Message decorators
        pub const REPLY         : u8 = 0x1e;
        pub const ATTACHMENT    : u8 = DECORATOR_ATTACHMENT_ID; 
    }

    // Zenoh message flags
    pub mod flag {
        // TO UPDATE THE REPLY AND REMOVE
        pub const E: u8 = 1 << 5; // 0x20 FromEval     if E==1 then the Reply is from Eval

        pub const F: u8 = 1 << 5; // 0x20 Final        if F==1 then this is the final message (e.g., Reply, Pull)
        pub const I: u8 = 1 << 6; // 0x40 Info         if I==1 then Info is present
        pub const K: u8 = 1 << 7; // 0x80 ResourceKey  if K==1 then only numerical ID
        pub const N: u8 = 1 << 6; // 0x40 MaxSamples   if N==1 then the MaxSamples is indicated
        pub const R: u8 = 1 << 5; // 0x20 Reliable     if R==1 then it concerns the reliable channel, best-effort otherwise
        pub const S: u8 = 1 << 5; // 0x80 SubMode      if S==1 then the declaration SubMode is indicated
        pub const T: u8 = 1 << 5; // 0x20 QueryTarget  if T==1 then the query target is present

        pub const X: u8 = 0;      // Unused flags are set to zero
    }

    // Flags used for DataInfo
    pub mod info_flag {
        pub const SRCID : u8 = 1 << 0; // 0x01
        pub const SRCSN : u8 = 1 << 1; // 0x02
        pub const BKRID : u8 = 1 << 2; // 0x04
        pub const BKRSN : u8 = 1 << 3; // 0x08
        pub const TS    : u8 = 1 << 4; // 0x10
        pub const KIND  : u8 = 1 << 5; // 0x20
        pub const ENC   : u8 = 1 << 6; // 0x40
    }

    // Header mask
    pub const HEADER_MASK   : u8 = 0x1f;

    pub fn mid(header: u8) -> u8 { header & HEADER_MASK }
    pub fn flags(header: u8) -> u8 { header & !HEADER_MASK }
    pub fn has_flag(byte: u8, flag: u8) -> bool { byte & flag != 0 }
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
        if source_id.is_some() { header |= zmsg::info_flag::SRCID }
        if source_sn.is_some() { header |= zmsg::info_flag::SRCSN }
        if fist_broker_id.is_some() { header |= zmsg::info_flag::BKRID }
        if fist_broker_sn.is_some() { header |= zmsg::info_flag::BKRSN }
        if timestamp.is_some() { header |= zmsg::info_flag::TS }
        if kind.is_some() { header |= zmsg::info_flag::KIND }
        if encoding.is_some() { header |= zmsg::info_flag::ENC }
        
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
    ///
    /// The wire format of message decorators is described below. That said,
    /// they are represented as fields in the memory-representation of the message.

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|F|  REPLY  |
    /// +-+-+-+---------+
    /// ~      qid      ~
    /// +---------------+
    /// ~   replier_id  ~ if F==0
    /// +---------------+
    ///
    /// - if F==1 then the message is a REPLY_FINAL 
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
    /// |X|X|X| DECLARE |
    /// +-+-+-+---------+
    /// ~ [Declaration] ~
    /// +---------------+
    Declare { declarations: Vec<Declaration> },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |K|I|R|  DATA   |
    /// +-+-+-+---------+
    /// ~    ResKey     ~ if K==1 -- Only numerical id
    /// +---------------+
    /// ~     Info      ~ if I==1
    /// +---------------+
    /// ~    Payload    ~
    /// +---------------+
    ///
    /// - if R==1 then the message is sent on the reliable channel, best-effort otherwise.
    /// 
    Data { key: ResKey, info: Option<RBuf>, payload: RBuf },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|R|  UNIT   |
    /// +-+-+-+---------+
    ///
    /// - if R==1 then the message is sent on the reliable channel, best-effort otherwise.
    /// 
    Unit { },

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |K|N|F|  PULL   |
    /// +-+-+-+---------+
    /// ~    ResKey     ~ if K==1 then only numerical id
    /// +---------------+
    /// ~    pullid     ~
    /// +---------------+
    /// ~  max_samples  ~ if N==1
    /// +---------------+
    Pull { key: ResKey, pull_id: ZInt, max_samples: Option<ZInt>},

    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |K|X|T|  QUERY  |
    /// +-+-+-+---------+
    /// ~    ResKey     ~ if K==1 then only numerical id
    /// +---------------+
    /// ~   predicate   ~
    /// +---------------+
    /// ~      qid      ~
    /// +---------------+
    /// ~     target    ~ if T==1
    /// +---------------+
    /// ~ consolidation ~
    /// +---------------+
    Query { key: ResKey, predicate: String, qid: ZInt, target: Option<QueryTarget>, consolidation: QueryConsolidation },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ZenohMessage {
    pub(crate) header: u8,
    pub(crate) body: ZenohBody,
    pub(crate) ch: Channel,
    pub(crate) reply_context: Option<ReplyContext>,
    pub(crate) attachment: Option<Arc<RBuf>>
}

impl ZenohMessage {
    pub fn make_declare(
        declarations: Vec<Declaration>, 
        attachment: Option<Arc<RBuf>>
    ) -> ZenohMessage {
        let header = zmsg::id::DECLARE;

        ZenohMessage {
            header,
            ch: channel::RELIABLE,
            body: ZenohBody::Declare { declarations },
            reply_context: None,
            attachment
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn make_data(
        ch: Channel,
        key: ResKey,
        info: Option<RBuf>,
        payload: RBuf,
        reply_context: Option<ReplyContext>,
        attachment: Option<Arc<RBuf>> 
    ) -> ZenohMessage {   
        let kflag = if key.is_numerical() { zmsg::flag::K } else { 0 };
        let iflag = if info.is_some() { zmsg::flag::I } else { 0 };  
        let rflag = if ch { zmsg::flag::R } else { 0 };
        let header = zmsg::id::DATA | iflag | rflag | kflag;

        ZenohMessage {
            header,
            ch, 
            body: ZenohBody::Data { key, info, payload },
            reply_context,
            attachment
        }
    }

    pub fn make_unit(
        ch: Channel,
        reply_context: Option<ReplyContext>,
        attachment: Option<Arc<RBuf>> 
    ) -> ZenohMessage {
        let rflag = if ch { zmsg::flag::R } else { 0 };
        let header = zmsg::id::UNIT | rflag;

        ZenohMessage {
            header,
            ch,
            body: ZenohBody::Unit { },
            reply_context,
            attachment
        }
    }

    pub fn make_pull(
        is_final: bool, 
        key: ResKey, 
        pull_id: ZInt, 
        max_samples: Option<ZInt>, 
        attachment: Option<Arc<RBuf>>
    ) -> ZenohMessage {
        let kflag = if key.is_numerical() { zmsg::flag::K } else { 0 };
        let nflag = if max_samples.is_some() { zmsg::flag::N } else { 0 };
        let fflag = if is_final { zmsg::flag::F } else { 0 };
        let header = zmsg::id::PULL | fflag | nflag | kflag;

        ZenohMessage {
            header,
            ch: channel::RELIABLE,
            body: ZenohBody::Pull { key, pull_id, max_samples },
            reply_context: None,
            attachment
        }
    }

    pub fn make_query(
        key: ResKey, 
        predicate: String, 
        qid: ZInt,
        target: Option<QueryTarget>, 
        consolidation: QueryConsolidation, 
        attachment: Option<Arc<RBuf>>
    ) -> ZenohMessage {
        let kflag = if key.is_numerical() { zmsg::flag::K } else { 0 };
        let tflag = if target.is_some() { zmsg::flag::T } else { 0 };
        let header = zmsg::id::QUERY | tflag | kflag;

        ZenohMessage {
            header,
            ch: channel::RELIABLE,
            body: ZenohBody::Query { key, predicate, qid, target, consolidation },
            reply_context: None,
            attachment
        }
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
    pub fn get_attachment(&self) -> &Option<Arc<RBuf>> {
        &self.attachment
    }
}

/*************************************/
/*        SESSION MESSAGES           */
/*************************************/
pub mod smsg {
    // Session message IDs
    pub mod id {
        // Messages
        pub const SCOUT         : u8 = 0x01;
        pub const HELLO         : u8 = 0x02;
        pub const OPEN          : u8 = 0x03;
        pub const ACCEPT        : u8 = 0x04;
        pub const CLOSE         : u8 = 0x05;
        pub const SYNC          : u8 = 0x06;
        pub const ACK_NACK      : u8 = 0x07;
        pub const KEEP_ALIVE    : u8 = 0x08;
        pub const PING_PONG     : u8 = 0x09;
        pub const FRAME         : u8 = 0x0a; 

        // Message decorators
        pub const ATTACHMENT    : u8 = 0x1f;
    }

    // Session message flags
    pub mod flag {
        pub const C: u8 = 1 << 6; // 0x40 Count         if C==1 then number of unacknowledged messages is present
        pub const D: u8 = 1 << 5; // 0x80 LeasePeriod   if D==1 then the lease period is present
        pub const E: u8 = 1 << 7; // 0x80 End           if E==1 then the it is the last FRAME fragment
        pub const F: u8 = 1 << 6; // 0x40 Fragment      if F==1 then the FRAME is a fragment
        pub const I: u8 = 1 << 5; // 0x20 PeerID        if I==1 then the PeerID is present
        pub const K: u8 = 1 << 6; // 0x40 CloseLink     if K==1 then close the transport link only
        pub const L: u8 = 1 << 7; // 0x20 Locators      if L==1 then Locators are present
        pub const M: u8 = 1 << 5; // 0x20 Mask          if M==1 then a Mask is present
        pub const O: u8 = 1 << 5; // 0x20 Options       if O==1 then options are present
        pub const P: u8 = 1 << 5; // 0x20 PingOrPong    if P==1 then the message is Ping, otherwise is Pong
        pub const R: u8 = 1 << 5; // 0x20 Reliable      if R==1 then it concerns the reliable channel, best-effort otherwise
        pub const S: u8 = 1 << 6; // 0x40 SN Resolution if S==1 then the SN Resolution is present
        pub const W: u8 = 1 << 5; // 0x20 WhatAmI       if W==1 then WhatAmI is indicated

        pub const X: u8 = 0;      // Unused flags are set to zero
    }

    // Reason for the Close message 
    pub mod close_reason {
        pub const GENERIC       : u8 = 0x00;
        pub const UNSUPPORTED   : u8 = 0x01;
        pub const MAX_SESSIONS  : u8 = 0x02;
        pub const MAX_LINKS     : u8 = 0x03;
    }

    // Header mask
    pub const HEADER_MASK   : u8 = 0x1f;

    pub fn mid(header: u8) -> u8 { header & HEADER_MASK }
    pub fn flags(header: u8) -> u8 { header & !HEADER_MASK }
    pub fn has_flag(byte: u8, flag: u8) -> bool { byte & flag != 0 }
}

// Channel values
pub type Channel = bool;
pub mod channel {
    use super::Channel;

    pub const BEST_EFFORT   : Channel = false;
    pub const RELIABLE      : Channel = true;
}

// WhatAmI values
pub type WhatAmI = ZInt;
pub mod whatami {
    use super::WhatAmI;
    pub const BROKER    : WhatAmI = 1 << 0; // 0x01
    pub const ROUTER    : WhatAmI = 1 << 1; // 0x02
    pub const PEER      : WhatAmI = 1 << 2; // 0x04
    pub const CLIENT    : WhatAmI = 1 << 3; // 0x08
    // b4-b13: Reserved
}

#[derive(Debug, Clone)]
pub enum SessionMode {
    Push,
    Pull,
    PeriodicPush(u32),
    PeriodicPull(u32),
    PushPull
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
    /// -- Message Decorators
    ///    Message decorators are used to represent on the wire certain message properties.
    ///    The wire format of message decorators is described below. That said,
    ///    they are represented as fields in the memory-representation of the message.

    /// NOTE: 16 bits (2 bytes) may be prepended to the serialized message indicating the total lenght 
    ///       in bytes of the message, resulting in the maximum lenght of a message being 65_536 bytes.
    ///       This is necessary in those stream-oriented transports (e.g., TCP) that do not preserve 
    ///       the boundary of the serialized messages. The length is encoded as little-endian.
    ///       In any case, the lenght of a message must not exceed 65_536 bytes.    
    /// 
    /// The Attachment can decorate any message (i.e., SessionMessage and ZenohMessage) and it allows to 
    /// append to the message any additional information. Since the information contained in the 
    /// Attchement is relevant only to the layer that provided them (e.g., Session, Zenoh, User) it 
    /// is the duty of that layer to serialize and de-serialize the attachment whenever deemed necessary.
    /// 
    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// | ENC |  ATTCH  |
    /// +-+-+-+---------+
    /// ~    Message    ~
    /// +---------------+
    /// ~   Attachment  ~
    /// +---------------+
    ///
    /// ENC values:
    /// - 0x00 => Zenoh Properties
    /// 
    Attachment { payload: Arc<RBuf> },

    /// -- Messages at session level

    /// NOTE: 16 bits (2 bytes) may be prepended to the serialized message indicating the total lenght 
    ///       in bytes of the message, resulting in the maximum lenght of a message being 65_536 bytes.
    ///       This is necessary in those stream-oriented transports (e.g., TCP) that do not preserve 
    ///       the boundary of the serialized messages. The length is encoded as little-endian.
    ///       In any case, the lenght of a message must not exceed 65_536 bytes.
    /// 
    /// The SCOUT message can be sent at any point in time to solicit HELLO messages from matching parties.
    ///
    ///  7 6 5 4 3 2 1 0
	/// +-+-+-+-+-+-+-+-+
	/// |X|X|W|  SCOUT  |
	/// +-+-+-+-+-------+
	/// ~    whatmai    ~ if W==1 -- Otherwise implicitly scouting for Brokers
	/// +---------------+  
    /// 
    Scout { what: Option<WhatAmI> },

    /// NOTE: 16 bits (2 bytes) may be prepended to the serialized message indicating the total lenght 
    ///       in bytes of the message, resulting in the maximum lenght of a message being 65_536 bytes.
    ///       This is necessary in those stream-oriented transports (e.g., TCP) that do not preserve 
    ///       the boundary of the serialized messages. The length is encoded as little-endian.
    ///       In any case, the lenght of a message must not exceed 65_536 bytes.
    /// 
    /// The HELLO message is sent in any of the following three cases:
    ///     1) in response to a SCOUT message;
    ///     2) to (periodically) advertise (e.g., on multicast) the Peer and the locators it is reachable at;
    ///     3) in a already established session to update the corresponding peer on the new capabilities 
    ///        (i.e., whatmai) and/or new set of locators (i.e., added or deleted).
    /// Locators are expressed as:
  	/// <code>
	///  udp/192.168.0.2:1234
	///  tcp/192.168.0.2:1234
	///  udp/239.255.255.123:5555
	/// <code>
    /// 
	///  7 6 5 4 3 2 1 0
	/// +-+-+-+-+-+-+-+-+
	/// |L|X|W|  HELLO  |
	/// +-+-+-+-+-------+
    /// ~    whatmai    ~ if W==1 -- Otherwise it is from a Broker
	/// +---------------+
    /// ~    Locators   ~ if L==1 -- Otherwise src-address is the locator
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
    /// The OPEN message is sent on a specific Locator to initiate a session with the peer associated 
    /// with that Locator.
    /// 
    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|O|   OPEN  |
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
    /// |L|S|X|X|X|X|X|X| if O==1
    /// +-+-+-+-+-+-+-+-+
    /// ~ sn_resolution ~ if S==1 -- Otherwise 2^28 is assumed(**)
    /// +---------------+
    /// ~    Locators   ~ if L==1 -- List of locators the sender of the OPEN is reachable at
    /// +---------------+
    /// 
    /// (*)  The Initial SN must be bound to the proposed SN Resolution. Otherwise the OPEN message is consmsg::idered 
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
    /// The ACCEPT message is sent in response of an OPEN message in case of accepting the new incoming session.
    /// 
    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|O| ACCEPT  |
    /// +-+-+-+-+-------+
    /// ~    whatami    ~ -- Client, Broker, Router, Peer or a combination of them
    /// +---------------+
    /// ~   o_peer_id   ~ -- PID of the sender of the OPEN this ACCEPT is for
    /// +---------------+  
    /// ~   a_peer_id   ~ -- PID of the sender of the ACCEPT
    /// +---------------+
    /// ~  initial_sn   ~ -- Initial SN proposed by the sender of the ACCEPT(*)
    /// +-+-+-+-+-+-+-+-+
    /// |L|S|D|X|X|X|X|X| if O==1
    /// +-+-+-+-+-+-+---+
    /// ~ sn_resolution + if S==1 -- Agreed SN Resolution(**)
    /// +---------------+
    /// ~ lease_period  ~ if D==1
    /// +---------------+
    /// ~    Locators   ~ if L==1
    /// +---------------+
    /// 
    /// - if S==0 then the agreed sequence number resolution is the one indicated in the OPEN message.
    /// - if S==1 then the agreed sequence number resolution is the one indicated in this ACCEPT message. 
    ///           The resolution in the ACCEPT must be less or equal than the resolution in the OPEN, 
    ///           otherwise the ACCEPT message is consmsg::idered invalid and it should be treated as a
    ///           CLOSE message with L==0 by the Opener Peer -- the recipient of the ACCEPT message.
    /// 
    /// - if D==0 then the agreed lease period is the one indicated in the OPEN message.
    /// - if D==1 then the agreed lease period is the one indicated in this ACCEPT message. 
    ///           The lease period in the ACCEPT must be less or equal than the lease period in the OPEN,
    ///           otherwise the ACCEPT message is consmsg::idered invalid and it should be treated as a
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
    /// The CLOSE message is sent in any of the following two cases:
    ///     1) in response to an OPEN message which is not accepted;
    ///     2) at any time to arbitrarly close the session with the corresponding peer.
    /// 
    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|K|I|  CLOSE  |
    /// +-+-+-+-+-------+
    /// ~    peer_id    ~  if I==1 -- PID of the target peer.
    /// +---------------+
    /// |     reason    |
    /// +---------------+
    /// 
    /// - if K==0 then close the whole zenoh session.
    /// - if K==1 then close the transport link the CLOSE message was sent on (e.g., TCP socket) but 
    ///           keep the whole session open. NOTE: the session will be automatically closed when
    ///           the session's lease period expires.
    /// 
    Close { pid: Option<PeerId>, reason: u8, link_only: bool },

    /// NOTE: 16 bits (2 bytes) may be prepended to the serialized message indicating the total lenght 
    ///       in bytes of the message, resulting in the maximum lenght of a message being 65_536 bytes.
    ///       This is necessary in those stream-oriented transports (e.g., TCP) that do not preserve 
    ///       the boundary of the serialized messages. The length is encoded as little-endian.
    ///       In any case, the lenght of a message must not exceed 65_536 bytes.
    /// 
    /// The SYNC message allows to signal the corresponding peer the sequence number of the next message
    /// to be transmitted on the reliable or best-effort channel. In the case of reliable channel, the  
    /// peer can optionally include the number of unacknowledged messages. A SYNC sent on the reliable
    /// channel triggers the transmission of an ACKNACK message.
    /// 
    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|C|R|  SYNC   |
    /// +-+-+-+-+-------+
    /// ~      sn       ~ -- Sequence number of the next message to be transmitted on this channel.
    /// +---------------+
    /// ~     count     ~ if R==1 && C==1 -- Number of unacknowledged messages. 
    /// +---------------+
    ///
    /// - if R==1 then the SYNC concerns the reliable channel, otherwise the best-effort channel.
    /// 
    Sync { ch: Channel, sn: ZInt, count: Option<ZInt> },

    /// NOTE: 16 bits (2 bytes) may be prepended to the serialized message indicating the total lenght 
    ///       in bytes of the message, resulting in the maximum lenght of a message being 65_536 bytes.
    ///       This is necessary in those stream-oriented transports (e.g., TCP) that do not preserve 
    ///       the boundary of the serialized messages. The length is encoded as little-endian.
    ///       In any case, the lenght of a message must not exceed 65_536 bytes.
    /// 
    /// The ACKNACK messages is used on the reliable channel to signal the corresponding peer the last
    /// sequence number received and optionally a bitmask of the non-received messages.
    /// 
    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|M| ACKNACK |
    /// +-+-+-+-+-------+
    /// ~      sn       ~ 
    /// +---------------+
    /// ~     mask      ~ if M==1
    /// +---------------+
    /// 
    AckNack { sn: ZInt, mask: Option<ZInt> },

    /// NOTE: 16 bits (2 bytes) may be prepended to the serialized message indicating the total lenght 
    ///       in bytes of the message, resulting in the maximum lenght of a message being 65_536 bytes.
    ///       This is necessary in those stream-oriented transports (e.g., TCP) that do not preserve 
    ///       the boundary of the serialized messages. The length is encoded as little-endian.
    ///       In any case, the lenght of a message must not exceed 65_536 bytes. 
    /// 
    /// The KEEP_ALIVE message can be sent periodically to avoid the expiration of the session lease 
    /// period in case there are no messages to be sent.
    /// 
    ///  7 6 5 4 3 2 1 0
    /// +-+-+-+-+-+-+-+-+
    /// |X|X|I| K_ALIVE |
    /// +-+-+-+-+-------+
    /// ~    peer_id    ~ if I==1 -- Peer ID of the KEEP_ALIVE sender.
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
    /// |X|X|P|  P_PONG |
    /// +-+-+-+-+-------+
    /// ~     hash      ~
    /// +---------------+
    /// 
    /// - if P==1 then the message is Ping, otherwise is Pong.
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
    /// |E|F|R|  FRAME  |
    /// +-+-+-+-+-------+
    /// ~      SN       ~
    /// +---------------+
    /// ~  FramePayload ~ -- Either a list of complete Zenoh Messages or a fragment of a single Zenoh Message
    /// +---------------+
    /// 
    /// - if R==1 then the FRAME is sent on the reliable channel, best-effort otherwise.
    /// - if F==1 then the FRAME is a fragment.
    /// - if E==1 then the FRAME is the last fragment. F==1 is valid iff C==1.
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
    pub(crate) body: SessionBody,
    pub(crate) attachment: Option<Arc<RBuf>>
}

impl SessionMessage {
    pub fn make_scout(
        what: Option<WhatAmI>,
        attachment: Option<Arc<RBuf>>
    ) -> SessionMessage {
        let wflag = if what.is_some() { smsg::flag::W } else { 0 };
        let header = smsg::id::SCOUT | wflag;

        SessionMessage {
            header,
            body: SessionBody::Scout { what },
            attachment
        }
    }

    pub fn make_hello(
        whatami: Option<WhatAmI>,
        locators: Option<Vec<Locator>>,
        attachment: Option<Arc<RBuf>>
    ) -> SessionMessage {
        let wflag = if whatami.is_some() { smsg::flag::W } else { 0 };
        let lflag = if locators.is_some() { smsg::flag::L } else { 0 };
        let header = smsg::id::HELLO | wflag;

        SessionMessage {
            header,
            body: SessionBody::Hello { whatami, locators },
            attachment
        }
    }

    pub fn make_open(
        version: u8, 
        whatami: WhatAmI, 
        pid: PeerId, 
        lease: ZInt, 
        initial_sn: ZInt,
        sn_resolution: Option<ZInt>,
        locators: Option<Vec<Locator>>, 
        attachment: Option<Arc<RBuf>>
    ) -> SessionMessage {
        let oflag = if sn_resolution.is_some() || locators.is_some() { smsg::flag::O } else { 0 };
        let header = smsg::id::OPEN | oflag;

        SessionMessage {
            header,
            body: SessionBody::Open { version, whatami, pid, lease, initial_sn, sn_resolution, locators },
            attachment         
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
        attachment: Option<Arc<RBuf>>
    ) -> SessionMessage {
        let oflag = if sn_resolution.is_some() || lease.is_some() 
                    || locators.is_some() { smsg::flag::O } else { 0 };
        let header = smsg::id::ACCEPT | oflag;

        SessionMessage {
            header,
            body: SessionBody::Accept { whatami, opid, apid, initial_sn, sn_resolution, lease, locators },
            attachment
         }
    }

    pub fn make_close(
        pid: Option<PeerId>, 
        reason: u8,
        link_only: bool,
        attachment: Option<Arc<RBuf>>
    ) -> SessionMessage {
        let kflag = if link_only { smsg::flag::K } else { 0 };
        let iflag = if pid.is_some() { smsg::flag::I } else { 0 };
        let header = smsg::id::CLOSE | kflag | iflag;

        SessionMessage {
            header,
            body: SessionBody::Close { pid, reason, link_only },
            attachment
        }
    }

    pub fn make_sync(
        ch: Channel, 
        sn: ZInt, 
        count: Option<ZInt>, 
        attachment: Option<Arc<RBuf>>
    ) -> SessionMessage {
        let cflag = if count.is_some() { smsg::flag::C } else { 0 };
        let rflag = if ch { smsg::flag::R } else { 0 };
        let header = smsg::id::SYNC | rflag | cflag;
        
        SessionMessage {
            header,
            body: SessionBody::Sync { ch, sn, count },
            attachment
        }
    }

    pub fn make_ack_nack(
        sn: ZInt, 
        mask: Option<ZInt>, 
        attachment: Option<Arc<RBuf>>
    ) -> SessionMessage {
        let mflag = if mask.is_some() { smsg::flag::M } else { 0 };
        let header = smsg::id::ACK_NACK | mflag;

        SessionMessage {
            header,
            body: SessionBody::AckNack { sn, mask },
            attachment
        }
    }

    pub fn make_keep_alive(
        pid: Option<PeerId>, 
        attachment: Option<Arc<RBuf>>
    ) -> SessionMessage {
        let iflag = if pid.is_some() { smsg::flag::I } else { 0 };
        let header = smsg::id::KEEP_ALIVE | iflag;

        SessionMessage {
            header,
            body: SessionBody::KeepAlive { pid },
            attachment
        }
    }

    pub fn make_ping(
        hash: ZInt, 
        attachment: Option<Arc<RBuf>>
    ) -> SessionMessage {
        let pflag = smsg::flag::P;
        let header = smsg::id::PING_PONG | pflag;

        SessionMessage {
            header,
            body: SessionBody::Ping { hash },
            attachment
        }
    }

    pub fn make_pong(
        hash: ZInt, 
        attachment: Option<Arc<RBuf>>
    ) -> SessionMessage {
        let pflag = 0;
        let header = smsg::id::PING_PONG | pflag;

        SessionMessage {
            header,
            body: SessionBody::Pong { hash },
            attachment
        }
    }

    pub fn make_frame(
        ch: Channel,
        sn: ZInt,
        payload: FramePayload,
        attachment: Option<Arc<RBuf>>
    ) -> SessionMessage {
        let rflag = if ch { smsg::flag::R } else { 0 };
        let (eflag, fflag) = match payload {
            FramePayload::Fragment { buffer, is_final} => {
                if is_final {
                    (smsg::flag::E, smsg::flag::F)
                } else {
                    (0, smsg::flag::F)
                }
            },
            FramePayload::Messages { messages } => {
                (0, 0)
            }
        };
        let header = smsg::id::PING_PONG | rflag | fflag | eflag;

        SessionMessage {
            header,
            body: SessionBody::Frame { ch, sn, payload },
            attachment
        }
    }

    // -- Accessor
    pub fn get_body(&self) -> &SessionBody {
        &self.body
    }

    pub fn get_attachment(&self) -> &Option<Arc<RBuf>> {
        &self.attachment
    }
}

#[derive(Debug, Clone)]
pub struct MalformedMessage { pub msg: String }

#[derive(Debug, Clone)]
pub struct InvalidMessage { pub msg: String }