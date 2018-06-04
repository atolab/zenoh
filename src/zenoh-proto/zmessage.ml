open Apero
open Ztypes
open Zlocator
open Zproperty
open Ziobuf

module PropertyId = struct
  let maxConduits = 2L
  let snLen = 4L
  let reliability = 6L
  let authData = 12L
end

module MessageId = struct
  let scoutId = char_of_int 0x01
  let helloId = char_of_int 0x02

  let openId = char_of_int 0x03
  let acceptId = char_of_int 0x04
  let closeId = char_of_int 0x05

  let declareId = char_of_int 0x06

  let sdataId = char_of_int 0x07
  let bdataId = char_of_int 0x08
  let wdataId = char_of_int 0x09

  let queryId = char_of_int 0x0a
  let pullId = char_of_int 0x0b

  let pingPongId = char_of_int 0x0c

  let synchId = char_of_int 0x0e
  let ackNackId = char_of_int 0x0f

  let keepAliveId = char_of_int 0x10

  let conduitCloseId = char_of_int 0x11
  let fragmetsId = char_of_int 0x12
  let rSpaceId = char_of_int 0x18

  let conduitId = char_of_int 0x13
  let migrateId = char_of_int 0x14

  let sdeltaDataId = char_of_int 0x15
  let bdeltaDataId = char_of_int 0x16
  let wdeltaDataId = char_of_int 0x17
end

module Flags = struct
  let sFlag = char_of_int 0x20
  let mFlag = char_of_int 0x20
  let pFlag = char_of_int 0x20

  let rFlag = char_of_int 0x40
  let nFlag = char_of_int 0x40
  let cFlag = char_of_int 0x40

  let aFlag = char_of_int 0x80
  let uFlag = char_of_int 0x80

  let zFlag = char_of_int 0x80
  let lFlag = char_of_int 0x20
  let hFlag = char_of_int 0x40

  let gFlag = char_of_int 0x80
  let iFlag = char_of_int 0x20
  let fFlag = char_of_int 0x80
  let oFlag = char_of_int 0x20

  let midMask = char_of_int 0x1f
  let hFlagMask = char_of_int 0xe0

  let hasFlag h f =  (int_of_char h) land (int_of_char f) <> 0
  let mid h =  char_of_int @@ (int_of_char h) land  (int_of_char midMask)
  let flags h = char_of_int @@ (int_of_char h) land  (int_of_char hFlagMask)
  let mid_len = 5

end

module ScoutFlags = struct
  let scoutBroker = char_of_int 0x01
  let scoutDurability = char_of_int 0x02
  let scoutPeer = char_of_int 0x04
  let scoutClient = char_of_int 0x08
end

module DeclarationId = struct
  let resourceDeclId = char_of_int 0x01
  let publisherDeclId = char_of_int 0x02
  let subscriberDeclId = char_of_int 0x03
  let selectionDeclId = char_of_int 0x04
  let bindingDeclId = char_of_int 0x05
  let commitDeclId = char_of_int 0x06
  let resultDeclId = char_of_int 0x07
  let forgetResourceDeclId = char_of_int 0x08
  let forgetPublisherDeclId = char_of_int 0x09
  let forgetSubscriberDeclId = char_of_int 0x0a
  let forgetSelectionDeclId = char_of_int 0x0b
end

module SubscriptionModeId = struct
  let pushModeId = char_of_int 0x01
  let pullModeId = char_of_int 0x02
  let periodicPushModeId = char_of_int 0x03
  let periodicPullModeId = char_of_int 0x04
end

module TemporalProperties = struct
  type t = {
    origin : Vle.t;
    period : Vle.t;
    duration : Vle.t;
  }

  let create origin period duration = {origin; period; duration}
  let origin p = p.origin
  let period p = p.period
  let duration p = p.duration

end

module SubscriptionMode = struct
  type t =
    | PushMode
    | PullMode
    | PeriodicPushMode of TemporalProperties.t
    | PeriodicPullMode of TemporalProperties.t

  let push_mode = PushMode
  let pull_mode = PullMode
  let periodic_push tp = PeriodicPushMode tp
  let periodic_pull tp = PeriodicPullMode tp

  let id = function
    | PushMode -> SubscriptionModeId.pushModeId
    | PullMode -> SubscriptionModeId.pullModeId
    | PeriodicPushMode _ -> SubscriptionModeId.periodicPushModeId
    | PeriodicPullMode _ -> SubscriptionModeId.periodicPullModeId

  let has_temporal_properties id = id = SubscriptionModeId.periodicPullModeId || id = SubscriptionModeId.periodicPushModeId

  let temporal_properties = function
    | PushMode -> None
    | PullMode -> None
    | PeriodicPullMode tp -> Some tp
    | PeriodicPushMode tp -> Some tp

end


module Header = struct
  type t = char
  let mid h =  (int_of_char h) land (int_of_char Flags.midMask)
  let flags h = (int_of_char h) land (int_of_char Flags.hFlagMask)
end

module type Headed =
sig
  type t
  val header : t -> char
end

module type Reliable =
sig
  type t
  val reliable : t -> bool
  val synch : t -> bool
  val sn : t -> Vle.t
end

module ResourceDecl = struct
  type t = {
    header : Header.t;
    rid : Vle.t;
    resource : string;
    properties : Properties.t;
  }

  let create rid resource properties =
    let header = match properties with
      | [] -> DeclarationId.resourceDeclId
      | _ -> char_of_int ((int_of_char DeclarationId.resourceDeclId) lor (int_of_char Flags.pFlag))
    in {header=header; rid=rid; resource=resource; properties=properties}
  let header resourceDecl = resourceDecl.header
  let rid resourceDecl = resourceDecl.rid
  let resource resourceDecl = resourceDecl.resource
  let properties resourceDecl = resourceDecl.properties
end

module PublisherDecl = struct
  type t = {
    header : Header.t;
    rid : Vle.t;
    properties : Properties.t;
  }

  let create rid properties =
    let header = match properties with
      | [] -> DeclarationId.publisherDeclId
      | _ -> char_of_int ((int_of_char DeclarationId.publisherDeclId) lor (int_of_char Flags.pFlag))
    in {header=header; rid=rid; properties=properties}
  let header resourceDecl = resourceDecl.header
  let rid resourceDecl = resourceDecl.rid
  let properties resourceDecl = resourceDecl.properties
end

module SubscriberDecl = struct
  type t = {
    header : Header.t;
    rid : Vle.t;
    mode : SubscriptionMode.t;
    properties : Properties.t;
  }

  let create rid mode properties =
    let header = match properties with
      | [] -> DeclarationId.subscriberDeclId
      | _ -> char_of_int ((int_of_char DeclarationId.subscriberDeclId) lor (int_of_char Flags.pFlag))
    in {header=header; rid=rid; mode=mode; properties=properties}
  let header subscriberDecl = subscriberDecl.header
  let rid subscriberDecl = subscriberDecl.rid
  let mode subscriberDecl = subscriberDecl.mode
  let properties subscriberDecl = subscriberDecl.properties
end

module SelectionDecl = struct
  type t = {
    header : Header.t;
    sid : Vle.t;
    query : string;
    properties : Properties.t;
  }

  let create sid query properties global =
    let header = match properties with
      | [] -> (match global with
        | true -> char_of_int ((int_of_char DeclarationId.selectionDeclId) lor (int_of_char Flags.gFlag))
        | false -> DeclarationId.selectionDeclId)
      | _ -> (match global with
        | true -> char_of_int ((int_of_char DeclarationId.selectionDeclId) lor (int_of_char Flags.pFlag) lor (int_of_char Flags.gFlag))
        | false -> char_of_int ((int_of_char DeclarationId.selectionDeclId) lor (int_of_char Flags.pFlag)))
    in
    {header=header; sid=sid; query=query; properties=properties}
  let header selectionDecl = selectionDecl.header
  let sid selectionDecl = selectionDecl.sid
  let query selectionDecl = selectionDecl.query
  let properties selectionDecl = selectionDecl.properties
  let global selectionDecl = ((int_of_char selectionDecl.header) land (int_of_char Flags.gFlag)) <> 0
end

module BindingDecl = struct
  type t = {
    header : Header.t;
    old_id : Vle.t;
    new_id : Vle.t;
  }

  let create old_id new_id global =
    let header = match global with
      | false -> DeclarationId.bindingDeclId
      | true -> char_of_int ((int_of_char DeclarationId.bindingDeclId) lor (int_of_char Flags.gFlag))
    in {header; old_id; new_id}
  let header bd = bd.header
  let old_id bd = bd.old_id
  let new_id bd = bd.new_id
  let global selectionDecl = ((int_of_char selectionDecl.header) land (int_of_char Flags.gFlag)) <> 0
end

module CommitDecl = struct
  type t = {
    header : Header.t;
    commit_id : char;
  }

  let create id = { header = DeclarationId.commitDeclId; commit_id = id }
  let header cd = cd.header
  let commit_id cd = cd.commit_id
end

module ResultDecl = struct
  type t = {
    header : Header.t;
    commit_id : char;
    status : char;
    id : Vle.t option;
  }

  let create commit_id status id = {
    header=DeclarationId.resultDeclId;
    commit_id;
    status;
    id = if status = char_of_int 0 then None else id}

  let header d = d.header
  let commit_id d = d.commit_id
  let status d = d.status
  let id d = d.id
end

module ForgetResourceDecl = struct
  type t = {
    header : Header.t;
    rid : Vle.t;
  }

  let create rid = {header=DeclarationId.forgetResourceDeclId; rid=rid}
  let header decl = decl.header
  let rid decl = decl.rid
end

module ForgetPublisherDecl = struct
  type t = {
    header : Header.t;
    id : Vle.t;
  }

  let create id = {header=DeclarationId.forgetPublisherDeclId; id=id}
  let header decl = decl.header
  let id decl = decl.id
end

module ForgetSubscriberDecl = struct
  type t = {
    header : Header.t;
    id : Vle.t;
  }

  let create id = {header=DeclarationId.forgetSubscriberDeclId; id=id}
  let header decl = decl.header
  let id decl = decl.id
end

module ForgetSelectionDecl = struct
  type t = {
    header : Header.t;
    sid : Vle.t;
  }

  let create sid = {header=DeclarationId.forgetSelectionDeclId; sid=sid}
  let header decl = decl.header
  let sid decl = decl.sid
end

module Declaration = struct
  type t =
    | ResourceDecl of ResourceDecl.t
    | PublisherDecl of PublisherDecl.t
    | SubscriberDecl of SubscriberDecl.t
    | SelectionDecl of SelectionDecl.t
    | BindingDecl of BindingDecl.t
    | CommitDecl of CommitDecl.t
    | ResultDecl of ResultDecl.t
    | ForgetResourceDecl of ForgetResourceDecl.t
    | ForgetPublisherDecl of ForgetPublisherDecl.t
    | ForgetSubscriberDecl of ForgetSubscriberDecl.t
    | ForgetSelectionDecl of ForgetSelectionDecl.t
end

module Declarations = struct
  type t = Declaration.t list

  let length = List.length
  let empty = []
  let singleton d = [d]
  let add ds d = d::ds
end

(* @AC: A conduit marker should always have a cid, its representation changes, but the id is always there. 
       This should be reflected in the type declaration *)
module ConduitMarker = struct
  type t = {
    header : Header.t;
    id : Vle.t option
  }

  let create id = 
    match Vle.to_int id with 
      | 0 -> {header=MessageId.conduitId; id=None}
      | 1 -> {header=char_of_int ((int_of_char MessageId.conduitId) lor (int_of_char Flags.lFlag)); id=None}
      | 2 -> {header=char_of_int ((int_of_char MessageId.conduitId) lor (int_of_char Flags.hFlag)); id=None}
      | 3 -> {header=char_of_int ((int_of_char MessageId.conduitId) lor (int_of_char Flags.hFlag) lor (int_of_char Flags.lFlag)); id=None}
      | _ -> {header=char_of_int ((int_of_char MessageId.conduitId) lor (int_of_char Flags.zFlag)); id=Some id}

  let header m = m.header

  let id m = 
    (* @AC: Olivier, this is way to complicated, 
       we shoud just mask and get the id from the header *)
    match (int_of_char m.header) land (int_of_char Flags.zFlag) with 
    | 0 -> (match (int_of_char m.header) land (int_of_char Flags.hFlag) with 
            | 0 -> (match (int_of_char m.header) land (int_of_char Flags.lFlag) with 
                    | 0 -> Vle.zero
                    | _ -> Vle.one)
            | _ -> (match (int_of_char m.header) land (int_of_char Flags.lFlag) with 
                    | 0 -> Vle.of_int 2
                    | _ -> Vle.of_int 3))
    | _ -> match m.id with 
           | Some id -> id
           | None -> Vle.zero (* Should never happen *)
end

module Frag = struct
  type t = {
    header : Header.t;
    sn_base : Vle.t;
    n : Vle.t option;
  }
  let create sn_base n = {header=MessageId.fragmetsId; sn_base=sn_base; n=n}
    
  let header m = m.header

  let sn_base m = m.sn_base
  let n m = m.n
end

module RSpace = struct
  type t = {
    header : Header.t;
    id : Vle.t option
  }

  let create id = 
    match Vle.to_int id with 
      | 0 -> {header=MessageId.conduitId; id=None}
      | 1 -> {header=char_of_int ((int_of_char MessageId.rSpaceId) lor (int_of_char Flags.lFlag)); id=None}
      | 2 -> {header=char_of_int ((int_of_char MessageId.rSpaceId) lor (int_of_char Flags.hFlag)); id=None}
      | 3 -> {header=char_of_int ((int_of_char MessageId.rSpaceId) lor (int_of_char Flags.hFlag) lor (int_of_char Flags.lFlag)); id=None}
      | _ -> {header=char_of_int ((int_of_char MessageId.rSpaceId) lor (int_of_char Flags.zFlag)); id=Some id}

  let header m = m.header

  let id m = 
    match (int_of_char m.header) land (int_of_char Flags.zFlag) with 
    | 0 -> (match (int_of_char m.header) land (int_of_char Flags.hFlag) with 
            | 0 -> (match (int_of_char m.header) land (int_of_char Flags.lFlag) with 
                    | 0 -> Vle.zero
                    | _ -> Vle.one)
            | _ -> (match (int_of_char m.header) land (int_of_char Flags.lFlag) with 
                    | 0 -> Vle.of_int 2
                    | _ -> Vle.of_int 3))
    | _ -> match m.id with 
           | Some id -> id
           | None -> Vle.zero (* Should never happen *)
end


module Marker =
struct
  type t =
    | ConduitMarker of ConduitMarker.t
    | Frag of Frag.t
    | RSpace of RSpace.t
end

module type Marked =
sig
  type t
  val markers : t -> Marker.t list
  val with_marker : t -> Marker.t -> t
  val with_markers : t -> Marker.t list -> t
  val remove_markers : t -> t
end

module Markers = 
struct
  type t = Marker.t list

  let empty = []
  let with_marker t m  = m :: t
  let with_markers t ms = ms @ t
end


(**
   The SCOUT message can be sent at any point in time to solicit HELLO messages from
   matching parties

   7 6 5 4 3 2 1 0
   +-+-+-+-+-+-+-+-+
   |X|X|P|   SCOUT |
   +-+-+-+-+-+-+-+-+
   ~      Mask     ~
   +-+-+-+-+-+-+-+-+
   ~   Properties  ~
   +-+-+-+-+-+-+-+-+

   b0:     Broker
   b1:     Durability
   b2:     Peer
   b3:     Client
   b4-b13: Reserved
 **)
module Scout = struct
  type t = {
    header : Header.t;
    mask : Vle.t;
    properties : Properties.t;
    markers : Markers.t;
  }

  let create mask properties =
    let header = match properties with
      | [] -> MessageId.scoutId
      | _ -> char_of_int ((int_of_char MessageId.scoutId) lor (int_of_char Flags.pFlag))
    in {header=header; mask=mask; properties=properties; markers=Markers.empty}
  
  let header msg = msg.header
  
  let markers msg = msg.markers
  let with_marker msg marker = {msg with markers=Markers.with_marker msg.markers marker}
  let with_markers msg markers = {msg with markers=Markers.with_markers msg.markers markers}
  let remove_markers msg = {msg with markers=Markers.empty}

  let mask scout = scout.mask
  let properties scout = scout.properties


end

(**
   The SCOUT message can be sent at any point in time to solicit HELLO messages from
   matching parties

     7 6 5 4 3 2 1 0
     +-+-+-+-+-+-+-+-+
     |X|X|P|  HELLO  |
     +-+-+-+-+-+-+-+-+
     ~      Mask     ~
     +-+-+-+-+-+-+-+-+
     ~    Locators   ~
     +-+-+-+-+-+-+-+-+
     ~   Properties  ~
     +-+-+-+-+-+-+-+-+

     The hello message advertise a node and its locators. Locators are expressed as:

     udp/192.168.0.2:1234
     tcp/192.168.0.2:1234
     udp/239.255.255.123:5555
 **)
module Hello = struct
  type t = {
    header : Header.t;
    markers : Markers.t;
    mask : Vle.t;
    locators : Locators.t;
    properties : Properties.t;
  }

  let create mask locators properties =
    let header =
      let pflag = match properties with | [] -> 0 | _ -> int_of_char Flags.pFlag in
      let mid = int_of_char MessageId.helloId in
      char_of_int @@ pflag lor mid in
    {header=header; mask=mask; locators=locators; properties=properties; markers=Markers.empty}
  
  let header hello = hello.header
  
  let markers msg = msg.markers
  let with_marker msg marker = {msg with markers=Markers.with_marker msg.markers marker}
  let with_markers msg markers = {msg with markers=Markers.with_markers msg.markers markers}
  let remove_markers msg = {msg with markers=Markers.empty}

  let mask hello = hello.mask
  let locators hello = hello.locators
  let properties hello = hello.properties
  let to_string h =
    Printf.sprintf "Hello:[header: %d, mask: %Ld, locators: %s]" (int_of_char @@ h.header) (h.mask) (Locators.to_string h.locators)

end

(**
      7 6 5 4 3 2 1 0
     +-+-+-+-+-+-+-+-+
     |X|X|P|  OPEN   |
     +-------+-------+
     | VMaj  | VMin  |  -- Protocol Version VMaj.VMin
     +-------+-------+
     ~      PID      ~
     +---------------+
     ~ lease_period  ~ -- Expressed in 100s of msec
     +---------------+
     ~    Locators   ~ -- Locators (e.g. multicast) not known due to asymetric knowledge (i.e. HELLO not seen)
     +---------------+
     |  Properties   |
     +---------------+

     - P = 1 => Properties are present and these are used to define (1) SeqLen (2 bytes by default),
     (2) Authorisation, (3) Protocol propertoes such as Commit behaviour, (4) Vendor ID,  etc...

     @TODO: Do we need to add locators for this peer? For client to-broker the client
           always communicates to the broker provided locators
**)
module Open = struct
  type t = {
    header : Header.t;
    markers : Markers.t;
    version : char;
    pid : IOBuf.t;
    lease : Vle.t;
    locators : Locators.t;
    properties : Properties.t;
  }

  let create version pid lease locators properties =
    let header =
      let pflag = match properties with | [] -> 0 | _ -> int_of_char Flags.pFlag in
      let mid = int_of_char MessageId.openId in
      char_of_int @@ pflag lor mid in
    {header=header; version=version; pid=pid; lease=lease; locators=locators; properties=properties; markers=Markers.empty}
  
  let header open_ = open_.header
  
  let markers msg = msg.markers
  let with_marker msg marker = {msg with markers=Markers.with_marker msg.markers marker}
  let with_markers msg markers = {msg with markers=Markers.with_markers msg.markers markers}
  let remove_markers msg = {msg with markers=Markers.empty}

  let version open_ = open_.version
  let pid open_ = open_.pid
  let lease open_ = open_.lease
  let locators open_ = open_.locators
  let properties open_ = open_.properties
end

(**
      7 6 5 4 3 2 1 0
     +-+-+-+-+-+-+-+-+
     |X|X|P|  ACCEPT |
     +---------------+
     ~     OPID      ~  -- PID of the sender of the OPEN
     +---------------+  -- PID of the "responder" to the OPEN, i.e. accepting entity.
     ~     APID      ~
     +---------------+
     ~ lease_period  ~ -- Expressed in 100s of msec
     +---------------+
     |  Properties   |
     +---------------+
 **)
module Accept = struct
  type t = {
    header : Header.t;
    markers : Markers.t;
    opid : IOBuf.t;
    apid : IOBuf.t;
    lease : Vle.t;
    properties : Properties.t;
  }

  let create opid apid lease properties =
    let header =
      let pflag = match properties with | [] -> 0 | _ -> int_of_char Flags.pFlag in
      let mid = int_of_char MessageId.acceptId in
      char_of_int @@ pflag lor mid in
    {header=header; opid=opid; apid=apid; lease=lease; properties=properties; markers=Markers.empty}

  let header accept = accept.header
  
  let markers msg = msg.markers
  let with_marker msg marker = {msg with markers=Markers.with_marker msg.markers marker}
  let with_markers msg markers = {msg with markers=Markers.with_markers msg.markers markers}
  let remove_markers msg = {msg with markers=Markers.empty}

  let apid accept = accept.apid
  let opid accept = accept.opid
  let lease accept = accept.lease
  let properties accept = accept.properties
end

(**
        7 6 5 4 3 2 1 0
       +-+-+-+-+-+-+-+-+
       |X|X|X|   CLOSE |
       +---------------+
       ~      PID      ~ -- The PID of the entity that wants to close
       +---------------+
       |     Reason    |
       +---------------+

       - The protocol reserves reasons in the set [0, 127] U {255}
 **)
module Close = struct
  type t = {
    header : Header.t;
    markers : Markers.t;
    pid : IOBuf.t;
    reason : char;
  }

  let create pid reason = {header=MessageId.closeId; pid=pid; reason=reason; markers=Markers.empty}

  let header close = close.header
  
  let markers msg = msg.markers
  let with_marker msg marker = {msg with markers=Markers.with_marker msg.markers marker}
  let with_markers msg markers = {msg with markers=Markers.with_markers msg.markers markers}
  let remove_markers msg = {msg with markers=Markers.empty}

  let pid close = close.pid
  let reason close = close.reason
end

(**
        7 6 5 4 3 2 1 0
       +-+-+-+-+-+-+-+-+
       |X|X|X|   K_A   |
       +---------------+
       ~      PID      ~ -- The PID of peer that wants to renew the lease.
       +---------------+
 **)
module KeepAlive = struct
  type t = {
    header : Header.t;
    markers : Markers.t;
    pid : IOBuf.t;
  }

  let create pid = {header=MessageId.keepAliveId; pid=pid; markers=Markers.empty}

  let header keep_alive = keep_alive.header
  
  let markers msg = msg.markers
  let with_marker msg marker = {msg with markers=Markers.with_marker msg.markers marker}
  let with_markers msg markers = {msg with markers=Markers.with_markers msg.markers markers}
  let remove_markers msg = {msg with markers=Markers.empty}

  let pid keep_alive = keep_alive.pid
end

(**
        7 6 5 4 3 2 1 0
       +-+-+-+-+-+-+-+-+
       |X|C|S| DECLARE |
       +---------------+
       ~      sn       ~
       +---------------+
       ~ declarations  ~
       +---------------+
 **)
module Declare = struct
  type t = {
    header : Header.t;
    markers : Markers.t;
    sn : Vle.t;
    declarations : Declarations.t;
  }

  let create (sync, committed) sn declarations =
    let header =
      let sflag = if sync then int_of_char Flags.sFlag  else 0 in
      let cflag = if committed then int_of_char Flags.cFlag  else 0 in
      let mid = int_of_char MessageId.declareId in
      char_of_int @@ sflag lor cflag lor mid in
    {header=header; sn=sn; declarations=declarations; markers=Markers.empty}

  let header declare = declare.header
  
  let markers msg = msg.markers
  let with_marker msg marker = {msg with markers=Markers.with_marker msg.markers marker}
  let with_markers msg markers = {msg with markers=Markers.with_markers msg.markers markers}
  let remove_markers msg = {msg with markers=Markers.empty}

  let sn declare = declare.sn
  let declarations declare = declare.declarations
  let sync declare = ((int_of_char declare.header) land (int_of_char Flags.sFlag)) <> 0
  let committed declare = ((int_of_char declare.header) land (int_of_char Flags.cFlag)) <> 0
end

module WriteData = struct
  type t = {
    header : Header.t;
    markers : Markers.t;
    sn : Vle.t;
    resource : string;
    payload: IOBuf.t;
  }

  let create (s, r) sn resource payload =
    let header  =
      let sflag =  if s then int_of_char Flags.sFlag  else 0 in
      let rflag =  if r then int_of_char Flags.rFlag  else 0 in
      let mid = int_of_char MessageId.wdataId in
      char_of_int @@ sflag lor rflag lor mid in
    { header; sn; resource; payload; markers=Markers.empty}

  let header d = d.header
  
  let markers msg = msg.markers
  let with_marker msg marker = {msg with markers=Markers.with_marker msg.markers marker}
  let with_markers msg markers = {msg with markers=Markers.with_markers msg.markers markers}
  let remove_markers msg = {msg with markers=Markers.empty}

  let sn d = d.sn
  let resource d = d.resource
  let reliable d = Flags.hasFlag d.header Flags.rFlag
  let synch d = Flags.hasFlag d.header Flags.sFlag
  let payload d = d.payload
  let with_sn d nsn = {d with sn = nsn}
end

module StreamData = struct
  type t = {
    header : Header.t;
    markers : Markers.t;
    sn : Vle.t;
    id : Vle.t;
    prid : Vle.t option;
    payload: IOBuf.t;
  }

  let create (s, r) sn id prid payload =
    let header  =
      let sflag =  if s then int_of_char Flags.sFlag  else 0 in
      let rflag =  if r then int_of_char Flags.rFlag  else 0 in
      let aflag = match prid with | None -> 0 | _ -> int_of_char Flags.aFlag in
      let mid = int_of_char MessageId.sdataId in
      char_of_int @@ sflag lor rflag lor aflag lor mid in
    { header; sn; id; prid; payload; markers=Markers.empty}

  let header d = d.header
  
  let markers msg = msg.markers
  let with_marker msg marker = {msg with markers=Markers.with_marker msg.markers marker}
  let with_markers msg markers = {msg with markers=Markers.with_markers msg.markers markers}
  let remove_markers msg = {msg with markers=Markers.empty}

  let sn d = d.sn
  let id d = d.id
  let reliable d = Flags.hasFlag d.header Flags.rFlag
  let synch d = Flags.hasFlag d.header Flags.sFlag
  let prid d = d.prid
  let payload d = d.payload
  let with_sn d nsn = {d with sn = nsn}
end

module Synch = struct
  type t = {
    header : Header.t;
    markers : Markers.t;
    sn : Vle.t;
    count : Vle.t option
  }
  let create (s, r) sn count =
    let header =
      let uflag = match count with | None -> 0 | _ -> int_of_char Flags.uFlag in
      let rflag = if r then int_of_char Flags.rFlag else 0 in
      let sflag = if s then int_of_char Flags.sFlag else 0 in
      let mid = int_of_char MessageId.synchId in
      char_of_int @@ uflag lor rflag lor sflag lor mid
    in { header; sn; count; markers=Markers.empty}

  let header s = s.header
  
  let markers msg = msg.markers
  let with_marker msg marker = {msg with markers=Markers.with_marker msg.markers marker}
  let with_markers msg markers = {msg with markers=Markers.with_markers msg.markers markers}
  let remove_markers msg = {msg with markers=Markers.empty}

  let sn s = s .sn
  let count s = s.count
end

module AckNack = struct
  type t = {
    header : Header.t;
    markers : Markers.t;
    sn : Vle.t;
    mask :Vle.t option
  }

  let create sn mask =
    let header =
      let mflag = match mask with | None -> 0 | _ -> int_of_char Flags.mFlag in
      let mid = int_of_char MessageId.ackNackId in
      char_of_int @@ mflag lor mid
    in  { header; sn; mask; markers=Markers.empty}

  let header a = a.header
  
  let markers msg = msg.markers
  let with_marker msg marker = {msg with markers=Markers.with_marker msg.markers marker}
  let with_markers msg markers = {msg with markers=Markers.with_markers msg.markers markers}
  let remove_markers msg = {msg with markers=Markers.empty}

  let sn a = a.sn
  let mask a = a.mask
end

module Migrate = struct
  type t = {
    header : Header.t;
    markers : Markers.t;
    ocid : Vle.t;
    id : Vle.t option;
    rch_last_sn : Vle.t;
    bech_last_sn : Vle.t;
  }

  let create ocid id rch_last_sn bech_last_sn =
    let header  =
      let iflag = match id with | None -> 0 | _ -> int_of_char Flags.iFlag in
      let mid = int_of_char MessageId.migrateId in
      char_of_int @@ iflag lor mid in
    { header; ocid; id; rch_last_sn; bech_last_sn; markers=Markers.empty}

  let header m = m.header
  
  let markers msg = msg.markers
  let with_marker msg marker = {msg with markers=Markers.with_marker msg.markers marker}
  let with_markers msg markers = {msg with markers=Markers.with_markers msg.markers markers}
  let remove_markers msg = {msg with markers=Markers.empty}

  let ocid m = m.ocid
  let id m = m.id
  let rch_last_sn m = m.rch_last_sn
  let bech_last_sn m = m.bech_last_sn
end

module Pull = struct
  type t = {
    header : Header.t;
    markers : Markers.t;
    sn : Vle.t;
    id : Vle.t;
    max_samples : Vle.t option;
  }

  let create (s, f) sn id max_samples =
    let header  =
      let sflag =  if s then int_of_char Flags.sFlag  else 0 in
      let fflag =  if f then int_of_char Flags.fFlag  else 0 in
      let nflag = match max_samples with | None -> 0 | _ -> int_of_char Flags.nFlag in
      let mid = int_of_char MessageId.pullId in
      char_of_int @@ sflag lor nflag lor fflag lor mid in
    { header; sn; id; max_samples; markers=Markers.empty}

  let header p = p.header
  
  let markers msg = msg.markers
  let with_marker msg marker = {msg with markers=Markers.with_marker msg.markers marker}
  let with_markers msg markers = {msg with markers=Markers.with_markers msg.markers markers}
  let remove_markers msg = {msg with markers=Markers.empty}

  let sn p = p.sn
  let id p = p.id
  let max_samples p = p.max_samples
  let final p = Flags.hasFlag p.header Flags.fFlag
  let sync p = Flags.hasFlag p.header Flags.sFlag
end

module PingPong = struct
  type t = {
    header : Header.t;
    markers : Markers.t;
    hash : Vle.t;
  }

  let create ?pong:(pong=false) hash =
    let header  =
      let oflag =  if pong then int_of_char Flags.oFlag  else 0 in
      let mid = int_of_char MessageId.pingPongId in
      char_of_int @@ oflag lor mid in
    { header; hash; markers=Markers.empty}

  let header p = p.header
  
  let markers msg = msg.markers
  let with_marker msg marker = {msg with markers=Markers.with_marker msg.markers marker}
  let with_markers msg markers = {msg with markers=Markers.with_markers msg.markers markers}
  let remove_markers msg = {msg with markers=Markers.empty}

  let is_pong p = Flags.hasFlag p.header Flags.oFlag
  let hash p = p.hash
  let to_pong p = {p with header = char_of_int @@ int_of_char p.header lor int_of_char Flags.oFlag}
end

module Message = struct
  type t =
    | Scout of Scout.t
    | Hello of Hello.t
    | Open of Open.t
    | Accept of Accept.t
    | Close of Close.t
    | Declare of Declare.t
    | WriteData of WriteData.t
    | StreamData of StreamData.t
    | Synch of Synch.t
    | AckNack of AckNack.t
    | KeepAlive of KeepAlive.t
    | Migrate of Migrate.t
    | Pull of Pull.t
    | PingPong of PingPong.t

  let header = function 
    | Scout s -> Scout.header s
    | Hello h ->  Hello.header h
    | Open o ->  Open.header o
    | Accept a ->  Accept.header a
    | Close c ->  Close.header c
    | Declare d ->  Declare.header d
    | WriteData d ->  WriteData.header d
    | StreamData d ->  StreamData.header d
    | Synch s ->  Synch.header s
    | AckNack a ->  AckNack.header a
    | KeepAlive a ->  KeepAlive.header a
    | Migrate m ->  Migrate.header m
    | Pull p ->  Pull.header p
    | PingPong p ->  PingPong.header p
  
  let markers = function
    | Scout s -> Scout.markers s
    | Hello h ->  Hello.markers h
    | Open o ->  Open.markers o
    | Accept a ->  Accept.markers a
    | Close c ->  Close.markers c
    | Declare d ->  Declare.markers d
    | WriteData d ->  WriteData.markers d
    | StreamData d ->  StreamData.markers d
    | Synch s ->  Synch.markers s
    | AckNack a ->  AckNack.markers a
    | KeepAlive a ->  KeepAlive.markers a
    | Migrate m ->  Migrate.markers m
    | Pull p ->  Pull.markers p
    | PingPong p ->  PingPong.markers p

  let with_marker msg marker = match msg with 
    | Scout s -> Scout (Scout.with_marker s marker)
    | Hello h ->  Hello (Hello.with_marker h marker)
    | Open o ->  Open (Open.with_marker o marker)
    | Accept a ->  Accept (Accept.with_marker a marker)
    | Close c ->  Close (Close.with_marker c marker)
    | Declare d ->  Declare (Declare.with_marker d marker)
    | WriteData d ->  WriteData (WriteData.with_marker d marker)
    | StreamData d ->  StreamData (StreamData.with_marker d marker)
    | Synch s ->  Synch (Synch.with_marker s marker)
    | AckNack a ->  AckNack (AckNack.with_marker a marker)
    | KeepAlive a ->  KeepAlive (KeepAlive.with_marker a marker)
    | Migrate m ->  Migrate (Migrate.with_marker m marker)
    | Pull p ->  Pull (Pull.with_marker p marker)
    | PingPong p ->  PingPong (PingPong.with_marker p marker)

  let with_markers msg markers = match msg with 
    | Scout s -> Scout (Scout.with_markers s markers)
    | Hello h ->  Hello (Hello.with_markers h markers)
    | Open o ->  Open (Open.with_markers o markers)
    | Accept a ->  Accept (Accept.with_markers a markers)
    | Close c ->  Close (Close.with_markers c markers)
    | Declare d ->  Declare (Declare.with_markers d markers)
    | WriteData d ->  WriteData (WriteData.with_markers d markers)
    | StreamData d ->  StreamData (StreamData.with_markers d markers)
    | Synch s ->  Synch (Synch.with_markers s markers)
    | AckNack a ->  AckNack (AckNack.with_markers a markers)
    | KeepAlive a ->  KeepAlive (KeepAlive.with_markers a markers)
    | Migrate m ->  Migrate (Migrate.with_markers m markers)
    | Pull p ->  Pull (Pull.with_markers p markers)
    | PingPong p ->  PingPong (PingPong.with_markers p markers)
    
  let remove_markers = function
    | Scout s -> Scout (Scout.remove_markers s)
    | Hello h ->  Hello (Hello.remove_markers h)
    | Open o ->  Open (Open.remove_markers o)
    | Accept a ->  Accept (Accept.remove_markers a)
    | Close c ->  Close (Close.remove_markers c)
    | Declare d ->  Declare (Declare.remove_markers d)
    | WriteData d ->  WriteData (WriteData.remove_markers d)
    | StreamData d ->  StreamData (StreamData.remove_markers d)
    | Synch s ->  Synch (Synch.remove_markers s)
    | AckNack a ->  AckNack (AckNack.remove_markers a)
    | KeepAlive a ->  KeepAlive (KeepAlive.remove_markers a)
    | Migrate m ->  Migrate (Migrate.remove_markers m)
    | Pull p ->  Pull (Pull.remove_markers p)
    | PingPong p ->  PingPong (PingPong.remove_markers p)
  
  let to_string = function (** This should actually call the to_string on individual messages *)
    | Scout s -> "Scout"
    | Hello h -> Hello.to_string h
    | Open o -> "Open"
    | Accept a -> "Accept"
    | Close c -> "Close"
    | Declare d -> "Declare"
    | WriteData d -> "WriteData"
    | StreamData d -> "StreamData"
    | Synch s -> "Synch"
    | AckNack a -> "AckNack"
    | KeepAlive a -> "KeepAlive"
    | Migrate m -> "Migrate"
    | Pull p -> "Pull"
    | PingPong p -> "PingPong"

    let make_scout s = Scout s
    let make_hello h = Hello h
    let make_open o = Open o
    let make_accept a =  Accept a
    let make_close c =  Close c
    let make_declare d = Declare d
    let make_stream_data sd =  StreamData sd
    let make_synch s = Synch s
    let make_ack_nack a = AckNack a
    let make_keep_alive a = KeepAlive a
end
