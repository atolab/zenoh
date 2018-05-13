open Ztypes
open Pervasives
open Apero
open Netbuf

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

  let pingId = char_of_int 0x0c
  let pongId = char_of_int 0x0d

  let synchId = char_of_int 0x0e
  let ackNackId = char_of_int 0x0f

  let keepAliveId = char_of_int 0x10

  let conduitCloseId = char_of_int 0x11
  let fragmetsId = char_of_int 0x12

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

  let midMask = char_of_int 0x1f
  let hFlagMask = char_of_int 0xe0

  let hasFlag h f =  (int_of_char h) land (int_of_char f) <> 0
  let mid h =  char_of_int @@ (int_of_char h) land  (int_of_char midMask)
  let flags h = char_of_int @@ (int_of_char h) land  (int_of_char hFlagMask)

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

module Property = struct
  type id_t = Vle.t
  type t = Vle.t * IOBuf.t
  let create id data = (id, data)
  let id p = fst p
  let data p = snd p

end

module Properties = struct
  type t = Property.t list
  let empty = []
  let singleton p = [p]
  let add p ps = p::ps
  let find f ps = List.find_opt f ps
  let get name ps = List.find_opt (fun (n, _) -> if n = name then true else false) ps
  let length ps = List.length ps
  let of_list xs = xs
  let to_list ps = ps
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
  val header : t -> char
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
  }

  let create mask properties =
    let header = match properties with
      | [] -> MessageId.scoutId
      | _ -> char_of_int ((int_of_char MessageId.scoutId) lor (int_of_char Flags.pFlag))
    in {header=header; mask=mask; properties=properties}
  let header scout = scout.header
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
    mask : Vle.t;
    locators : Locators.t;
    properties : Properties.t
  }

  let create mask locators properties =
    let header = match properties with
      | [] -> MessageId.helloId
      | _ -> char_of_int ((int_of_char MessageId.helloId) lor (int_of_char Flags.pFlag))
    in {header=header; mask=mask; locators=locators; properties=properties}
  let header hello = hello.header
  let mask hello = hello.mask
  let locators hello = hello.locators
  let properties hello = hello.properties
  let to_string h =
    Printf.sprintf "Hello:[header: %d, mask: %Ld, locators: %s]" (int_of_char @@ h.header) (h.mask) (List.to_string h.locators (fun l -> Locator.to_string l))

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
    version : char;
    pid : IOBuf.t;
    lease : Vle.t;
    locators : Locators.t;
    properties : Properties.t;
  }

  let create version pid lease locators properties =
    let header = match properties with
      | [] -> MessageId.openId
      | _ -> char_of_int ((int_of_char MessageId.openId) lor (int_of_char Flags.pFlag))
    in {header=header; version=version; pid=pid; lease=lease; locators=locators; properties=properties}
  let header open_ = open_.header
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
    opid : IOBuf.t;
    apid : IOBuf.t;
    lease : Vle.t;
    properties : Properties.t;
  }

  let create opid apid lease properties =
    let header = match properties with
      | [] -> MessageId.acceptId
      | _ -> char_of_int ((int_of_char MessageId.acceptId) lor (int_of_char Flags.pFlag))
    in {header=header; opid=opid; apid=apid; lease=lease; properties=properties}
  let header accept = accept.header
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
    pid : IOBuf.t;
    reason : char;
  }

  let create pid reason = {header=MessageId.closeId; pid=pid; reason=reason}
  let header close = close.header
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
    pid : IOBuf.t;
  }

  let create pid = {header=MessageId.keepAliveId; pid=pid}
  let header keep_alive = keep_alive.header
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
    sn : Vle.t;
    declarations : Declarations.t;
  }

  let create sn declarations sync committed =
    let header = match sync with
      | false -> (match committed with
          | false -> MessageId.declareId
          | true -> char_of_int ((int_of_char MessageId.declareId) lor (int_of_char Flags.cFlag)))
      | true -> (match committed with
          | false -> char_of_int ((int_of_char MessageId.declareId) lor (int_of_char Flags.sFlag))
          | true -> char_of_int ((int_of_char MessageId.declareId) lor (int_of_char Flags.cFlag) lor (int_of_char Flags.sFlag)))
    in {header=header; sn=sn; declarations=declarations}
  let header declare = declare.header
  let sn declare = declare.sn
  let declarations declare = declare.declarations
  let sync declare = ((int_of_char declare.header) land (int_of_char Flags.sFlag)) <> 0
  let committed declare = ((int_of_char declare.header) land (int_of_char Flags.cFlag)) <> 0
end

module StreamData = struct
  type t = {
    header : Header.t;
    sn : Vle.t;
    id : Vle.t;
    prid : Vle.t option;
    payload: IOBuf.t;
  }

  let header d = d.header

  let create (s, r) sn id prid payload =
    let header  =
      let sflag =  if s then int_of_char Flags.sFlag  else 0 in
      let rflag =  if s then int_of_char Flags.rFlag  else 0 in
      let aflag = match prid with | None -> 0 | _ -> int_of_char Flags.aFlag in
      let mid = int_of_char MessageId.sdataId in
      char_of_int @@ sflag lor rflag lor aflag lor mid in
    { header; sn; id; prid; payload}

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
    sn : Vle.t;
    count : Vle.t option
  }
  let  create (r, s) sn count =
    let header =
      let uflag = match count with | None -> 0 | _ -> int_of_char Flags.uFlag in
      let rflag = if r then int_of_char Flags.rFlag else 0 in
      let sflag = if s then int_of_char Flags.sFlag else 0 in
      let mid = int_of_char MessageId.synchId in
      char_of_int @@ uflag lor rflag lor sflag lor mid
    in { header; sn; count }

  let header s = s.header
  let sn s = s .sn
  let count s = s.count
end

module AckNack = struct
  type t = {
    header : Header.t;
    sn : Vle.t;
    mask :Vle.t option
  }

  let create sn mask =
    let header =
      let mflag = match mask with | None -> 0 | _ -> int_of_char Flags.mFlag in
      let mid = int_of_char MessageId.ackNackId in
      char_of_int @@ mflag lor mid
    in  { header; sn; mask }

  let header a = a.header
  let sn a = a.sn
  let mask a = a.mask
end

module Message = struct
  type t =
    | Scout of Scout.t
    | Hello of Hello.t
    | Open of Open.t
    | Accept of Accept.t
    | Close of Close.t
    | Declare of Declare.t
    | StreamData of StreamData.t
    | Synch of Synch.t
    | AckNack of AckNack.t
    | KeepAlive of KeepAlive.t


  let to_string = function (** This should actually call the to_string on individual messages *)
    | Scout s -> "Scout"
    | Hello h -> Hello.to_string h
    | Open o -> "Open"
    | Accept a -> "Accept"
    | Close c -> "Close"
    | Declare d -> "Declare"
    | StreamData d -> "StreamData"
    | Synch s -> "Synch"
    | AckNack a -> "AckNack"
    | KeepAlive a -> "KeepAlive"

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
