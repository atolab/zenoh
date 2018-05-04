open Ztypes
open Pervasives

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
  let pushModeId = Vle.of_int 0x01
  let pullModeId = Vle.of_int 0x02
  let periodicPushModeId = Vle.of_int 0x03
  let periodicPullModeId = Vle.of_int 0x04
end

module SubscriptionMode = struct
  type timing = {
    temporalOrigin : Ztypes.Vle.t;
    period : Ztypes.Vle.t;
    duration : Ztypes.Vle.t;
  }
  type t =
    | PushMode
    | PullMode
    | PeriodicPushMode of timing
    | PeriodicPullMode of timing
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
    oldId : Vle.t;
    newId : Vle.t;
  }

  let create oldId newId global =
    let header = match global with
      | false -> DeclarationId.bindingDeclId
      | true -> char_of_int ((int_of_char DeclarationId.bindingDeclId) lor (int_of_char Flags.gFlag))
    in {header=header; oldId=oldId; newId=newId}
  let header bindingDecl = bindingDecl.header
  let oldId bindingDecl = bindingDecl.oldId
  let newId bindingDecl = bindingDecl.newId
  let global selectionDecl = ((int_of_char selectionDecl.header) land (int_of_char Flags.gFlag)) <> 0
end

module CommitDecl = struct
  type t = {
    header : Header.t;
    commitId : char;
  }

  let create commitId = {header=DeclarationId.commitDeclId; commitId=commitId}
  let header commitDecl = commitDecl.header
  let commitId commitDecl = commitDecl.commitId
end

module ResultDecl = struct
  type t = {
    header : Header.t;
    commitId : char;
    status : char;
    id : Vle.t;
  }

  let create commitId status id = {header=DeclarationId.resultDeclId; commitId=commitId; status=status; id=id}
  let header resultDecl = resultDecl.header
  let commitId resultDecl = resultDecl.commitId
  let status resultDecl = resultDecl.commitId
  let id resultDecl = resultDecl.id
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
    pid : Lwt_bytes.t;
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
    opid : Lwt_bytes.t;
    apid : Lwt_bytes.t;
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
    pid : Lwt_bytes.t;
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
    pid : Lwt_bytes.t;
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
    declarations : Declaration.t list;
  }

  let create sn declarations sync committed =
    let header = match sync with
      | false -> (match committed with
          | false -> MessageId.declareId
          | true -> char_of_int ((int_of_char MessageId.declareId) lor (int_of_char Flags.sFlag)))
      | true -> (match committed with
          | false -> char_of_int ((int_of_char MessageId.declareId) lor (int_of_char Flags.cFlag))
          | true -> char_of_int ((int_of_char MessageId.declareId) lor (int_of_char Flags.cFlag) lor (int_of_char Flags.sFlag)))
    in {header=header; sn=sn; declarations=declarations}
  let header declare = declare.header
  let sn declare = declare.sn
  let declarations declare = declare.declarations
  let sync declare = ((int_of_char declare.header) land (int_of_char Flags.sFlag)) <> 0
  let committed declare = ((int_of_char declare.header) land (int_of_char Flags.cFlag)) <> 0
end

module Message = struct
  type t =
    | Scout of Scout.t
    | Hello of Hello.t
    | Open of Open.t
    | Accept of Accept.t
    | Close of Close.t
end
