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

  let midMask = char_of_int 0x1f
  let hFlagMask = char_of_int 0xe0

end

module ScoutFlags = struct
  let scoutBroker = char_of_int 0x01
  let scoutDurability = char_of_int 0x02
  let scoutPeer = char_of_int 0x04
  let scoutClient = char_of_int 0x08
end

module Header = struct
  type t = char
  let mid h =  (int_of_char h) land (int_of_char Flags.midMask)
  let flags h = (int_of_char h) land (int_of_char Flags.hFlagMask)
end

module type Msg =
sig
  type t
  val header : t -> char
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
    match properties with
    | [] -> {header=MessageId.scoutId; mask=mask; properties=properties}
    | _ -> {header=char_of_int ((int_of_char MessageId.scoutId) lor (int_of_char Flags.pFlag));
            mask=mask; properties=properties}
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
    match properties with
    | [] -> {header=MessageId.helloId; mask=mask; locators=locators; properties=properties}
    | _ -> {header=char_of_int ((int_of_char MessageId.helloId) lor (int_of_char Flags.pFlag));
            mask=mask; locators=locators; properties=properties}
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
    match properties with
    | [] -> {header=MessageId.openId; version=version; pid=pid; lease=lease; locators=locators; properties=properties}
    | _ -> {header=char_of_int ((int_of_char MessageId.openId) lor (int_of_char Flags.pFlag));
            version=version; pid=pid; lease=lease; locators=locators; properties=properties}
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
    match properties with
    | [] -> {header=MessageId.acceptId; opid=opid; apid=apid; lease=lease; properties=properties}
    | _ -> {header=char_of_int ((int_of_char MessageId.acceptId) lor (int_of_char Flags.pFlag));
            opid=opid; apid=apid; lease=lease; properties=properties}
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

module Message = struct
  type t =
    | Scout of Scout.t
    | Hello of Hello.t
    | Open of Open.t
    | Accept of Accept.t
    | Close of Close.t

  (* let put msg bs = match msg with
      Scout s -> Bytes. *)

end
