module MessageId :
sig
  val scoutId : char
  val helloId : char
  val openId : char
  val acceptId : char
  val closeId : char
  val declareId : char
  val sdataId : char
  val bdataId : char
  val wdataId : char
  val queryId : char
  val pullId : char
  val pingId : char
  val pongId : char
  val synchId : char
  val ackNackId : char
  val keepAliveId : char
  val conduitCloseId : char
  val fragmetsId : char
  val conduitId : char
  val migrateId : char
  val sdeltaDataId : char
  val bdeltaDataId : char
  val wdeltaDataId : char
end

module Flags :
sig
  val sFlag : char
  val mFlag : char
  val pFlag : char
  val rFlag : char
  val nFlag : char
  val cFlag : char
  val aFlag : char
  val uFlag : char
  val zFlag : char
  val lFlag : char
  val hFlag : char
  val midMask : char
  val hFlagMask : char
end

module ScoutFlags :
sig
  val scoutBroker : char
  val scoutDurability : char
  val scoutPeer : char
  val scoutClient : char
end

module Header :
sig
  type t = char
  val mid : char -> int
  val flags : char -> int
end

module type Msg =
sig
  type t
  val header : t -> char
end

module Scout :
sig
  include Msg
  val create : Ztypes.Vle.t -> Ztypes.Properties.t -> t
  val mask : t -> Ztypes.Vle.t
  val properties : t -> Ztypes.Properties.t
end

module Hello :
sig
  include Msg
  val create : Ztypes.Vle.t -> Ztypes.Locators.t -> Ztypes.Properties.t -> t
  val mask : t -> Ztypes.Vle.t
  val locators : t -> Ztypes.Locators.t
  val properties : t -> Ztypes.Properties.t
end

module Open :
sig
  include Msg
  val create : char -> Lwt_bytes.t -> Ztypes.Vle.t -> Ztypes.Locators.t -> Ztypes.Properties.t -> t
  val version : t -> char
  val pid : t -> Lwt_bytes.t
  val lease : t -> Ztypes.Vle.t
  val locators : t -> Ztypes.Locators.t
  val properties : t -> Ztypes.Properties.t
end

module Accept :
sig
  include Msg
  val create : Lwt_bytes.t -> Lwt_bytes.t -> Ztypes.Vle.t -> Ztypes.Properties.t -> t
  val opid : t -> Lwt_bytes.t
  val apid : t -> Lwt_bytes.t
  val lease : t -> Ztypes.Vle.t
  val properties : t -> Ztypes.Properties.t
end

module Close :
sig
  include Msg
  val create : Lwt_bytes.t -> char -> t
  val pid : t -> Lwt_bytes.t
  val reason : t -> char
end

module Message :
  sig
    type t =
        Scout of Scout.t
      | Hello of Hello.t
      | Open of Open.t
      | Accept of Accept.t
      | Close of Close.t
  end
