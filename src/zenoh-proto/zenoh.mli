open Ztypes
open Netbuf

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
  val gFlag : char
  val midMask : char
  val hFlagMask : char

  val hasFlag : char -> char -> bool
  val mid : char -> char
  val flags : char -> char

end

module ScoutFlags :
sig
  val scoutBroker : char
  val scoutDurability : char
  val scoutPeer : char
  val scoutClient : char
end

module DeclarationId :
sig
  val resourceDeclId : char
  val publisherDeclId : char
  val subscriberDeclId : char
  val selectionDeclId : char
  val bindingDeclId : char
  val commitDeclId : char
  val resultDeclId : char
  val forgetResourceDeclId : char
  val forgetPublisherDeclId : char
  val forgetSubscriberDeclId : char
  val forgetSelectionDeclId : char
end

module SubscriptionModeId :
sig
  val pushModeId : char
  val pullModeId : char
  val periodicPushModeId : char
  val periodicPullModeId : char
end

module TemporalProperties : sig
  type t = {
    origin : Vle.t;
    period : Vle.t;
    duration : Vle.t;
  }

  val create : Vle.t -> Vle.t -> Vle.t -> t
  val origin : t -> Vle.t
  val period : t -> Vle.t
  val duration : t -> Vle.t
end

module SubscriptionMode :
sig

  type t =
      | PushMode
      | PullMode
      | PeriodicPushMode of TemporalProperties.t
      | PeriodicPullMode of TemporalProperties.t

  val push_mode : t
  val pull_mode : t
  val periodic_push : TemporalProperties.t -> t
  val periodic_pull : TemporalProperties.t -> t

  val id : t -> char

  val has_temporal_properties : char -> bool

  val temporal_properties : t -> TemporalProperties.t option
end

module Header :
sig
  type t = char
  val mid : char -> int
  val flags : char -> int
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

module ResourceDecl :
sig
  include Headed
  val create : Vle.t -> string -> Properties.t -> t
  val rid : t -> Vle.t
  val resource : t -> string
  val properties : t -> Properties.t
end

module PublisherDecl :
sig
  include Headed
  val create : Vle.t -> Properties.t -> t
  val rid : t -> Vle.t
  val properties : t -> Properties.t
end

module  SubscriberDecl :
sig
  include Headed
  val create : Vle.t -> SubscriptionMode.t -> Properties.t -> t
  val rid : t -> Vle.t
  val mode : t -> SubscriptionMode.t
  val properties : t -> Properties.t
end

module SelectionDecl :
sig
  include Headed
  val create : Vle.t -> string -> Properties.t -> bool -> t
  val sid : t -> Vle.t
  val query : t -> string
  val properties : t -> Properties.t
  val global : t -> bool
end

module BindingDecl :
sig
  include Headed
  val create : Vle.t -> Vle.t -> bool -> t
  val old_id : t -> Vle.t
  val new_id : t -> Vle.t
  val global : t -> bool
end

module CommitDecl :
sig
  include Headed
  val create : char -> t
  val commit_id : t -> char
end

module ResultDecl :
sig
  include Headed
  val create : char -> char -> Vle.t option-> t
  val commit_id : t -> char
  val status : t -> char
  val id : t -> Vle.t option
end

module ForgetResourceDecl :
sig
  include Headed
  val create : Vle.t -> t
  val rid : t -> Vle.t
end

module ForgetPublisherDecl :
sig
  include Headed
  val create : Vle.t -> t
  val id : t -> Vle.t
end

module ForgetSubscriberDecl :
sig
  include Headed
  val create : Vle.t -> t
  val id : t -> Vle.t
end

module ForgetSelectionDecl :
sig
  include Headed
  val create : Vle.t -> t
  val sid : t -> Vle.t
end

module Declaration :
sig
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

module Declarations : sig
  type t = Declaration.t list

  val length : t -> int
  val  empty : t
  val singleton : Declaration.t -> t
  val add : t -> Declaration.t -> t
end

module Scout :
sig
  include Headed
  val create : Vle.t -> Properties.t -> t
  val mask : t -> Vle.t
  val properties : t -> Properties.t
end

module Hello :
sig
  include Headed
  val create : Vle.t -> Locators.t -> Properties.t -> t
  val mask : t -> Vle.t
  val locators : t -> Locators.t
  val properties : t -> Properties.t
end

module Open :
sig
  include Headed
  val create : char -> Lwt_bytes.t -> Vle.t -> Locators.t -> Properties.t -> t
  val version : t -> char
  val pid : t -> Lwt_bytes.t
  val lease : t -> Vle.t
  val locators : t -> Locators.t
  val properties : t -> Properties.t
end

module Accept :
sig
  include Headed
  val create : Lwt_bytes.t -> Lwt_bytes.t -> Vle.t -> Properties.t -> t
  val opid : t -> Lwt_bytes.t
  val apid : t -> Lwt_bytes.t
  val lease : t -> Vle.t
  val properties : t -> Properties.t
end

module Close :
sig
  include Headed
  val create : Lwt_bytes.t -> char -> t
  val pid : t -> Lwt_bytes.t
  val reason : t -> char
end

module KeepAlive :
sig
  include Headed
  val create : Lwt_bytes.t -> t
  val pid : t -> Lwt_bytes.t
end

module Declare :
sig
  include Headed
  val create : Vle.t -> Declaration.t list -> bool -> bool -> t
  val sn : t -> Vle.t
  val declarations : t -> Declaration.t list
  val sync : t -> bool
  val committed : t -> bool
end

module StreamData :
sig
  include  Reliable
  val create : bool * bool -> Vle.t -> Vle.t -> Vle.t option -> IOBuf.t -> t
  val id : t -> Vle.t
  val prid : t -> Vle.t option
  val payload : t -> IOBuf.t
end

module Synch :
sig
  include Headed
  val create : bool * bool -> Vle.t -> Vle.t option -> t
  val sn : t -> Vle.t
  val count : t -> Vle.t option
end

module AckNack :
sig
  include Headed
  val create : Vle.t -> Vle.t option -> t
  val sn : t -> Vle.t
  val mask : t -> Vle.t option
end


module Message :
  sig
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


    val to_string : t -> string

    val make_scout : Scout.t -> t
    val make_hello : Hello.t -> t
    val make_open : Open.t -> t
    val make_accept : Accept.t -> t
    val make_close : Close.t -> t
    val make_declare : Declare.t -> t
    val make_stream_data : StreamData.t -> t
    val make_synch : Synch.t -> t
    val make_ack_nack : AckNack.t -> t

  end
