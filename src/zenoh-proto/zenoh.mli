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
  val pushModeId : Ztypes.Vle.t
  val pullModeId : Ztypes.Vle.t
  val periodicPushModeId : Ztypes.Vle.t
  val periodicPullModeId : Ztypes.Vle.t
end

module SubscriptionMode :
sig
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

module ResourceDecl :
sig
  include Headed
  val create : Ztypes.Vle.t -> string -> Ztypes.Properties.t -> t
  val rid : t -> Ztypes.Vle.t
  val resource : t -> string
  val properties : t -> Ztypes.Properties.t
end

module PublisherDecl :
sig
  include Headed
  val create : Ztypes.Vle.t -> Ztypes.Properties.t -> t
  val rid : t -> Ztypes.Vle.t
  val properties : t -> Ztypes.Properties.t
end

module  SubscriberDecl :
sig
  include Headed
  val create : Ztypes.Vle.t -> SubscriptionMode.t -> Ztypes.Properties.t -> t
  val rid : t -> Ztypes.Vle.t
  val mode : t -> SubscriptionMode.t
  val properties : t -> Ztypes.Properties.t
end

module SelectionDecl :
sig
  include Headed
  val create : Ztypes.Vle.t -> string -> Ztypes.Properties.t -> bool -> t
  val sid : t -> Ztypes.Vle.t
  val query : t -> string
  val properties : t -> Ztypes.Properties.t
  val global : t -> bool
end

module BindingDecl :
sig
  include Headed
  val create : Ztypes.Vle.t -> Ztypes.Vle.t -> bool -> t
  val oldId : t -> Ztypes.Vle.t
  val newId : t -> Ztypes.Vle.t
  val global : t -> bool
end

module CommitDecl :
sig
  include Headed
  val create : char -> t
  val commitId : t -> char
end

module ResultDecl :
sig
  include Headed
  val create : char -> char -> Ztypes.Vle.t -> t
  val commitId : t -> char
  val status : t -> char
  val id : t -> Ztypes.Vle.t
end

module ForgetResourceDecl :
sig
  include Headed
  val create : Ztypes.Vle.t -> t
  val rid : t -> Ztypes.Vle.t
end

module ForgetPublisherDecl :
sig
  include Headed
  val create : Ztypes.Vle.t -> t
  val id : t -> Ztypes.Vle.t
end

module ForgetSubscriberDecl :
sig
  include Headed
  val create : Ztypes.Vle.t -> t
  val id : t -> Ztypes.Vle.t
end

module ForgetSelectionDecl :
sig
  include Headed
  val create : Ztypes.Vle.t -> t
  val sid : t -> Ztypes.Vle.t
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

  val  empty : t
  val singleton : Declaration.t -> t
  val add : t -> Declaration.t -> t
end

module Scout :
sig
  include Headed
  val create : Ztypes.Vle.t -> Ztypes.Properties.t -> t
  val mask : t -> Ztypes.Vle.t
  val properties : t -> Ztypes.Properties.t
end

module Hello :
sig
  include Headed
  val create : Ztypes.Vle.t -> Ztypes.Locators.t -> Ztypes.Properties.t -> t
  val mask : t -> Ztypes.Vle.t
  val locators : t -> Ztypes.Locators.t
  val properties : t -> Ztypes.Properties.t
end

module Open :
sig
  include Headed
  val create : char -> Lwt_bytes.t -> Ztypes.Vle.t -> Ztypes.Locators.t -> Ztypes.Properties.t -> t
  val version : t -> char
  val pid : t -> Lwt_bytes.t
  val lease : t -> Ztypes.Vle.t
  val locators : t -> Ztypes.Locators.t
  val properties : t -> Ztypes.Properties.t
end

module Accept :
sig
  include Headed
  val create : Lwt_bytes.t -> Lwt_bytes.t -> Ztypes.Vle.t -> Ztypes.Properties.t -> t
  val opid : t -> Lwt_bytes.t
  val apid : t -> Lwt_bytes.t
  val lease : t -> Ztypes.Vle.t
  val properties : t -> Ztypes.Properties.t
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
  val create : Ztypes.Vle.t -> Declaration.t list -> bool -> bool -> t
  val sn : t -> Ztypes.Vle.t
  val declarations : t -> Declaration.t list
  val sync : t -> bool
  val committed : t -> bool
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

    val to_string : t -> string

  end
