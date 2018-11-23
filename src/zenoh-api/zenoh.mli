open Iobuf

type sub
type pub
type sto
type sublistener = IOBuf.t -> string -> unit Lwt.t
type queryreply = 
  | StorageData of {stoid:IOBuf.t; rsn:int; resname:string; data:IOBuf.t}
  | StorageFinal of {stoid:IOBuf.t; rsn:int}
  | ReplyFinal 
type rephandler = queryreply -> unit Lwt.t
type quehandler = string -> string -> (string * IOBuf.t) list Lwt.t
type submode
type t

val zopen : string -> t Lwt.t

val publish : string -> t -> pub Lwt.t

val unpublish : pub -> t -> unit Lwt.t

val write : IOBuf.t -> string -> t -> unit Lwt.t

val stream : IOBuf.t -> pub -> unit Lwt.t

val push_mode : submode

val pull_mode : submode

val subscribe : string -> sublistener -> ?mode:submode -> t -> sub Lwt.t

val pull : sub -> unit Lwt.t

val unsubscribe : sub -> t -> unit Lwt.t

val store : string -> sublistener -> quehandler -> t -> sto Lwt.t

val query : string -> string -> rephandler -> ?quorum:int -> ?max_samples:int -> t -> unit Lwt.t

val unstore : sto -> t -> unit Lwt.t

(* val terminate : t -> unit  *)