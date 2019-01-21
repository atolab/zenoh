open Iobuf

type sub
type pub
type storage
type sublistener = IOBuf.t -> string -> unit Lwt.t
type queryreply = 
  | StorageData of {stoid:IOBuf.t; rsn:int; resname:string; data:IOBuf.t}
  | StorageFinal of {stoid:IOBuf.t; rsn:int}
  | ReplyFinal 
type reply_handler = queryreply -> unit Lwt.t
type query_handler = string -> string -> (string * IOBuf.t) list Lwt.t
type submode
type t

val zopen : string -> t Lwt.t

val info : t -> Apero.properties

val publish : string -> t -> pub Lwt.t

val unpublish : pub -> t -> unit Lwt.t

val write : IOBuf.t -> string -> t -> unit Lwt.t

val stream : IOBuf.t -> pub -> unit Lwt.t

val push_mode : submode

val pull_mode : submode

val subscribe : string -> sublistener -> ?mode:submode -> t -> sub Lwt.t

val pull : sub -> unit Lwt.t

val unsubscribe : sub -> t -> unit Lwt.t

val storage : string -> sublistener -> query_handler -> t -> storage Lwt.t

val query : string -> string -> reply_handler -> ?dest:Queries.dest -> t -> unit Lwt.t

val unstore : storage -> t -> unit Lwt.t

(* val terminate : t -> unit  *)