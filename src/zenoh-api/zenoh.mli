open Ztypes

type sub
type pub
type storage
type sublistener = string -> (Abuf.t * data_info) list -> unit Lwt.t
type queryreply = 
  | StorageData of {stoid:Abuf.t; rsn:int; resname:string; data:Abuf.t; info:data_info}
  | StorageFinal of {stoid:Abuf.t; rsn:int}
  | ReplyFinal 
type reply_handler = queryreply -> unit Lwt.t
type query_handler = string -> string -> (string * Abuf.t * data_info) list Lwt.t
type submode
type t

val zopen : string -> t Lwt.t

val info : t -> Apero.properties

val publish : t -> string -> pub Lwt.t

val unpublish : t -> pub -> unit Lwt.t

val write : t -> string -> ?timestamp:Timestamp.t option -> ?kind:int64 option -> ?encoding:int64 option -> Abuf.t -> unit Lwt.t

val stream : pub -> ?timestamp:Timestamp.t option -> ?kind:int64 option -> ?encoding:int64 option -> Abuf.t -> unit Lwt.t

val lstream : pub -> Abuf.t list -> unit Lwt.t

val push_mode : submode

val pull_mode : submode

val subscribe : t -> ?mode:submode -> string -> sublistener -> sub Lwt.t

val pull : sub -> unit Lwt.t

val unsubscribe : t -> sub -> unit Lwt.t

val store : t -> string -> sublistener -> query_handler -> storage Lwt.t

val query : t -> ?dest:Ztypes.query_dest -> string -> string -> reply_handler -> unit Lwt.t

val squery : t -> ?dest:Ztypes.query_dest -> string -> string -> queryreply Lwt_stream.t
(* [lquery] returns a stream that will allow to asynchronously iterate through the 
replies of the query *)

val lquery : t -> ?dest:Ztypes.query_dest -> string -> string -> (string * Abuf.t * data_info) list Lwt.t
(* [lquery] consolidates the results of a query and returns them into a list of key values *)

val unstore : t -> storage -> unit Lwt.t
(* val terminate : t -> unit  *)