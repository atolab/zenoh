type sub
type pub
type storage
type sublistener = Abuf.t -> string -> unit Lwt.t
type queryreply = 
  | StorageData of {stoid:Abuf.t; rsn:int; resname:string; data:Abuf.t}
  | StorageFinal of {stoid:Abuf.t; rsn:int}
  | ReplyFinal 
type reply_handler = queryreply -> unit Lwt.t
type query_handler = string -> string -> (string * Abuf.t) list Lwt.t
type submode
type t

val zopen : string -> t Lwt.t

val info : t -> Apero.properties

val publish : string -> t -> pub Lwt.t

val unpublish : pub -> t -> unit Lwt.t

val write : Abuf.t -> string -> t -> unit Lwt.t

val stream : Abuf.t -> pub -> unit Lwt.t

val push_mode : submode

val pull_mode : submode

val subscribe : string -> sublistener -> ?mode:submode -> t -> sub Lwt.t

val pull : sub -> unit Lwt.t

val unsubscribe : sub -> t -> unit Lwt.t

val store : string -> sublistener -> query_handler -> t -> storage Lwt.t

val query : string -> string -> reply_handler -> ?dest:Queries.dest -> t -> unit Lwt.t

val squery : string -> string -> ?dest:Queries.dest -> t -> queryreply Lwt_stream.t
(* [lquery] returns a stream that will allow to asynchronously iterate through the 
replies of the query *)

val lquery : string -> string -> ?dest:Queries.dest -> t -> (string * Abuf.t) list Lwt.t
(* [lquery] consolidates the results of a query and returns them into a list of key values *)

val unstore : storage -> t -> unit Lwt.t
(* val terminate : t -> unit  *)