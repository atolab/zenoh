open Iobuf

type sub
type pub
type listener = IOBuf.t -> string -> unit Lwt.t
type t

val zopen : string -> t Lwt.t

val publish : string -> t -> pub Lwt.t

val unpublish : pub -> t -> unit Lwt.t

val write : IOBuf.t -> string -> t -> unit Lwt.t

val stream : IOBuf.t -> pub -> unit Lwt.t

val subscribe : string -> listener -> t -> sub Lwt.t

val unsubscribe : sub -> t -> unit Lwt.t

(* val terminate : t -> unit  *)