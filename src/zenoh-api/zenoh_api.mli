open Iobuf

type sub
type pub
type listener = IOBuf.t -> string -> unit Lwt.t
type t

val zopen : string -> t Lwt.t

val publish : string -> t -> pub Lwt.t

(* val unpublish : pub -> t -> unit *)

(* val write : bytes -> string -> t -> unit *)

val stream : IOBuf.t -> pub -> unit Lwt.t

val subscribe : string -> listener -> t -> sub Lwt.t

(* val unsubscriber : sub -> t -> unit *)

(* val terminate : t -> unit  *)