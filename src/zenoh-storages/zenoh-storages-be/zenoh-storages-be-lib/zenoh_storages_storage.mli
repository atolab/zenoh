open Apero
open Zenoh_types

module Storage : sig 

  module Id : (module type of Apero.Uuid)

  type t 

  val make : Selector.t -> properties ->
    (unit -> unit Lwt.t) ->
    (Selector.t -> (Path.t * TimedValue.t) list Lwt.t) ->
    (Path.t -> TimedValue.t -> unit Lwt.t) ->
    (Path.t -> TimedValue.t -> unit Lwt.t) ->
    (Path.t -> Timestamp.t -> unit Lwt.t) -> t

  val dispose : t -> unit Lwt.t

  val id : t -> Id.t
  val alias : t -> string option
  val selector : t -> Selector.t
  val properties : t -> properties

  val to_string : t -> string

  val covers_fully : t -> Selector.t -> bool
  (** [covers_fully s sel] tests if [s] fully covers the Selector [sel]
      (i.e. if each Path matching [sel] also matches the selector of [s]).
      Note: to test with a Path use { Selector.of_path } *)
  val covers_partially : t -> Selector.t -> bool
  (** [covers_partially s sel] tests if [s] partially covers the Selector [sel]
      (i.e. if it exists at least 1 Path matching [sel] that also matches the selector of [s]).
      Note: to test with a Path use { Selector.of_path } *)

  val get : t -> Selector.t -> (Path.t * TimedValue.t) list Lwt.t

  val put : t -> Path.t -> TimedValue.t -> unit Lwt.t
  val update : t -> Path.t -> TimedValue.t -> unit Lwt.t

  val remove : t -> Path.t -> Timestamp.t -> unit Lwt.t

  val on_zenoh_write : t -> Path.t -> change list -> unit Lwt.t
  val align : t -> Zenoh_net.t -> Selector.t -> unit Lwt.t

end  [@@deriving show]
