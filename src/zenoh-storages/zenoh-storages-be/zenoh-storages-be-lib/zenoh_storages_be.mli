open Apero
open Zenoh_storages_storage
open Zenoh_types
open Zenoh_storages_core_types

module type Backend = sig
  val id : BeId.t
  val properties : properties
  val to_string : string

  val create_storage : Selector.t -> properties -> Storage.t Lwt.t
end

module type BackendFactory  = sig 
  val make : BeId.t -> properties  -> (module Backend) Lwt.t
end


val register_backend_factory : (module BackendFactory) -> unit
val get_loaded_backend_factory : unit -> (module BackendFactory)