open Apero
open Zenoh_types
open Zenoh_storages_storage
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

let just_loaded = ref None
let register_backend_factory bf = just_loaded := Some bf
let get_loaded_backend_factory () : (module BackendFactory) =
  match !just_loaded with
  | Some bf -> bf
  | None -> failwith "No Backend Factory loded"
