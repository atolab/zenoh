(*
 * Copyright (c) 2014, 2020 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 *
 * Contributors:
 * Angelo Corsaro, <angelo.corsaro@adlinktech.com>
 * Olivier Hecart, <olivier.hecart@adlinktech.com>
 * Julien Enoch, <julien.enoch@adlinktech.com>.
 *)
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
