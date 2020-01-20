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

type on_dispose = Drop | Truncate | DoNothing

let on_dispose_from_properties props =
  match Properties.get Be_sql_properties.Key.on_dispose props with
  | Some text ->
    let t = String.uppercase_ascii text in
    if t = "DROP" then Drop
    else if t = "TRUNCATE" then Truncate
    else if t = "TRUNC" then Truncate
    else (Logs.err (fun m -> m "[SQL]: unsuppoerted property: %s=%s - ignore it" Be_sql_properties.Key.on_dispose text); DoNothing)
  | None -> DoNothing


module RemovalMap = Map.Make(String)

type storage_info =
  {
    selector : Selector.t
  ; keys_prefix : Path.t     (* prefix of the selector that is not included in the keys stored in the table *)
  ; keys_prefix_length : int
  ; props : properties
  ; connection: Caqti_driver.connection
  ; table_name : string
  ; schema : string list * Caqti_driver.Dyntype.t
  ; on_dispose : on_dispose
  ; removals : (Timestamp.t * unit Lwt.t) RemovalMap.t Guard.t
  }
