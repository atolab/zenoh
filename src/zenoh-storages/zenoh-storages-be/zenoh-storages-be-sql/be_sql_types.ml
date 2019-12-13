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
