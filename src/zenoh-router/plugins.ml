module type Plugin =
  sig
    val run: Zenoh.t -> string array -> unit Lwt.t
  end

let plugin = ref None
let register_plugin p = plugin := Some p
let get_plugin () : (module  Plugin)  =
  match !plugin with 
  | Some s -> s
  | None -> failwith "No plugin loaded"