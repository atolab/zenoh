open Cmdliner

let zopen argv = 
  let run tcpport peers strength bufn = 
    Zengine.run tcpport peers strength bufn |> Lwt.ignore_result; 
    Zenoh.zopen ("tcp/127.0.0.1:" ^ (string_of_int tcpport))
  in 
  
  Term.(eval (const run $ Zengine.tcpport $ Zengine.peers $ Zengine.strength $ Zengine.bufn, Term.info "zenohd") ~argv) |> function
  | `Ok result -> result
  | _ -> Lwt.fail_with "Invalid arguments"
  