open Cmdliner

let zopen argv = 
  let run tcpport peers strength userfile bufn timestamp = 
    let (instream, inpush) = Lwt_stream.create_bounded 256 in
    let (outstream, outpush) = Lwt_stream.create_bounded 256 in
    Zrouter.run tcpport peers strength userfile bufn timestamp (Some (instream, outpush)) |> Lwt.ignore_result; 
    Zenoh.zropen (outstream, inpush)
  in 
  
  Term.(eval (const run $ Zrouter.tcpport $ Zrouter.peers $ Zrouter.strength $ Zrouter.users $ Zrouter.bufn $ Zrouter.timestamp, Term.info "zenohd") ~argv) |> function
  | `Ok result -> result
  | _ -> Lwt.fail_with "Invalid arguments"
  