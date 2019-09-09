open Zenoh
open Apero
open Cmdliner

let peers = Arg.(value & opt string "tcp/127.0.0.1:7447" & info ["p"; "peers"] ~docv:"PEERS" ~doc:"peers")
let res = Arg.(value & opt string "/demo/eval" & info ["r"; "resource"] ~docv:"RESOURCE" ~doc:"resource")

let qhandler spath resname predicate = 
  Printf.printf "EVAL QHANDLER [%-8s] RECIEVED QUERY : %s?%s\n%!" spath resname predicate;
  let buf = Abuf.create 1024 in
  encode_string "EVAL_RESULT" buf;
  Lwt.return [(spath, buf, Ztypes.empty_data_info)]

let run peers res = 
  Lwt_main.run 
  (
    let%lwt z = zopen peers in 
    let%lwt _ = evaluate z res (qhandler res) in 
    let%lwt _ = Lwt_unix.sleep 3000.0 in 
    Lwt.return_unit
  )

let () = 
  let _ = Term.(eval (const run $ peers $ res, Term.info "demo_eval")) in  ()