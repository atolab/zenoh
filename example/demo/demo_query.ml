open Zenoh
open Apero
open Cmdliner

let peers = Arg.(value & opt string "tcp/127.0.0.1:7447" & info ["p"; "peers"] ~docv:"PEERS" ~doc:"peers")
let res = Arg.(value & opt string "/demo/**" & info ["r"; "resource"] ~docv:"RESOURCE" ~doc:"resource")

let handler = function
  | StorageData rep ->
    let str = decode_string rep.data in
    Printf.printf "  QUERY HANDLER RECIEVED FROM STORAGE [%-16s:%02i] RESOURCE [%-20s] : %s\n%!" (Abuf.hexdump rep.stoid) rep.rsn rep.resname str;
    Lwt.return_unit
  | StorageFinal rep -> 
    Printf.printf "  QUERY HANDLER RECIEVED FROM STORAGE [%-16s:%02i] FINAL\n%!" (Abuf.hexdump rep.stoid) rep.rsn;
    Lwt.return_unit
  | EvalData rep ->
    let str = decode_string rep.data in
    Printf.printf "  QUERY HANDLER RECIEVED FROM EVAL    [%-16s:%02i] RESOURCE [%-20s] : %s\n%!" (Abuf.hexdump rep.stoid) rep.rsn rep.resname str;
    Lwt.return_unit
  | EvalFinal rep -> 
    Printf.printf "  QUERY HANDLER RECIEVED FROM EVAL    [%-16s:%02i] FINAL\n%!" (Abuf.hexdump rep.stoid) rep.rsn;
    Lwt.return_unit
  | ReplyFinal -> 
    Printf.printf "  QUERY HANDLER RECIEVED GLOBAL FINAL\n%!";
    Lwt.return_unit

let run peers res = 
  Lwt_main.run 
  (
    let%lwt z = zopen peers in    
    Printf.printf "QUERY %s :\n%!" res;
    let%lwt _ = query z res "" handler in 

    Lwt_unix.sleep 1.0
  )

let () = 
  let _ = Term.(eval (const run $ peers $ res, Term.info "demo_query")) in  ()