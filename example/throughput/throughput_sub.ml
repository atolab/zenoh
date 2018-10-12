open Zenoh
open Apero
open Cmdliner

let peers = Arg.(value & opt string "tcp/127.0.0.1:7447" & info ["p"; "peers"] ~docv:"PEERS" ~doc:"peers")

type state = {starttime:float; count:int;}

let state = MVar_lwt.create {starttime=0.0; count=0}

let listener _ _ = 
  Lwt.ignore_result @@ (
    let%lwt s = MVar_lwt.take state in
    let now = Unix.gettimeofday() in 
    let s = match s.starttime with 
    | 0.0 -> {starttime = now; count = 1}
    | time when now < (time +. 1.0) -> {s with count = s.count + 1;}
    | _ -> Printf.printf "%d\n%!" (s.count + 1);{starttime=now; count=0} in
    MVar_lwt.put state s);
  Lwt.return_unit

let run peers = 
  Lwt_main.run 
  (
    let%lwt z = zopen peers in 
    let%lwt _ = subscribe "/home1" listener z in 
    let%lwt _ = Lwt_unix.sleep 3000.0 in 
    Lwt.return_unit
  )

let () = 
  let _ = Term.(eval (const run $ peers, Term.info "throughput_sub")) in  ()