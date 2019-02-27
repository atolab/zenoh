open Zenoh
open Cmdliner

let peers = Arg.(value & opt string "tcp/127.0.0.1:7447" & info ["p"; "peers"] ~docv:"PEERS" ~doc:"peers")

type state = {mutable starttime:float; mutable count:int;}

let state =  {starttime=0.0; count=0}

let listener bufs _ = 
    let n = List.length bufs in 
    let now = Unix.gettimeofday() in 
    Lwt.return @@ match state.starttime with 
    | 0.0 -> state.starttime <- now; state.count <- 1
    | time when now < (time +. 1.0) -> state.count <- state.count + n
    | _ -> 
      Printf.printf "%d\n%!" (state.count + 1)
      ; state.starttime <-now
      ; state.count <- 0 
    

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