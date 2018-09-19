open Zenoh
open Apero
open Cmdliner

let peers = Arg.(value & opt string "tcp/127.0.0.1:7447" & info ["p"; "peers"] ~docv:"PEERS" ~doc:"peers")
let nb = Arg.(value & opt int 10000 & info ["n"; "nb"] ~docv:"NB" ~doc:"nb roundtrip iteration")
let size = Arg.(value & opt int 8 & info ["s"; "size"] ~docv:"SIZE" ~doc:"payload size")

type state = {wr_time:float; count:int; rt_times:float list}
let state = MVar_lwt.create_empty ()

let (promise, resolver) = Lwt.task ()

let run peers nb size = 
  Lwt_main.run 
  (
    let%lwt _ = MVar_lwt.put state {wr_time=Unix.gettimeofday(); count=0; rt_times=[];} in

    let%lwt z = zopen peers in
    let%lwt pub = publish "/roundtrip/ping" z in

    let listener _ _ state = 
      let now = Unix.gettimeofday() in 
      (match state.count + 1 < nb with 
      | true -> Lwt.ignore_result @@ stream (IOBuf.create size) pub
      | false -> Lwt.wakeup_later resolver ());
      Lwt.return (Lwt.return_unit, {wr_time=now; count=state.count + 1; rt_times=(now -. state.wr_time) :: state.rt_times}) in

    let%lwt _ = subscribe "/roundtrip/pong" (fun d s -> MVar_lwt.guarded state (listener d s)) z in

    Lwt.ignore_result @@ stream (IOBuf.create size) pub;

    let%lwt _ = promise in
    let%lwt state = MVar_lwt.take state in
    state.rt_times |> List.rev |> List.iter (fun time -> Printf.printf "%i\n" (int_of_float (time *. 1000000.0))); 
    Printf.printf "%!";
    Lwt.return_unit
  )

let () = 
  Printexc.record_backtrace true;
  let _ = Term.(eval (const run $ peers $ nb $ size, Term.info "roundtrip_ping")) in  ()
