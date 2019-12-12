open Zenoh_net
open Apero.Infix
open Cmdliner

let peers = Arg.(value & opt string "tcp/127.0.0.1:7447" & info ["p"; "peers"] ~docv:"PEERS" ~doc:"peers")

let run peers = 
  Lwt_main.run 
  (
    let%lwt z = zopen peers in
    let%lwt pub = publish z "/roundtrip/pong" in
    let     listener _ = List.split %> fst %> lstream pub in
    let%lwt _ = subscribe z "/roundtrip/ping" listener in

    let rec infinitewait () = 
      let%lwt _ = Lwt_unix.sleep 1000.0 in 
      infinitewait () in
    infinitewait ()
  )

let () = 
  Printexc.record_backtrace true;
  Lwt_engine.set (new Lwt_engine.libev ()) ;
  let _ = Term.(eval (const run $ peers, Term.info "roundtrip_pong")) in  ()