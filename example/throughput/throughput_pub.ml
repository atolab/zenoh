open Zenoh
open Apero
open Cmdliner
open Lwt.Infix

let peers = Arg.(value & opt string "tcp/127.0.0.1:7447" & info ["p"; "peers"] ~docv:"PEERS" ~doc:"peers")
let size = Arg.(value & opt int 8 & info ["s"; "size"] ~docv:"SIZE" ~doc:"payload size")

let run peers size = 
  Lwt_main.run 
  (
    let%lwt z = zopen peers in 
    let%lwt home1pub = publish "/home1" z in 
    let buf = MIOBuf.create size in
    let rec loop () = stream buf home1pub >>= loop in 
    loop ()
  )

let () = 
  let _ = Term.(eval (const run $ peers $ size, Term.info "throughput_pub")) in  ()