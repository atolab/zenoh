open Zenoh
open Cmdliner
open Lwt.Infix

let peers = Arg.(value & opt string "tcp/127.0.0.1:7447" & info ["p"; "peers"] ~docv:"PEERS" ~doc:"peers")
let size = Arg.(value & opt int 8 & info ["s"; "size"] ~docv:"SIZE" ~doc:"payload size")

let run peers size = 
  Lwt_main.run 
  (
    let%lwt z = zopen peers in 
    let%lwt home1pub = publish z "/home1" in 
    let buf = Abuf.create size in
    Abuf.set_w_pos size buf;
    let rec loop () = stream home1pub buf >>= loop in 
    loop ()
  )

let () = 
  let _ = Term.(eval (const run $ peers $ size, Term.info "throughput_pub")) in  ()