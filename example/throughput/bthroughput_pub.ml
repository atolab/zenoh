open Zenoh
open Cmdliner
open Lwt.Infix

let peers = Arg.(value & opt string "tcp/127.0.0.1:7447" & info ["p"; "peers"] ~docv:"PEERS" ~doc:"peers")
let size = Arg.(value & opt int 8 & info ["s"; "size"] ~docv:"SIZE" ~doc:"payload size")
let batch_size = 16 * 1024 

let create_batch n size =
  let rec rec_create_batch n bs =
    if n > 0 then 
      let buf = Abuf.create size in 
      Abuf.set_w_pos size buf; 
      rec_create_batch (n-1) (buf::bs)
    else 
      bs
  in 
    rec_create_batch n []

let run peers size = 
  let batch = batch_size / size in 
  let bufs = create_batch batch size in 

  Lwt_main.run 
  (
    let%lwt z = zopen peers in 
    let%lwt home1pub = publish "/home1" z in 
    let buf = Abuf.create size in
    Abuf.set_w_pos size buf;
    let rec loop () = lstream bufs home1pub >>= loop in 
    loop ()
  )

let () = 
  let _ = Term.(eval (const run $ peers $ size, Term.info "throughput_pub")) in  ()