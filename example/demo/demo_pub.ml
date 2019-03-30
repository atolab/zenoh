open Zenoh
open Cmdliner
open Lwt.Infix

let peers = Arg.(value & opt string "tcp/127.0.0.1:7447" & info ["p"; "peers"] ~docv:"PEERS" ~doc:"peers")
let res = Arg.(value & opt string "/demo/default" & info ["r"; "resource"] ~docv:"RESOURCE" ~doc:"resource")
let value = Arg.(value & opt string "DEFAULT" & info ["v"; "value"] ~docv:"VALUE" ~doc:"value")

let run peers res value = 
  Lwt_main.run 
  (
    let%lwt z = zopen peers in 
    let%lwt pub = publish z res in 
    let buf = Abuf.create 1024 in
    Apero.encode_string res buf;
    let rec loop () = 
      stream pub buf 
      >>= fun () -> Printf.printf "SEND %s\n%!" value |> Lwt.return
      >>= fun () -> Lwt_unix.sleep 0.02 
      >>= loop in 
    loop ()
  )

let () = 
  let _ = Term.(eval (const run $ peers $ res $ value, Term.info "demo_pub")) in  ()