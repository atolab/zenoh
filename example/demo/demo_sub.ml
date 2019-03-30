open Apero.Infix
open Lwt.Infix
open Zenoh
open Cmdliner

let peers = Arg.(value & opt string "tcp/127.0.0.1:7447" & info ["p"; "peers"] ~docv:"PEERS" ~doc:"peers")
let res = Arg.(value & opt string "/demo/**" & info ["r"; "resource"] ~docv:"RESOURCE" ~doc:"resource")

let listener name = 
  List.iter (fun (buf, _) ->
    let value = Apero.decode_string buf in
    Printf.printf "RECEIVED %s %s\n%!" name value)
  %> Lwt.return

let run peers res = 
  Lwt_main.run 
  (
    let%lwt z = zopen peers in 
    let%lwt _ = subscribe z res listener in 
    let rec loop () = Lwt_unix.sleep 3000.0 >>= loop in 
    loop ()
  )

let () = 
  let _ = Term.(eval (const run $ peers $ res, Term.info "demo_sub")) in  ()