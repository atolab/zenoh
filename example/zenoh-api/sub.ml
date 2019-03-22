open Zenoh
open Apero
open Apero.Infix

let peer = match Array.length Sys.argv with 
  | 1 -> "tcp/127.0.0.1:7447"
  | _ -> Sys.argv.(1)

let listener sub src =
  List.iter (fst %> decode_string %> Printf.printf "LISTENER [%-8s] RECIEVED RESOURCE [%-20s] : %s\n%!" sub src) 
  %> Lwt.return

let run = 
  let%lwt z = zopen peer in 
  let%lwt _ = subscribe z "/*" (listener "/*") in 
  let%lwt _ = subscribe z "/home*" (listener "/home*") in 
  let%lwt _ = Lwt_unix.sleep 3000.0 in 
  Lwt.return_unit

let () = 
  Lwt_main.run @@ run