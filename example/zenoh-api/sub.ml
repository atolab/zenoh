open Zenoh
open Apero

let peer = match Array.length Sys.argv with 
  | 1 -> "tcp/127.0.0.1:7447"
  | _ -> Sys.argv.(1)

let listener sub data src = 
  let (str, _) = Result.get @@ decode_string data in
  Printf.printf "LISTENER [%-8s] RECIEVED RESOURCE [%-20s] : %s\n%!" sub src str;
  Lwt.return_unit

let run = 
  let%lwt z = zopen peer in 
  let%lwt _ = subscribe "/*" (listener "/*") z in 
  let%lwt _ = subscribe "/home*" (listener "/home*") z in 
  let%lwt _ = Lwt_unix.sleep 3000.0 in 
  Lwt.return_unit

let () = 
  Lwt_main.run @@ run