open Zenoh_api
open Common
open Apero


let listener sub data src = 
  let (str, _) = Result.get @@ decode_string data in
  Printf.printf "LISTENER [%-8s] RECIEVED FROM [%-8s] : %s\n%!" sub src str;
  Lwt.return_unit
  

let run peer = 
  let%lwt z = zopen "tcp/127.0.0.1:7447" in 
  let%lwt _ = subscribe "/*" (listener "/*") z in 
  let%lwt _ = subscribe "/home*" (listener "/home*") z in 
  let%lwt _ = Lwt_unix.sleep 3000.0 in 
  Lwt.return_unit


let () = 
  Lwt_main.run @@ run "tcp/127.0.0.1:7447"