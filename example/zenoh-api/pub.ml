open Zenoh
open Apero

let peer = match Array.length Sys.argv with 
  | 1 -> "tcp/127.0.0.1:7447"
  | _ -> Sys.argv.(1)

let run = 
  let%lwt z = zopen peer in 
  let%lwt home1pub = publish "/home1" z in 
  let%lwt res1pub = publish "/res1" z in 
  let buf = Abuf.create 1024 in
  encode_string "HOME1_MSG1" buf;
  let%lwt _ = stream buf home1pub in
  let buf = Abuf.create 1024 in
  encode_string "RES1_MSG1" buf;
  let%lwt _ = stream buf res1pub in
  let%lwt _ = Lwt_unix.sleep 1.0 in
  Lwt.return_unit


let () = 
  Lwt_main.run @@ run