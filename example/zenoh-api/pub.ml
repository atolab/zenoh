open Zenoh
open Apero

let peer = match Array.length Sys.argv with 
  | 1 -> "tcp/127.0.0.1:7447"
  | _ -> Sys.argv.(1)

let run = 
  let%lwt z = zopen peer in 
  let%lwt home1pub = publish z "/home1" in 
  let%lwt res1pub = publish z "/res1" in 
  let buf = Abuf.create 1024 in
  encode_string "HOME1_MSG1" buf;
  let%lwt _ = stream home1pub buf in
  let buf = Abuf.create 1024 in
  encode_string "RES1_MSG1" buf;
  let%lwt _ = stream res1pub buf in
  let%lwt _ = Lwt_unix.sleep 1.0 in
  Lwt.return_unit


let () = 
  Lwt_main.run @@ run