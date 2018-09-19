open Zenoh
open Common.Result.Infix
open Apero

let run = 
  let%lwt z = zopen "tcp/127.0.0.1:7447" in 
  let%lwt home1pub = publish "/home1" z in 
  let%lwt res1pub = publish "/res1" z in 
  let buf = Result.get (encode_string "HOME1_MSG1" (IOBuf.clear (IOBuf.create 1024)) >>> IOBuf.flip) in
  let%lwt _ = stream buf home1pub in
  let buf = Result.get (encode_string "RES1_MSG1" (IOBuf.clear (IOBuf.create 1024)) >>> IOBuf.flip) in
  let%lwt _ = stream buf res1pub in
  let%lwt _ = Lwt_unix.sleep 1.0 in
  Lwt.return_unit


let () = 
  Lwt_main.run @@ run