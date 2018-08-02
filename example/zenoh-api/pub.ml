open Zenoh_api
open Common
open Common.Result.Infix
open Apero

let run peer = 
  let%lwt z = zopen "tcp/127.0.0.1:7447" in 
  let%lwt pub = publish "/home1" z in 
  let buf = Result.get (encode_string "MSG" (IOBuf.clear (IOBuf.create 1024))
  >>> IOBuf.flip) in
  let%lwt _ = stream buf pub in
  let%lwt _ = Lwt_unix.sleep 1.0 in
  Lwt.return_unit


let () = 
  Lwt_main.run @@ run "tcp/127.0.0.1:7447"