open Zenoh
open Apero

let peer = match Array.length Sys.argv with 
  | 1 -> "tcp/127.0.0.1:7447"
  | _ -> Sys.argv.(1)


let qhandler spath resname predicate = 
  Printf.printf "EVAL QHANDLER [%-8s] RECIEVED QUERY : %s?%s\n%!" spath resname predicate;
  let buf = Abuf.create 1024 in
  encode_string "EVAL1_RESULT" buf;
  Lwt.return [(spath, buf, Ztypes.empty_data_info)]

let run = 
  let%lwt z = zopen peer in 
  let%lwt _ = evaluate z "/home1/eval1" (qhandler "/home1/eval1") in 
  let%lwt _ = Lwt_unix.sleep 3000.0 in 
  Lwt.return_unit

let () = 
  Lwt_main.run @@ run