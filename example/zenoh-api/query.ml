open Zenoh
open Apero
open Lwt.Infix

let peer = match Array.length Sys.argv with 
  | 1 -> "tcp/127.0.0.1:7447"
  | _ -> Sys.argv.(1)

let handler = function
  | StorageData rep ->
    let str = decode_string rep.data in
    Printf.printf "  QUERY HANDLER RECIEVED FROM STORAGE [%-16s:%02i] RESOURCE [%-20s] : %s\n%!" (Abuf.hexdump rep.stoid) rep.rsn rep.resname str;
    Lwt.return_unit
  | StorageFinal rep -> 
    Printf.printf "  QUERY HANDLER RECIEVED FROM STORAGE [%-16s:%02i] FINAL\n%!" (Abuf.hexdump rep.stoid) rep.rsn;
    Lwt.return_unit
  | ReplyFinal -> 
    Printf.printf "  QUERY HANDLER RECIEVED GLOBAL FINAL\n%!";
    Lwt.return_unit

let run = 
  let%lwt z = zopen peer in    
  Printf.printf "QUERY /home1/** :\n%!";
  let%lwt _ = query z "/home1/**" "" handler in 

  let%lwt _ = Lwt_unix.sleep 0.2 in

  Printf.printf "\n%!";
  Printf.printf "LQUERY /home1/** :\n%!";
  let%lwt _ = lquery z "/home1/**" "" >|= List.iter (fun (k,v,_) -> Printf.printf "  RECEIVED RESOURCE [%-20s] : %s\n%!" k (decode_string v)) in

  Lwt_unix.sleep 1.0


let () = 
  Lwt_main.run @@ run