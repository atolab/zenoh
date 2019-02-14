open Zenoh
open Apero
open Lwt.Infix

let peer = match Array.length Sys.argv with 
  | 1 -> "tcp/127.0.0.1:7447"
  | _ -> Sys.argv.(1)

let handler = function
  | StorageData rep ->
    let (str, _) = Result.get @@ decode_string rep.data in
    Printf.printf "QUERY HANDLER RECIEVED FROM STORAGE [%-16s:%02i] RESOURCE [%-20s] : %s\n%!" (IOBuf.hexdump rep.stoid) rep.rsn rep.resname str;
    Lwt.return_unit
  | StorageFinal rep -> 
    Printf.printf "QUERY HANDLER RECIEVED FROM STORAGE [%-16s:%02i] FINAL\n%!" (IOBuf.hexdump rep.stoid) rep.rsn;
    Lwt.return_unit
  | ReplyFinal -> 
    Printf.printf "QUERY HANDLER RECIEVED GLOBAL FINAL\n%!";
    Lwt.return_unit

let run = 
  let%lwt z = zopen peer in    
  let%lwt _ = query "/home1/**" "" handler z in 
  let%lwt _ = lquery "/home1/**" "" z >|= List.iter (fun (k,_) -> Printf.printf "%s\n" k) in
  Lwt_unix.sleep 1.0


let () = 
  Lwt_main.run @@ run