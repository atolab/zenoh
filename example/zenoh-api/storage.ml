open Zenoh
open Apero

module StrMap = Map.Make(String)

let storagevar = Lwt_mvar.create (StrMap.empty)

let peer = match Array.length Sys.argv with 
  | 1 -> "tcp/127.0.0.1:7447"
  | _ -> Sys.argv.(1)

let listener spath data src = 
  let (str, _) = Result.get @@ decode_string data in
  Printf.printf "STORAGE LISTENER [%-8s] RECIEVED RESOURCE [%-20s] : %s\n%!" spath src str;
  let%lwt storage = Lwt_mvar.take storagevar in
  let storage = StrMap.add src data storage in
  let%lwt _ = Lwt_mvar.put storagevar storage in
  Lwt.return_unit

let qhandler spath resname predicate = 
  Printf.printf "STORAGE QHANDLER [%-8s] RECIEVED QUERY : %s?%s\n%!" spath resname predicate;
  let%lwt storage = Lwt_mvar.take storagevar in
  let%lwt _ = Lwt_mvar.put storagevar storage in
  storage
  |> StrMap.filter (fun storesname _ -> PathExpr.intersect (PathExpr.of_string storesname) (PathExpr.of_string resname)) 
  |> StrMap.bindings 
  |> Lwt.return

let run = 
  let%lwt z = zopen peer in 
  let%lwt _ = storage "/home*/**" (listener "/home*/**") (qhandler "/home*/**") z in 
  let%lwt _ = Lwt_unix.sleep 3000.0 in 
  Lwt.return_unit

let () = 
  Lwt_main.run @@ run