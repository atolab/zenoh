open Zenoh
open Apero

module StrMap = Map.Make(String)

let storagevar = Lwt_mvar.create (StrMap.empty)

let peer = match Array.length Sys.argv with 
  | 1 -> "tcp/127.0.0.1:7447"
  | _ -> Sys.argv.(1)

let listener spath (bufs: Abuf.t list) src = 
  bufs 
  |> List.map (fun b -> decode_string b)
  |> List.iter (fun s -> Printf.printf "STORAGE LISTENER [%-8s] RECIEVED RESOURCE [%-20s] : %s\n%!" spath src s);
  let%lwt storage = Lwt_mvar.take storagevar in
  let storage = List.fold_left (fun storage data -> StrMap.add src data storage) storage bufs in  
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
  let%lwt _ = store "/home*/**" (listener "/home*/**") (qhandler "/home*/**") z in 
  let%lwt _ = Lwt_unix.sleep 3000.0 in 
  Lwt.return_unit

let () = 
  Lwt_main.run @@ run