open Zenoh
open Apero


let uri_match uri1 uri2 =
let pattern_match uri pattern = 
    let expr = Str.regexp (pattern 
    |> Str.global_replace (Str.regexp "\\.") "\\."
    |> Str.global_replace (Str.regexp "\\*\\*") ".*"
    |> Str.global_replace (Str.regexp "\\([^\\.]\\)\\*") "\\1[^/]*"
    |> Str.global_replace (Str.regexp "^\\*") "[^/]*"
    |> Str.global_replace (Str.regexp "\\\\\\.\\*") "\\.[^/]*") in
    (Str.string_match expr uri 0) && (Str.match_end() = String.length uri) in
(pattern_match uri1 uri2) || (pattern_match uri2 uri1)

module StrMap = Map.Make(String)

let storagevar = Lwt_mvar.create (StrMap.empty)

let listener sto data src = 
  let (str, _) = Result.get @@ decode_string data in
  Printf.printf "STORAGE LISTENER [%-8s] RECIEVED DATA FROM [%-8s] : %s\n%!" sto src str;
  let%lwt storage = Lwt_mvar.take storagevar in
  let storage = StrMap.add src data storage in
  let%lwt _ = Lwt_mvar.put storagevar storage in
  Lwt.return_unit

let qhandler sto resname predicate = 
  Printf.printf "STORAGE HANDLER [%-8s] RECIEVED QUERY : %s?%s\n%!" sto resname predicate;
  let%lwt storage = Lwt_mvar.take storagevar in
  storage
  |> StrMap.filter (fun storesname _ -> uri_match storesname resname) 
  |> StrMap.bindings 
  |> Lwt.return

let run = 
  let%lwt z = zopen "tcp/127.0.0.1:7447" in 
  let%lwt _ = store "/home**" (listener "/home**") (qhandler "/home**") z in 
  let%lwt _ = Lwt_unix.sleep 3000.0 in 
  Lwt.return_unit

let () = 
  Lwt_main.run @@ run