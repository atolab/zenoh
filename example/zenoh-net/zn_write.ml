open Zenoh_net

let locator = match Array.length Sys.argv with 
  | 1 -> "tcp/127.0.0.1:7447"
  | _ -> Sys.argv.(1)

let uri = match Array.length Sys.argv with 
  | 1 | 2 -> "/demo/example/zenoh-net-ocaml-write"
  | _ -> Sys.argv.(2)

let value = match Array.length Sys.argv with 
  | 1 | 2 | 3 -> "Write from OCaml!"
  | _ -> Sys.argv.(3)

let run =
  Printf.printf "Connecting to %s...\n%!" locator;
  let%lwt z = zopen locator in 

  Printf.printf "Writing Data ('%s': '%s')...\n%!" uri value;
  let%lwt _ = write z uri (Abuf.from_bytes @@ Bytes.of_string value) in
  
  zclose z


let () = 
  Lwt_main.run @@ run
