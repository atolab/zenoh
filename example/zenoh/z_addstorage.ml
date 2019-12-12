open Apero
open Zenoh_ocaml
open Zenoh


let _ = 
  let argv = Sys.argv in
  Lwt_main.run 
  (
    let locator = if (Array.length argv > 1) then Array.get argv 1 else "tcp/127.0.0.1:7447" in
    let selector = if (Array.length argv > 2) then Array.get argv 2 else "/demo/example/**" in
    let storage_id = if (Array.length argv > 3) then Array.get argv 3 else "Demo" in

    let%lwt () = Lwt_io.printf "Login to %s\n" locator in
    let%lwt y = Zenoh.login locator Properties.empty in

    let%lwt a = Zenoh.admin y in

    let%lwt () = Lwt_io.printf "Add storage %s with selector %s\n" storage_id selector in
    let properties = Properties.singleton "selector" selector in
    let%lwt () = Admin.add_storage storage_id properties a in

    Zenoh.logout y
  )
