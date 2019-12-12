open Apero
open Zenoh_ocaml
open Zenoh
open Zenoh.Infix


let _ = 
  let argv = Sys.argv in
  Lwt_main.run 
  (
    let locator = if (Array.length argv > 1) then Array.get argv 1 else "tcp/127.0.0.1:7447" in
    (* If not specified as 2nd argument, use a relative path (to the workspace below): "zenoh-ocaml-put" *)
    let path = if (Array.length argv > 2) then Array.get argv 2 else "zenoh-ocaml-put" in
    let value = if (Array.length argv > 3) then Array.get argv 3 else "Put from zenoh OCaml!" in

    let%lwt () = Lwt_io.printf "Login to %s\n" locator in
    let%lwt y = Zenoh.login locator Properties.empty in

    let%lwt () = Lwt_io.printf "Use Workspace on '/demo/example'\n" in
    let%lwt w = Zenoh.workspace ~//"/demo/example" y in

    let%lwt () = Lwt_io.printf "Put on %s : %s\n" path value in
    let%lwt () = Workspace.put ~//path ~$value w in

    Zenoh.logout y
  )
