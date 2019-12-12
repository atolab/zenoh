open Lwt.Infix
open Apero
open Zenoh_ocaml
open Zenoh
open Zenoh.Infix


let _ = 
  let argv = Sys.argv in
  Lwt_main.run 
  (
    let locator = if (Array.length argv > 1) then Array.get argv 1 else "tcp/127.0.0.1:7447" in
    let selector = if (Array.length argv > 2) then Array.get argv 2 else "/demo/example/**" in

    let%lwt () = Lwt_io.printf "Login to %s\n" locator in
    let%lwt y = Zenoh.login locator Properties.empty in

    let%lwt () = Lwt_io.printf "Use Workspace on '/'\n" in
    let%lwt w = Zenoh.workspace ~//"/" y in


    let%lwt () = Lwt_io.printf "Get from %s\n" selector in
    let%lwt () = 
      Workspace.get ~/*selector w >>=
      Lwt_list.iter_p (fun (path, value) -> Lwt_io.printf "  %s : %s\n" (Path.to_string path) (Value.to_string value))
    in

    Zenoh.logout y
  )
