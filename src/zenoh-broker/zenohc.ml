open Lwt
open Lwt.Infix
open Netbuf
open Marshaller
open Zenoh
open Ztypes

let () = Lwt_log.add_rule "*" Lwt_log.Info

let lbuf = IOBuf.(Result.get @@ create 16)
let buf = IOBuf.(Result.get @@ create 8192)

let get_args () =
  if Array.length Sys.argv < 3 then ("192.168.1.11", 7447)
  else (Array.get Sys.argv 1, int_of_string @@ Array.get Sys.argv 2)

let send_scout sock =
  let scout = Scout.create (Vle.of_int 1) [] in

  let _ = IOBuf.Result.do_
  ; buf <-- IOBuf.clear buf
  ; lbuf <-- IOBuf.clear lbuf
  ; buf <-- write_scout buf scout
  ; buf <-- IOBuf.flip buf
  ; lbuf <-- IOBuf.put_vle lbuf (Vle.of_int (IOBuf.get_limit buf))
  ; lbuf <-- IOBuf.flip lbuf
  ; () ; (let _ = Lwt_bytes.send sock (IOBuf.to_bytes lbuf) 0 (IOBuf.get_limit lbuf) [] in ())
  ; () ; (let _ = Lwt_bytes.send sock (IOBuf.to_bytes buf) 0 (IOBuf.get_limit buf) [] in return ())

  in return_unit

let send_message sock msg = match msg with
  | "scout" -> send_scout sock
  | _ -> Lwt_io.printf "Error: The message <%s> is unkown\n" msg >>= return

let rec run_loop sock =
  let _ = Lwt_io.printf ">> "  in
  (Lwt_io.read_line Lwt_io.stdin) >>= (fun msg -> send_message sock msg) >>= (fun _ -> run_loop sock)

let () =
  let addr, port  = get_args () in
  let open Lwt_unix in
  let sock = socket PF_INET SOCK_STREAM 0 in
  let saddr = ADDR_INET (Unix.inet_addr_of_string addr, port) in
  let _ = connect sock  saddr in
  Lwt_main.run @@ (run_loop sock)
