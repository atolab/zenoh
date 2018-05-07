
open Lwt
open Lwt.Infix
open Netbuf
open Marshaller
open Zenoh
open Transport

let () = Lwt_log.add_rule "*" Lwt_log.Info


let listen_address = Unix.inet_addr_any
let port = 7447
let backlog = 10
let max_buf_len = 64 * 1024
let tcp_tx_id = 0x01

let handle_message sid msg = Lwt_log.info @@ "Received message: " ^ (Message.to_string msg)

let () =
  let locator = Unix.ADDR_INET(listen_address, port) in
  let rbuf = IOBuf.(Result.get @@ create max_buf_len) in
  let wbuf = IOBuf.(Result.get @@ create max_buf_len) in
  let tx = Tcp.create tcp_tx_id locator rbuf wbuf handle_message in
  let _ = Lwt_log.info "Starting zenoh broker..." in
  let run_loop = Tcp.run_loop tx in
  Lwt_main.run @@ run_loop ()

(*
let () = Lwt_log.add_rule "*" Lwt_log.Info

let get_conf () =
  let open Zconfig_t in
  let txconf = [`TCPConfig {port = 7447; connection_backlog = 32}] in
  {transports = txconf; log_level = `INFO}

let get_port conf = 7447

let get_backlog conf = 32

let string_of_sockaddr = function
  | Lwt_unix.ADDR_UNIX s -> s
  | Lwt_unix.ADDR_INET (ip, port) -> (Unix.string_of_inet_addr ip) ^ (string_of_int port)

let get_message_length sock =
  let buf = Lwt_bytes.create 16 in
  let rec extract_length buf v bc =
    (Lwt_bytes.recv sock buf 0 1 []) >>=
    (fun n ->
       if n != 1 then (Lwt.return 0)
       else
        begin
          let c = int_of_char @@ (Lwt_bytes.get buf 0) in
          if c <= 0x7f then Lwt.return (v lor (c lsl (bc * 7)))
          else extract_length buf (v lor ((c land 0x7f) lsl bc)) (bc + 1)
        end
    ) in extract_length buf 0 0


let handle_connection sock addr  =
  let buf = Lwt_bytes.create max_buf_len in
  let rec serve_connection () =
    get_message_length sock >>= (fun len ->
        if len <= 0 then Lwt_unix.close sock
        else
          let _ = Lwt_log.info @@ "Received " ^ (string_of_int len) ^ " bytes"  in
          Lwt_bytes.recv sock buf 0 len [] >>= (fun rb ->
              let msg = IOBuf.Result.do_
              ; iobuf <-- IOBuf.from_bytes buf
              ; iobuf <-- IOBuf.set_limit iobuf len
              ; read_msg iobuf
              in match msg with
              | Ok (m, buf) -> Lwt_log.info ("Received Message " ^ (Zenoh.Message.to_string m))  >>= (fun _ -> serve_connection ())
              | _ -> Lwt_log.error "Received Invalid Message" >>= (fun _ -> Lwt_unix.close sock)

              )
      )
  in serve_connection ()


let accept_connection conn =
  let fd, addr = conn in
  let _ = Lwt_log.info ("Incoming connection from: " ^ (string_of_sockaddr addr)) in
  let _ = Lwt.on_failure (handle_connection fd addr)  (fun e -> Lwt_log.ign_error (Printexc.to_string e)) in
  Lwt_log.info "New connection" >>= return

let create_socket () =
  let open Lwt_unix in
  let sock = socket PF_INET SOCK_STREAM 0 in
  let _ = setsockopt sock SO_REUSEADDR true in
  let _ = setsockopt sock TCP_NODELAY true in
  let _ = bind sock @@ ADDR_INET(listen_address, port) in
  listen sock backlog;
  sock

let create_server sock =
  let rec serve () =
    Lwt_unix.accept sock >>= accept_connection >>= serve
  in serve

let () =
  let sock = create_socket () in
  let serve = create_server sock in
  Lwt_main.run @@ serve () *)
