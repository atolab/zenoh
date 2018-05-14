
open Lwt
open Lwt.Infix
open Zenoh

(* let () =
    (Lwt_log.append_rule "*" Lwt_log.Debug) *)

let pid = let open Result in
  get (do_
      ; buf <-- IOBuf.create 16
      ; buf <-- IOBuf.put_string buf "zenohd"
      ; IOBuf.flip buf)


let lease = 0L
let version = Char.chr 0x01


let listen_address = Unix.inet_addr_any
let port = 7447
let backlog = 10
let max_buf_len = 64 * 1024
let tcp_tx_id = 0x01

let handle_message s msg =
  print_endline " >> handle_message" ;
  let _ = Lwt_log.debug @@ "Received message: " ^ (Message.to_string msg) in None

let () =
  let locator = Unix.ADDR_INET(listen_address, port) in
  let tcp_locator = Locator.of_string "tcp/192.168.1.11:7447" in
  let engine = ProtocolEngine.create pid lease @@ Locators.singleton tcp_locator in
  let tx =
    Tcp.create tcp_tx_id locator
      (fun s msg -> ProtocolEngine.process engine s msg)
      (fun s -> ProtocolEngine.remove_session engine s)
      max_buf_len in
  let _ = Lwt_log.debug "Starting zenoh broker..." in
  let run_loop = Tcp.run_loop tx in
  Lwt_main.run @@ run_loop ()
