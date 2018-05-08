
open Lwt
open Lwt.Infix
open Netbuf
open Marshaller
open Zenoh
open Transport
open Zengine
open Ztypes

let () = Lwt_log.add_rule "*" Lwt_log.Info

let pid =
  let bs = Lwt_bytes.create 4 in
  for i = 0 to 3 do
    Lwt_bytes.set bs i (Char.chr (3-i))
  done
  ; bs

let lease = 10000L
let version = Char.chr 0x01


let listen_address = Unix.inet_addr_any
let port = 7447
let backlog = 10
let max_buf_len = 64 * 1024
let tcp_tx_id = 0x01

let handle_message s msg =
  print_endline " >> handle_message" ;
  let _ = Lwt_log.info @@ "Received message: " ^ (Message.to_string msg) in None

let () =
  let locator = Unix.ADDR_INET(listen_address, port) in
  let tcp_locator = Locator.of_string "tcp/192.168.1.11:7447" in
  let engine = ProtocolEngine.create pid lease @@ Locators.singleton tcp_locator in
  let tx = Tcp.create tcp_tx_id locator (fun s msg -> ProtocolEngine.process engine s msg) max_buf_len in
  let _ = Lwt_log.info "Starting zenoh broker..." in
  let run_loop = Tcp.run_loop tx in
  Lwt_main.run @@ run_loop ()
