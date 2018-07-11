
open Lwt
open Lwt.Infix
open Zenoh
open Apero 
open Proto_engine
open Cmdliner

let pid  = IOBuf.flip @@ Result.get @@ IOBuf.put_string "zenohd" (IOBuf.create 16) 

let lease = 0L
let version = Char.chr 0x01


let listen_address = Unix.inet_addr_any
let port = 7447
let backlog = 10
let max_buf_len = 64 * 1024
let tcp_tx_id = 0x01


let setup_log style_renderer level =
  Fmt_tty.setup_std_outputs ?style_renderer ();
  Logs.set_level level;
  Logs.set_reporter (Logs_fmt.reporter ());
  ()

let tcpport = Arg.(value & opt int 7447 & info ["t"; "tcpport"] ~docv:"TCPPORT" ~doc:"listening port")
let peers = Arg.(value & opt string "" & info ["p"; "peers"] ~docv:"PEERS" ~doc:"peers")

let to_string peers = 
  peers
  |> List.map (fun p -> Locator.to_string p) 
  |> String.concat "," 

let run_broker tcpport peers = 
  let peers = String.split_on_char ',' peers 
  |> List.filter (fun s -> not (String.equal s ""))
  |> List.map (fun s -> Option.get @@ Locator.of_string s) in
  let%lwt _ = Logs_lwt.debug (fun m -> m "tcpport : %d" tcpport) in
  let%lwt _ = Logs_lwt.debug (fun m -> m "peers : %s" (to_string peers)) in
  let locator = Option.get @@ Iplocator.TcpLocator.of_string (Printf.sprintf "tcp/0.0.0.0:%d" tcpport);  in
  let%lwt tx = makeTcpTransport [locator] in  
  let module TxTcp = (val tx : Transport.S) in   
  let e = ProtocolEngine.create pid lease (Locators.of_list [Locator.TcpLocator locator]) peers in
  let _ = TxTcp.start (ProtocolEngine.event_push e) in
  ProtocolEngine.start e tx

let run tcpport peers style_renderer level = 
  setup_log style_renderer level; 
  Lwt_main.run @@ run_broker tcpport peers
   
let () =
  let env = Arg.env_var "ZENOD_VERBOSITY" in
  let _ = Term.(eval (const run $ tcpport $ peers $ Fmt_cli.style_renderer () $ Logs_cli.level ~env (), Term.info "broker")) in  ()
  
