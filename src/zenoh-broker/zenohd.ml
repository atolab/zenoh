
open Lwt
open Lwt.Infix
open Zenoh
open Apero 
open Proto_engine
open Cmdliner

let pid  = IOBuf.flip @@ ResultM.get @@ IOBuf.put_string "zenohd" (IOBuf.create 16) 

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


let setup_log =
  let env = Arg.env_var "ZENOD_VERBOSITY" in
  Term.(const setup_log $ Fmt_cli.style_renderer () $ Logs_cli.level ~env ())

let run_broker () =   
  let locator = OptionM.get @@ Iplocator.TcpLocator.of_string "tcp/0.0.0.0:7447" in   
  let%lwt tx = makeTcpTransport [locator] in  
  let module TxTcp = (val tx : Transport.S) in   
  let e = ProtocolEngine.create pid lease (Locators.of_list [Locator.TcpLocator locator]) in
  let _ = ProtocolEngine.start e in
  TxTcp.start (ProtocolEngine.event_push e)   
  
 

let () =
  let _ = Term.(eval (setup_log, Term.info "tool")) in  
  Lwt_main.run @@ run_broker ()
