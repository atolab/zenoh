
open Lwt
open Lwt.Infix
open Zenoh
open Apero 
open Engine

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


(* Command line interface *)

open Cmdliner

let setup_log =
  let env = Arg.env_var "ZENOD_VERBOSITY" in
  Term.(const setup_log $ Fmt_cli.style_renderer () $ Logs_cli.level ~env ())

(* let msg =
  let doc = "Startup message."  in
  Arg.(value & pos 0 string "Starting the zenod broker!" & info [] ~doc)

let main () =
  match Term.(eval (const hello $ setup_log $ msg, Term.info "tool")) with
  | `Error _ -> exit 1
  | _ -> exit (if Logs.err_count () > 0 then 1 else 0) *)

let run_broker () =   
  let locator = OptionM.get @@ Iplocator.TcpLocator.of_string "tcp/0.0.0.0:7447" in   
  let%lwt tx = makeTcpTransport [locator] in  
  let module TxTcp = (val tx : Transport.S) in   
  let e = ProtocolEngine.create pid lease (Locators.of_list [Locator.TcpLocator locator]) in
  let%lwt (push, loop) = TxTcp.start (ProtocolEngine.event_push e) in  
  ProtocolEngine.attach_tx push e;    
  Lwt.join [loop; ProtocolEngine.start e]
 

let () =
  let _ = Term.(eval (setup_log, Term.info "tool")) in
  (* let locator = Unix.ADDR_INET(listen_address, port) in  
  let tcp_locator = OptionM.get @@ Locator.of_string "tcp/0.0.0.0:7447" in  
  let engine = ProtocolEngine.create pid lease @@ Locators.singleton tcp_locator in
  let tx =
    Tcp.create tcp_tx_id locator
      (fun s msg -> ProtocolEngine.process engine s msg)
      (fun s -> ProtocolEngine.remove_session engine s)
      max_buf_len in
  Logs.debug (fun m -> m "Starting zenoh broker...") ;
  let run_loop = Tcp.run_loop tx in *)
  Lwt_main.run @@ run_broker ()
