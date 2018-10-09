open Zenoh_proto
open Zenoh_tx_inet
open Apero 
open Apero_net
open Cmdliner

let locator = Apero.Option.get @@ TcpLocator.of_string "tcp/0.0.0.0:7447"
let listen_address = Unix.inet_addr_any
let port = 7447
let backlog = 10
let max_connections = 1000
let buf_size = 64 * 1024
let svc_id = 0x01

(* let config = ZTcpConfig.make ~backlog ~max_connections ~buf_size ~svc_id locator *)

let pid  = IOBuf.flip @@ 
  Result.get @@ IOBuf.put_string (Printf.sprintf "%08d" (Unix.getpid ())) @@
  Result.get @@ IOBuf.put_string hostid @@
  (IOBuf.create 16) 

let lease = 0L
let version = Char.chr 0x01




let reporter ppf =
  let report _ level ~over k msgf =
    let k _ = over (); k () in
    let with_stamp h _ k ppf fmt =
      Format.kfprintf k ppf ("[%f]%a @[" ^^ fmt ^^ "@]@.")
        (Unix.gettimeofday ()) Logs.pp_header (level, h)
    in
    msgf @@ fun ?header ?tags fmt -> with_stamp header tags k ppf fmt
  in
  { Logs.report = report }

let setup_log style_renderer level =
  Fmt_tty.setup_std_outputs ?style_renderer ();
  Logs.set_level level;
  Logs.set_reporter (reporter (Format.std_formatter));
  ()

let tcpport = Arg.(value & opt int 7447 & info ["t"; "tcpport"] ~docv:"TCPPORT" ~doc:"listening port")
let peers = Arg.(value & opt string "" & info ["p"; "peers"] ~docv:"PEERS" ~doc:"peers")
let strength = Arg.(value & opt int 0 & info ["s"; "strength"] ~docv:"STRENGTH" ~doc:"broker strength")

let to_string peers = 
  peers
  |> List.map (fun p -> Locator.to_string p) 
  |> String.concat "," 

module ZEngine = ZEngine(MVar_lwt)
let run_broker tcpport peers strength = 
  let open ZEngine in   
  let peers = String.split_on_char ',' peers 
  |> List.filter (fun s -> not (String.equal s ""))
  |> List.map (fun s -> Option.get @@ Locator.of_string s) in
  let%lwt _ = Logs_lwt.debug (fun m -> m "tcpport : %d" tcpport) in
  let%lwt _ = Logs_lwt.debug (fun m -> m "peers : %s" (to_string peers)) in
  let locator = Option.get @@ Iplocator.TcpLocator.of_string (Printf.sprintf "tcp/0.0.0.0:%d" tcpport);  in

  let config = ZTcpConfig.make ~backlog ~max_connections ~buf_size ~svc_id locator in 
  let tx = ZTcpTransport.make config in 
  let tx_connector = ZTcpTransport.establish_session tx in 
  let engine = ProtocolEngine.create pid lease Locators.empty peers strength tx_connector in     
  let dispatcher_svc sex  =     
    let rbuf = IOBuf.create buf_size in 
    let wbuf = IOBuf.create buf_size in
    let socket = (TxSession.socket sex) in
    let zreader = ztcp_read_frame socket in 
    let zwriter = ztcp_write_frame socket  in            
    let open Lwt.Infix in 
    fun () ->
      let rbuf = IOBuf.clear rbuf in              
      let wbuf = IOBuf.clear wbuf in                        
      zreader rbuf () 
      >>= fun frame ->
        match%lwt ProtocolEngine.handle_message engine sex (Frame.to_list frame)  with 
        | [] -> Lwt.return_unit
        | ms -> zwriter wbuf @@ (Frame.create ms) >>= fun _ -> Lwt.return_unit            
  in 
  Lwt.join [ZTcpTransport.start tx dispatcher_svc; ProtocolEngine.start engine]



let run tcpport peers strength style_renderer level = 
  setup_log style_renderer level; 
  Lwt_main.run @@ run_broker tcpport peers strength
   
let () =
  Printexc.record_backtrace true;
  let env = Arg.env_var "ZENOD_VERBOSITY" in
  let _ = Term.(eval (const run $ tcpport $ peers $ strength $ Fmt_cli.style_renderer () $ Logs_cli.level ~env (), Term.info "zenohd")) in  ()
  
