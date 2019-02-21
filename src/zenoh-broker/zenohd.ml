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
let buf_size = 256 * 1024
let svc_id = 0x01

let pid  = 
  Abuf.create_bigstring 32 |> fun buf -> 
  Abuf.write_bytes (Bytes.unsafe_of_string ((Uuidm.to_bytes @@ Uuidm.v5 (Uuidm.create `V4) (string_of_int @@ Unix.getpid ())))) buf; 
  buf

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
let bufn = Arg.(value & opt int 8 & info ["w"; "wbufn"] ~docv:"BUFN" ~doc:"number of write buffers")

let to_string peers = 
  peers
  |> List.map (fun p -> Locator.to_string p) 
  |> String.concat "," 

module ZEngine = Zengine.ZEngine(MVar_lwt)
let run_broker tcpport peers strength bufn = 
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
  let engine = ProtocolEngine.create ~bufn pid lease Locators.empty peers strength tx_connector in     
  let dispatcher_svc sex  =     
    let rbuf = Abuf.create ~grow:4096 buf_size in 
    let wbuf = Abuf.create ~grow:4096 buf_size in
    let socket = (TxSession.socket sex) in
    (* let zreader = ztcp_read_frame socket in  *)
    Abuf.clear rbuf;
    let zwriter = ztcp_write_frame socket  in 
    let open Lwt.Infix in 
    fun () ->      
      match%lwt Net.read socket rbuf (Abuf.writable_bytes rbuf) with
      | 0 -> Lwt.fail_invalid_arg "peer closed connection" 
      | _->         
        let rec decode_msgs rbuf ms =           
          let rpos = Abuf.r_pos rbuf in 
          try                    
            let mlen = Int64.to_int @@ Apero.fast_decode_vle rbuf in                             
            let available = Abuf.readable_bytes rbuf in            
            
            if available > mlen then 
              begin            
                try 
                  let m = decode_msg rbuf in 
                  decode_msgs rbuf (m::ms)
                with
                | _ -> 
                  let _ = Logs_lwt.warn (fun m -> m "Failed to decode message\n") in 
                  decode_msgs rbuf ms
              end
            else if available = mlen then
              begin                                
                try 
                  let m = (decode_msg rbuf) in 
                  Abuf.clear rbuf;
                  m::ms
                with
                | _ ->
                  let _ = Logs_lwt.warn (fun m -> m "Failed to decode message\n") in 
                  ms
              end          
            else
              begin                
                let i = ref 0 in 
                Abuf.set_r_pos rpos rbuf;               
                let rbs = Abuf.readable_bytes rbuf in
                while !i < rbs do 
                  Abuf.set_byte (Abuf.read_byte rbuf) ~at:!i rbuf;
                  i := !i + 1
                done ;
                Abuf.set_r_pos 0 rbuf;
                Abuf.set_w_pos !i rbuf;                   
                ms 
              end
            with 
            | _ -> 
              Printf.printf ">> Failed to read msg-len (rpos = %d) (available=%d)\n" rpos (Abuf.readable_bytes rbuf);
              Abuf.set_r_pos rpos rbuf;
              ms
          in 
          let frame = Frame.to_list @@ Frame.create @@ decode_msgs rbuf [] in       
          Lwt.catch
              (fun () -> ProtocolEngine.handle_message engine sex frame) 
              (fun e -> 
                Logs_lwt.warn (fun m -> m "Error handling messages from session %s : %s" 
                  (Id.to_string (TxSession.id sex))
                  (Printexc.to_string e))
                >>= fun _ -> Lwt.return []) 
            >>= function
            | [] -> Lwt.return_unit
            | ms -> 
              Abuf.clear wbuf;
              zwriter (Frame.create ms) wbuf >>= fun _ -> Lwt.return_unit
              
  in 
  Lwt.join [ZTcpTransport.start tx dispatcher_svc; ProtocolEngine.start engine]



let run tcpport peers strength bufn style_renderer level = 
  setup_log style_renderer level; 
  Lwt_main.run @@ run_broker tcpport peers strength bufn
   
let () =
  Printexc.record_backtrace true;
  Lwt_engine.set (new Lwt_engine.libev ()) ;
  let env = Arg.env_var "ZENOD_VERBOSITY" in
  let _ = Term.(eval (const run $ tcpport $ peers $ strength $ bufn $ Fmt_cli.style_renderer () $ Logs_cli.level ~env (), Term.info "zenohd")) in  ()
  
