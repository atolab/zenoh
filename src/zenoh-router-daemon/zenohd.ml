open Cmdliner

let scout_port = 7447 
let scout_mcast_addr = Unix.inet_addr_of_string "239.255.0.1"
let scout_addr = Unix.ADDR_INET (scout_mcast_addr, scout_port)
let max_scout_msg_size = 1024


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

let make_hello_buf locator = 
  let open Message in 
  let open Locator in 
  let open Mcodec in 
  let hello = Hello.create ScoutFlags.scoutBroker (Locators.singleton locator) [] in 
  let buf = Bytes.create max_scout_msg_size in 
  let abuf = Abuf.from_bytes buf in  
  let _ = Logs.debug (fun m -> m "Buffer Capacity %d" (Abuf.capacity abuf)) in 
  Abuf.clear abuf; 
  encode_hello hello abuf ;
  (buf, 0, Abuf.w_pos abuf)

let handle_msg socket msg dest hello_buf =  
  let open Message in 
  let open Lwt.Infix in 
  match msg with         
  | Scout scout  
      when (Int64.logand (Scout.mask scout) ScoutFlags.scoutBroker)!= Int64.of_int 0 ->     
    let (buf, offset, len) = hello_buf in 
    Lwt_bytes.sendto socket (Lwt_bytes.of_bytes buf) offset len [] dest >>= fun _ -> Lwt.return_unit
                    
  | Scout _ -> Lwt.return_unit               
  | _ -> let _ = Logs.warn (fun m -> m "Received unexpected Message on scouting locator") in Lwt.return_unit


let rec scout_loop socket hello_buf = 
  let open Lwt.Infix in 
  let buf = Bytes.create max_scout_msg_size in 
  let abuf = Abuf.from_bytes buf in 
  Abuf.clear abuf ;
  let _ = Logs.debug (fun m -> m "Scouting..." ) in
  let%lwt (n, src) = Lwt_unix.recvfrom socket buf 0 max_scout_msg_size  []  in 
  let _ = Logs.debug (fun m -> m "Received Message %d bytes" n ) in
  (match n with 
  | 0 ->  Lwt.return_unit
  | _ ->     
    let () = Abuf.set_r_pos 0 abuf in
    let () = Abuf.set_w_pos n abuf in
    (Lwt.try_bind 
      (fun () -> Lwt.return @@ Mcodec.decode_msg abuf) 
      (fun msg -> handle_msg socket msg src hello_buf)
      (fun _ -> Lwt.return_unit))) >>= fun _ -> scout_loop socket hello_buf


let run_scouting iface locator = 
  
  let socket = Lwt_unix.socket Unix.PF_INET Unix.SOCK_DGRAM 0 in     
  let saddr = Unix.ADDR_INET (Unix.inet_addr_any, scout_port) in 
  let%lwt () = Lwt_unix.bind socket saddr in 
  let _ = Logs.info (fun m -> m "Joining MCast group") in  
  let  () = Lwt_unix.mcast_add_membership socket ~ifname:iface scout_mcast_addr in 
  let hello_buf = make_hello_buf locator in 
  scout_loop socket hello_buf
      
      


let run_disco disco = 
  match disco with
  | "" -> 
    let _ = Logs.info (fun m -> m "Scouting disabled, run with `--discovery <iface>` to enable it\n") 
    in Lwt.return_unit
  | _ -> 
    let open Locator in 
    match Locator.of_string ("tcp/" ^ disco ^ ":" ^ (string_of_int(scout_port))) with
    | Some locator ->       
      let _ = Logs.info (fun m -> m "Running scouting on interface %s\n" disco) in 
      let iface = Unix.inet_addr_of_string disco in         
      run_scouting iface locator
    | _ -> let _ = Logs.warn (fun m -> m "Invalid scouting interface %s" disco) in Lwt.return_unit
    

let run tcpport peers strength usersfile plugins bufn timestamp style_renderer level disco =
  setup_log style_renderer level;
  Lwt_main.run @@ Lwt.join [Zrouter.run tcpport peers strength usersfile plugins bufn timestamp; run_disco disco]

let () =
  Printexc.record_backtrace true;  
  (* Lwt_engine.set (new Lwt_engine.libev ()); *)
  let env = Arg.env_var "ZENOD_VERBOSITY" in
  let _ = Term.(eval (const run $ Zrouter.tcpport $ Zrouter.peers $ Zrouter.strength $ Zrouter.users $ Zrouter.plugins $ Zrouter.bufn $ Zrouter.timestamp $ Fmt_cli.style_renderer () $ Logs_cli.level ~env () $ Zrouter.disco, Term.info "zenohd")) in  ()

