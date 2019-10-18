open Cmdliner

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


let run_disco disco = 
  let open Lwt in
  let open Locator in 
  let scout_addr = Unix.inet_addr_of_string "239.255.0.1" in    
  let scout_port = 7447 in 
  let%lwt locator = match (Locator.of_string @@ "tcp/" ^ disco ^ ":" ^ (string_of_int scout_port)) with 
  | Some l -> Lwt.return l
  | None -> Lwt.fail_with "unable to parse locator"
  in 

  match%lwt (if String.length disco == 0 then 
    Logs_lwt.warn (fun m -> m "Discovery disabled, run with  \"--discovery <iface-addr>\" to enable it") >>= fun _ -> Lwt.return `NoDisco
  else 
    Logs_lwt.info (fun m -> m "Discovery running on interface %s port %d" disco scout_port) >>= fun _ -> Lwt.return `Disco) with 
    | `NoDisco -> Lwt.return_unit
    | `Disco -> 
      (let socket = Lwt_unix.socket Unix.PF_INET Unix.SOCK_DGRAM 0 in 
      let iface = Unix.inet_addr_of_string disco in 
      let saddr = Unix.ADDR_INET (iface, scout_port) in 
      let _ = Lwt_unix.bind socket saddr in 
      let  () = Lwt_unix.mcast_add_membership socket ~ifname:iface scout_addr in 
      let len = 512 in 
      let buf = Bytes.create len in 
      let abuf = Abuf.from_bytes buf in 
      let rec scout_loop () = 
        let open Message in 
        let%lwt (n, src) = Lwt_unix.recvfrom socket buf 0 len  []  in 
        let () = Abuf.set_r_pos 0 abuf in
        let () = Abuf.set_w_pos n abuf in
        let msg = Mcodec.decode_msg abuf in 
        
        let _ = (match src with 
        | Unix.ADDR_INET (ip, port) -> 
          let _ = Logs_lwt.debug (fun m -> m "Received Scout Message %s from %s:%d" (Bytes.to_string buf) (Unix.string_of_inet_addr ip) port) in 
          let _ = match msg with         
          | Scout scout  
            when (Int64.logand (Scout.mask scout) ScoutFlags.scoutBroker)!= Int64.of_int 0 -> 
            let _ = Logs_lwt.info(fun m -> m "Broker scouting from %s, replying with hello" (Unix.string_of_inet_addr ip)) in
            Abuf.clear abuf ;
            let hello = Hello.create ScoutFlags.scoutBroker (Locators.singleton locator) [] in 
            Mcodec.encode_hello hello abuf ;
            let%lwt _ = Lwt_bytes.sendto socket (Lwt_bytes.of_bytes buf) 0 (Abuf.w_pos abuf) [] src in 
            Lwt.return_unit         
          | Scout _ -> Lwt.return_unit               
          | _ -> Logs_lwt.warn (fun m -> m "Received unexpected Message on scouting locator")
          in Lwt.return_unit
        | _ -> 
          let _ = Logs_lwt.warn (fun m -> m "Received Message with non-IP address") 
          in Lwt.return_unit)
        in 
        scout_loop ()
      in 
      scout_loop ())

let run tcpport peers strength usersfile plugins bufn timestamp style_renderer level disco =
  setup_log style_renderer level;
  Lwt_main.run @@ Lwt.join [Zrouter.run tcpport peers strength usersfile plugins bufn timestamp; run_disco disco]

let () =
  Printexc.record_backtrace true;  
  (* Lwt_engine.set (new Lwt_engine.libev ()); *)
  let env = Arg.env_var "ZENOD_VERBOSITY" in
  let _ = Term.(eval (const run $ Zrouter.tcpport $ Zrouter.peers $ Zrouter.strength $ Zrouter.users $ Zrouter.plugins $ Zrouter.bufn $ Zrouter.timestamp $ Fmt_cli.style_renderer () $ Logs_cli.level ~env () $ Zrouter.disco, Term.info "zenohd")) in  ()

