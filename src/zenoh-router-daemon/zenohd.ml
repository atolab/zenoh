open Cmdliner


let tcpport = Arg.(value & opt int 7447 & info ["t"; "tcpport"] ~docv:"TCPPORT" ~doc:"listening port")
let peers = Arg.(value & opt string "" & info ["p"; "peers"] ~docv:"PEERS" ~doc:"peers")
let strength = Arg.(value & opt int 0 & info ["s"; "strength"] ~docv:"STRENGTH" ~doc:"broker strength")
let bufn = Arg.(value & opt int 8 & info ["w"; "wbufn"] ~docv:"BUFN" ~doc:"number of write buffers")

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

let run tcpport peers strength bufn style_renderer level = 
  setup_log style_renderer level; 
  Lwt_main.run @@ Zengine.run_broker tcpport peers strength bufn
   
let () =
  Printexc.record_backtrace true;
  Lwt_engine.set (new Lwt_engine.libev ());
  let env = Arg.env_var "ZENOD_VERBOSITY" in
  let _ = Term.(eval (const run $ tcpport $ peers $ strength $ bufn $ Fmt_cli.style_renderer () $ Logs_cli.level ~env (), Term.info "zenohd")) in  ()
  
