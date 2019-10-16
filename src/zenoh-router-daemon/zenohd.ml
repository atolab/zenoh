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

let run tcpport peers strength usersfile plugins bufn timestamp style_renderer level =
  setup_log style_renderer level;
  Lwt_main.run @@ Zrouter.run tcpport peers strength usersfile plugins bufn timestamp

let () =
  Printexc.record_backtrace true;  
  (* Lwt_engine.set (new Lwt_engine.libev ()); *)
  let env = Arg.env_var "ZENOD_VERBOSITY" in
  let _ = Term.(eval (const run $ Zrouter.tcpport $ Zrouter.peers $ Zrouter.strength $ Zrouter.users $ Zrouter.plugins $ Zrouter.bufn $ Zrouter.timestamp $ Fmt_cli.style_renderer () $ Logs_cli.level ~env (), Term.info "zenohd")) in  ()

