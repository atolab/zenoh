open Zenoh
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


let iface = Arg.(value & opt string "127.0.0.1"  & info ["i"; "iface"] ~docv:"INTERFACE" ~doc:"interface")


let scouting style_renderer level iface  =  
  let open Lwt.Infix in 
  setup_log style_renderer level;
  print_endline "scouting...." ;
  Lwt_main.run @@ (zscout iface ~tries:10 ~period:3.0 () >>= fun locs -> 
    print_endline "Resolved Locators done with scouting";
    let open Locator in 
    let _ = match (Locators.to_list locs) with 
    | [] -> print_endline "no locator found"
    | ls -> List.iteri (fun i l -> Printf.printf "%d-th locator %s\n" i (Locator.to_string l)) ls 
    in  Lwt.return_unit)

let () = 
  Printexc.record_backtrace true;
  let env = Arg.env_var "ZENOH_VERBOSITY" in
  let _ = Term.(eval (const scouting $ Fmt_cli.style_renderer () $ Logs_cli.level ~env () $ iface, Term.info "demo_scout")) in  ()  
