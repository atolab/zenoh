

let setup_log style_renderer level =
  Fmt_tty.setup_std_outputs ?style_renderer ();
  Logs.set_level level;
  Logs.set_reporter (Logs_fmt.reporter ());
  ()


(* Run it *)
let () =
  setup_log (Some `None) (Some Logs.Debug);
  Printexc.record_backtrace true;
  Alcotest.run "Zenoh Test" [
    "test_zenoh_common", Test_zenoh_common.all_tests;
  ]
