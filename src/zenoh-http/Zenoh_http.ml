open Apero
open Lwt.Infix
open Httpaf
open Httpaf_lwt_unix


let zwrite_kind_put = 0L
let zwrite_kind_update = 1L
let zwrite_kind_remove = 2L
let empty_buf = Abuf.create 0

let encoding_raw = 0x00L
let encoding_string = 0x02L
let encoding_json = 0x04L


let respond ?(body="") ?(headers=Headers.empty) ?(status=`OK) reqd =
  let headers = Headers.add headers "content-length" (String.length body |> string_of_int) in
  let headers = Headers.add headers "Access-Control-Allow-Origin" "*" in
  Reqd.respond_with_string reqd (Response.create ~headers status) body

let respond_usage = respond ~status:`Bad_request ~body:(
  "Welcome to ZENOH REST API\n\n"^
  "Usage:\n\n"^
  "  GET resource?query \n"^
  "    => get all the values for 'resource' using 'query'\n"^
  "  POST /@/local/**\n"^
  "   => get all keys/values from Admin Space\n")

let respond_internal_error reqd error = respond reqd ~status:`Internal_server_error ~body:
  ("INTERNAL ERROR: "^error)

let respond_unsupported reqd meth path = respond reqd ~status:`Bad_request ~body:
  ("Operation "^(Method.to_string meth)^" not supported on path: "^path)


let json_of_results (results : (string * Abuf.t * Ztypes.data_info) list) =
  (* We assume the value can be decoded as a string *)
  let string_of_buf = Apero.compose Bytes.to_string Apero.decode_bytes in
  results
  |> List.map (fun (resname, buf, _) -> Printf.sprintf "\"%s\" : %s" resname  (string_of_buf buf))
  |> String.concat ",\n"
  |> Printf.sprintf "{%s}"


let on_body_read_complete body (action:Abuf.t -> unit) =
  let rec on_read buffer chunk ~off ~len =
    let chunk = Bigstringaf.substring chunk ~off ~len in
    Abuf.write_bytes (Bytes.of_string chunk) buffer;
    Body.schedule_read body ~on_eof:(on_eof buffer) ~on_read:(on_read buffer)
  and on_eof buffer () =
    let buffer' = Abuf.create ~grow:2 (Abuf.readable_bytes buffer + 1) in
    Apero.encode_buf buffer buffer';
    action buffer'
  in
  let buffer = Abuf.create ~grow:1024 1024 in
  Body.schedule_read body ~on_eof:(on_eof buffer) ~on_read:(on_read buffer)

let media_type_regex =
  (* RFC6838 Media type format:   type "/" [tree "."] subtype ["+" suffix] *[";" parameter]   *)
  Str.regexp @@ Printf.sprintf "^\\(%s\\)/\\(\\(%s\\)\\.\\)?\\(%s\\)\\(\\+\\(%s\\)\\)?\\(;\\(%s\\)\\)?$"
    "[A-Za-z0-9][-_!#$&^A-Za-z0-9]*" "[A-Za-z0-9][-_!#$&^A-Za-z0-9]*" "[A-Za-z0-9][-_!#$&^A-Za-z0-9]*" "[A-Za-z0-9][-_!#$&^A-Za-z0-9]*" ".+"

let decode_media_type s =
  let matched_group_option n =
    try Str.matched_group n s with Not_found -> ""
  in
  if Str.string_match media_type_regex s 0 then
    (matched_group_option 1) :: (matched_group_option 3) :: (matched_group_option 4) :: (matched_group_option 6) :: (matched_group_option 8) :: []
  else ( Logs.warn (fun m -> m "[Zhttp] Invalid media type: %s (consider value as RAW encoding)" s); [] )

let encoding_of_content_type = function
  | None -> encoding_raw
  | Some s ->
    let l = decode_media_type s in Logs.info (fun m -> m "[Zhttp] media type: %s" (String.concat " / " l));
    match l with
    | [ "text" ; _ ; _ ; _ ; _ ]
    | [ "application" ; _ ; "x-www-form-urlencoded" ; _ ; _ ]
    | [ "application" ; _ ; "xml" ; _ ; _ ]
    | [ "application" ; _ ; "xhtml+xml" ; _ ; _ ]
      -> encoding_string
    | [ "application" ; _ ; "json" ; _ ; _ ]
      -> encoding_json
    | [] -> Logs.warn (fun m -> m "[Zhttp] Invalid media type: %s (consider value as RAW encoding)" s);
      encoding_raw
    | _ -> Logs.debug (fun m -> m "[Zhttp] Unknown media type: %s (default as RAW encoding)" s);
      encoding_raw


let request_handler zenoh zpid (_ : Unix.sockaddr) reqd =
  let req = Reqd.request reqd in
  Logs.debug (fun m -> m "[Zhttp] HTTP req: %a on %s with headers: %a"
                                  Method.pp_hum req.meth req.target
                                  Headers.pp_hum req.headers);
  let resname, predicate = Astring.span ~sat:(fun c -> c <> '?') req.target in
  let resname =
    if Astring.is_prefix ~affix:"/@/local" resname
    then "/@/"^zpid^(Astring.with_index_range ~first:8 resname)
    else resname
  in
  let predicate = Astring.with_range ~first:1 predicate in
  try begin
    if resname = "/" then
      respond_usage reqd
    else
      match req.meth with
      | `GET -> begin
        Lwt.async (fun _ ->
          try begin
            Logs.debug (fun m -> m "[Zhttp] Zenoh.lquery on %s with predicate: %s" resname predicate);
            Zenoh.lquery zenoh resname predicate >|=
            json_of_results >|= fun body ->
            respond reqd ~body
          end with
          | exn ->
            respond_internal_error reqd (Printexc.to_string exn); Lwt.return_unit
        )
        end
      | `PUT -> begin
          try begin
            on_body_read_complete (Reqd.request_body reqd) (
              fun buf ->
                Lwt.async (fun _ ->
                  let encoding = encoding_of_content_type @@ Headers.get req.headers "content-type" in
                  Logs.debug (fun m -> m "[Zhttp] Zenoh.write put on %s %d bytes with encoding %Ld" resname (Abuf.readable_bytes buf) encoding);
                  Zenoh.write zenoh resname buf ~kind:zwrite_kind_put ~encoding >|= fun _ ->
                  respond reqd ~status:`No_content)
            )
          end with
          | exn ->
            respond_internal_error reqd (Printexc.to_string exn)
        end
      | `Other m when m = "PATCH" -> begin
          try begin
            on_body_read_complete (Reqd.request_body reqd) (
              fun buf ->
                Lwt.async (fun _ ->
                  Logs.debug (fun m -> m "[Zhttp] Zenoh.write update on %s %d bytes" resname (Abuf.readable_bytes buf));
                  Zenoh.write zenoh resname buf ~kind:zwrite_kind_update >|= fun _ ->
                  respond reqd ~status:`No_content)
            )
          end with
          | exn ->
            respond_internal_error reqd (Printexc.to_string exn)
        end
      | `DELETE -> begin
        Lwt.async (fun _ ->
          try begin
            Logs.debug (fun m -> m "[Zhttp] Zenoh.write remove on %s" resname);
            Zenoh.write zenoh resname empty_buf ~kind:zwrite_kind_remove >|= fun _ ->
            respond reqd ~status:`No_content
          end with
          | exn ->
            respond_internal_error reqd (Printexc.to_string exn); Lwt.return_unit
        )
        end
      | _ -> respond_unsupported reqd req.meth resname
  end with
  | exn ->
    Logs.err (fun m -> m "Exception %s raised:\n%s" (Printexc.to_string exn) (Printexc.get_backtrace ()));
    raise exn


let error_handler _ (_ : Unix.sockaddr) ?request:_ error start_response =
  let response_body = start_response Headers.empty in
  begin match error with
  | `Exn exn ->
    Logs.debug (fun m -> m "[Zhttp] error_handler: %s\n%s" (Printexc.to_string exn) (Printexc.get_backtrace ()));
    Body.write_string response_body "INTERNAL SERVER ERROR:\n";
    Body.write_string response_body (Printexc.to_string exn);
    Body.write_string response_body "\n";
  | #Status.standard as error ->
    Logs.debug (fun m -> m "[Zhttp] error_handler: #Status.standard \n%s" (Printexc.get_backtrace ()));
    Body.write_string response_body "INTERNAL SERVER ERROR:\n";
    Body.write_string response_body (Status.default_reason_phrase error)
  end;
  Body.close_writer response_body

let run port =
  let listen_address = Unix.(ADDR_INET (inet_addr_loopback, port)) in
  let%lwt zenoh = Zenoh.zopen "" in
  let zprops = Zenoh.info zenoh in
  let zpid = match Properties.get "peer_pid" zprops with
    | Some pid -> pid
    | None -> Uuid.make () |> Uuid.to_string
  in
  Lwt_io.establish_server_with_client_socket listen_address 
    (Server.create_connection_handler ~request_handler:(request_handler zenoh zpid) ~error_handler:(error_handler zenoh))
  >|= fun _ ->
  Logs.info (fun m -> m "[Zhttp] listening on port tcp/0.0.0.0:%d" port)

let port = Cmdliner.Arg.(value & opt int 8000 & info ["h"; "httpport"] ~docv:"HTTPPORT" ~doc:"Listening http port")

let _ = 
  Logs.debug (fun m -> m "[Zhttp] starting with args: %s" (Array.to_list Sys.argv |> String.concat " "));
  Cmdliner.Term.(eval ~argv:Sys.argv (const run $ port, Cmdliner.Term.info "zenoh-http-plugin"))

