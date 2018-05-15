open Lwt
open Lwt.Infix
open Zenoh


let () =
  (Lwt_log.append_rule "*" Lwt_log.Debug)


let%lwt dbuf = IOBuf.create 1024

let%lwt pid =
  let%lwt buf = IOBuf.create 16 in
  let%lwt buf = IOBuf.put_string buf "zenohc" in
  IOBuf.flip buf

let lease = 0L
let version = Char.chr 0x01


let default_conduit = Conduit.make 0

module Command = struct
  type t = Cmd of string | CmdIArgs of string * int list | CmdSArgs of string * string list | NoCmd

  let of_string s =
    match (String.split_on_char ' ' s |> List.filter (fun x -> x != "") |> List.map (fun s -> String.trim s)) with
    | [] ->  NoCmd
    | [a] -> Cmd a
    | h::tl when h = "pub" -> CmdSArgs (h, tl)
    | a::tl  -> CmdIArgs (a, tl |> (List.map (int_of_string)))

end

let%lwt lbuf = IOBuf.create 16
let%lwt wbuf = IOBuf.create 8192
let%lwt rbuf = IOBuf.create 8192

let get_message_length sock buf =
  let rec extract_length buf v bc =
    let%lwt buf = IOBuf.reset_with buf 0 1 in
    match%lwt IOBuf.recv sock buf with
    | 0 -> fail @@ ZError Error.(ClosedSession (Msg "Peer closed the session unexpectedly"))
    | _ ->
      let%lwt (b, buf) = (IOBuf.get_char buf) in
      match int_of_char b with
      | c when c <= 0x7f -> return (v lor (c lsl (bc * 7)))
      | c  -> extract_length buf (v lor ((c land 0x7f) lsl bc)) (bc + 1)
  in extract_length buf 0 0

let get_args () =
  if Array.length Sys.argv < 3 then
    begin
      let ipaddr = "127.0.0.1" in
      let port = 7447 in
      print_endline (Printf.sprintf "[Connecting to broker at %s:%d -- to use other address run as: zenohc <ipaddr> <port>]" ipaddr port)
    ; (ipaddr, port)
    end
  else (Array.get Sys.argv 1, int_of_string @@ Array.get Sys.argv 2)

let send_message sock msg =
  ignore_result @@ Lwt_io.printf "[Send Message]\n"  ;

  let%lwt wbuf = IOBuf.clear wbuf
  and lbuf = IOBuf.clear lbuf in

  let%lwt wbuf = Marshaller.write_msg wbuf msg in
  let%lwt wbuf = IOBuf.flip wbuf in
  let len = IOBuf.get_limit wbuf in
  let%lwt lbuf = IOBuf.put_vle lbuf (Vle.of_int len) in
  let%lwt lbuf = IOBuf.flip lbuf in
  ignore_result @@ Lwt_log.debug (Printf.sprintf "tx-buffer: %s" (IOBuf.to_string wbuf)) ;
  let%lwt n = IOBuf.send sock lbuf in
  let%lwt n = IOBuf.send sock wbuf in
  ignore_result @@ Lwt_io.printf "[send_message: sent %d/%d bytes]\n" n len
  ; Lwt_log.debug @@ Printf.sprintf "tx-send: " ^ (IOBuf.to_string wbuf) ^ "\n"


let send_scout sock =
  ignore_result (Lwt_io.print "send_scout\n") ;
  let msg = Message.Scout (Scout.create (Vle.of_int 1) []) in send_message sock msg

let send_open  sock =
  ignore_result (Lwt_io.print "send_open\n") ;
  let msg = Message.Open (Open.create version pid lease Locators.empty Properties.empty)
  in send_message sock msg

let send_close sock =
  ignore_result (Lwt_io.print "send_close\n") ;
  let msg = Message.Close (Close.create pid (Char.chr 1)) in send_message sock msg

let send_declare_pub sock id =
  let pub_id = Vle.of_int id in
  let decls = Declarations.singleton @@ PublisherDecl (PublisherDecl.create pub_id Properties.empty)  in
  let msg = Message.Declare (Declare.create (true, true) (Conduit.next_rsn default_conduit) decls)
  in send_message sock msg

let send_declare_sub sock id =
  let sub_id = Vle.of_int id in
  let decls = Declarations.singleton @@ SubscriberDecl (SubscriberDecl.create sub_id SubscriptionMode.push_mode [])  in
  let msg = Message.Declare (Declare.create (true, true) (Conduit.next_rsn default_conduit) decls)
  in send_message sock msg


let send_stream_data sock rid data =
  (do_
  ; buf <-- IOBuf.clear dbuf
  ; buf <-- IOBuf.put_string buf data
  ; buf <-- IOBuf.flip buf
  ;  sn <-- return (Conduit.next_usn default_conduit)
  ;  msg  <-- return @@ Message.StreamData (StreamData.create (false,false) sn rid None buf)
  ; send_message sock msg)


let produce_message sock cmd =
  ignore_result @@ Lwt_io.printf "[Producing Message]\n"  ;
  match cmd with
  | Command.Cmd msg -> (
    match msg with
    | "scout" -> send_scout sock
    | "close" -> send_close sock
    | "open" -> send_open sock
    | _ -> Lwt_io.printf "[Error: The message <%s> is unkown]\n" msg >>= return)

  | Command.CmdIArgs (msg, xs) -> (
      match msg with
      | "dpub" -> send_declare_pub sock (List.hd xs)
      | "dsub" -> send_declare_sub sock (List.hd xs)
      | _ -> Lwt_io.printf "[Error: The message <%s> is unkown]\n" msg >>= return)

  | Command.CmdSArgs (msg, xs) -> (
      match msg with
      | "pub" ->
        let rid = Vle.of_string (List.hd xs) in
        let data = List.hd @@ List.tl @@ xs in
        send_stream_data sock rid data
      | _ -> Lwt_io.printf "[Error: The message <%s> is unkown]\n" msg >>= return)

  | Command.NoCmd -> return_unit


let rec run_write_loop sock =
  ignore_result @@ Lwt_io.printf "[Starting run_write_loop]\n"  ;
  let _ = Lwt_io.printf ">> "  in
  let%lwt msg = Lwt_io.read_line Lwt_io.stdin in
  ignore_result @@ Lwt_io.printf "[Read Data from input]\n"  ;
  let _ = produce_message sock (Command.of_string msg) in
  run_write_loop sock


let process_incoming_message = function
  | Message.StreamData dmsg ->
    let rid = StreamData.id dmsg in
    let buf = StreamData.payload dmsg in
    let%lwt (data, buf) =  IOBuf.get_string  buf in
    let%lwt _ = Lwt_io.printf "\n[received data rid: %Ld payload: %s]\n>>\n" rid data in
    return_true
  | msg ->
    let%lwt _ = Lwt_io.printf "\n[received: %s]\n>>\n" (Message.to_string msg) in
    return_true


let rec run_read_loop sock continue =
  ignore_result @@ Lwt_io.printf "[Starting run_read_loop]\n"  ;
  if continue then
    let%lwt len = get_message_length sock rbuf in
    ignore_result @@ Lwt_io.printf "[Received message of %d bytes]\n" len ;
    if len = 0 then Lwt_unix.close sock
    else
      let%lwt n = Lwt_bytes.recv sock (IOBuf.to_bytes rbuf) 0 len [] in
      let%lwt rbuf = IOBuf.set_position rbuf 0 in
      let%lwt rbuf = IOBuf.set_limit rbuf len in
      ignore_result @@ (Lwt_log.debug @@ Printf.sprintf "tx-received: " ^ (IOBuf.to_string rbuf) ^ "\n") ;
      let%lwt (msg, rbuf) =  Marshaller.read_msg rbuf in
      let%lwt c = process_incoming_message msg in
      run_read_loop sock c

  else
    return_unit


let () =
  let addr, port  = get_args () in
  let open Lwt_unix in
  let sock = socket PF_INET SOCK_STREAM 0 in
  let saddr = ADDR_INET (Unix.inet_addr_of_string addr, port) in
  let _ = connect sock  saddr in

  Lwt_main.run @@ Lwt.join [run_write_loop sock; run_read_loop sock true]
