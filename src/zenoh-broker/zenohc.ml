open Lwt
open Lwt.Infix
open Zenoh

let () =
  (Lwt_log.append_rule "*" Lwt_log.Debug)


let (<.>) = Apero.(<.>)

let%lwt dbuf = ZIOBuf.create 1024

let%lwt pid =
  let%lwt buf = ZIOBuf.create 16 in
  let%lwt buf = ZIOBuf.put_string buf "zenohc" in
  ZIOBuf.flip buf

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

let from_ziobuf zbuf =
  let tx = (IOBuf.from_bytes <.> ZIOBuf.to_bytes) in
  Result.( get (do_
                ; buf <-- tx zbuf
                ; buf <-- IOBuf.set_limit buf (ZIOBuf.get_limit zbuf)
                ; IOBuf.set_position buf (ZIOBuf.get_position zbuf)))

(* let from_ziobuf =  Result.(get) <.> (IOBuf.from_bytes <.> ZIOBuf.to_bytes) *)
let to_ziobuf buf =
  let tx = ZIOBuf.from_bytes <.>  IOBuf.to_bytes in
  (do_
  ; zbuf <-- tx buf
  ; zbuf <-- ZIOBuf.set_limit zbuf (IOBuf.get_limit buf)
  ; ZIOBuf.set_position zbuf (IOBuf.get_position buf))


let%lwt lbuf = ZIOBuf.create 16
let%lwt wbuf = ZIOBuf.create 8192
let%lwt rbuf = ZIOBuf.create 8192

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

  let%lwt wbuf = ZIOBuf.clear wbuf
  and lbuf = ZIOBuf.clear lbuf in

  let%lwt wbuf = to_ziobuf @@ (Result.get @@ write_msg (from_ziobuf wbuf) msg) in
  let%lwt wbuf = ZIOBuf.flip wbuf in
  let len = ZIOBuf.get_limit wbuf in
  let%lwt lbuf = ZIOBuf.put_vle lbuf (Vle.of_int len) in
  let%lwt lbuf = ZIOBuf.flip lbuf in
  ignore_result @@ Lwt_log.debug (Printf.sprintf "tx-buffer: %s" (ZIOBuf.to_string wbuf)) ;
  let%lwt n = ZIOBuf.send sock lbuf in
  let%lwt n = ZIOBuf.send sock wbuf in
  ignore_result @@ Lwt_io.printf "[send_message: sent %d/%d bytes]\n" n len
  ; Lwt_log.debug @@ Printf.sprintf "tx-send: " ^ (ZIOBuf.to_string wbuf) ^ "\n"


let send_scout sock =
  ignore_result (Lwt_io.print "send_scout\n") ;
  let msg = Message.Scout (Scout.create (Vle.of_int 1) []) in send_message sock msg

let send_open  sock =
  ignore_result (Lwt_io.print "send_open\n") ;
  let msg = Message.Open (Open.create version (from_ziobuf pid) lease Locators.empty Properties.empty)
  in send_message sock msg

let send_close sock =
  ignore_result (Lwt_io.print "send_close\n") ;
  let msg = Message.Close (Close.create (from_ziobuf pid) (Char.chr 1)) in send_message sock msg

let send_declare_pub sock id =
  let pub_id = Vle.of_int id in
  let decls = Declarations.singleton @@ PublisherDecl (PublisherDecl.create pub_id [])  in
  let msg = Message.Declare (Declare.create (true, true) (Conduit.next_rsn default_conduit) decls)
  in send_message sock msg

let send_declare_sub sock id =
  let sub_id = Vle.of_int id in
  let decls = Declarations.singleton @@ SubscriberDecl (SubscriberDecl.create sub_id SubscriptionMode.push_mode [])  in
  let msg = Message.Declare (Declare.create (true, true) (Conduit.next_rsn default_conduit) decls)
  in send_message sock msg


let send_stream_data sock rid data =
  (do_
  ; buf <-- ZIOBuf.clear dbuf
  ; buf <-- ZIOBuf.put_string buf data
  ; buf <-- ZIOBuf.flip buf
  ;  sn <-- return (Conduit.next_usn default_conduit)
  ;  msg  <-- return @@ Message.StreamData (StreamData.create (false,false) sn rid None (from_ziobuf buf))
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


let get_message_length sock buf =
  let rec extract_length buf v bc =
    let%lwt buf = ZIOBuf.reset_with buf 0 1 in
    match%lwt ZIOBuf.recv sock buf with
    | 0 -> fail @@ ZError Error.(ClosedSession (Msg "Peer closed the session unexpectedly"))
    | _ ->
      let%lwt (b, buf) = (ZIOBuf.get_char buf) in
      match int_of_char b with
      | c when c <= 0x7f -> return (v lor (c lsl (bc * 7)))
      | c  -> extract_length buf (v lor ((c land 0x7f) lsl bc)) (bc + 1)
    in extract_length buf 0 0

let process_incoming_message = function
  | Message.StreamData dmsg ->
    let rid = StreamData.id dmsg in
    (do_
    ; buf <-- to_ziobuf (StreamData.payload dmsg)
    ; (data, a) <-- ZIOBuf.get_string  buf
    ; () ; ignore_result (Lwt_io.printf "\n[received data rid: %Ld payload: %s]\n>>\n" rid data)
    ; return_true)
  | msg -> (Lwt_io.printf "\n[received: %s]\n>>\n" (Message.to_string msg)) >>= (fun _ -> return_true)


let rec run_read_loop sock continue =
  ignore_result @@ Lwt_io.printf "[Starting run_read_loop]\n"  ;
  if continue then
    let%lwt len = get_message_length sock rbuf in
    ignore_result @@ Lwt_io.printf "[Received message of %d bytes]\n" len ;
    if len = 0 then Lwt_unix.close sock
    else
      let%lwt n = Lwt_bytes.recv sock (ZIOBuf.to_bytes rbuf) 0 len [] in
      let%lwt rbuf = ZIOBuf.set_position rbuf 0 in
      let%lwt rbuf = ZIOBuf.set_limit rbuf len in
      ignore_result @@ (Lwt_log.debug @@ Printf.sprintf "tx-received: " ^ (ZIOBuf.to_string rbuf) ^ "\n") ;
      let (msg, rbuf) =  Result.get @@ read_msg (from_ziobuf rbuf) in
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
