open Lwt
open Lwt.Infix
open Zenoh
open Apero
open Zproperty

(* let () =
  (Lwt_log.append_rule "*" Lwt_log.Debug) *)


let dbuf = IOBuf.create 1024

let pid  = IOBuf.flip @@ ResultM.get @@ IOBuf.put_string "zenohc" (IOBuf.create 16) 

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

let lbuf = IOBuf.create 16
let wbuf = IOBuf.create 8192
let rbuf = IOBuf.create 8192

let get_message_length sock buf =
  let rec extract_length buf v bc =
    let buf = ResultM.get @@ IOBuf.reset_with  0 1 buf in
    match%lwt Znet.recv sock buf with
    | 0 -> fail @@ ZError Error.(ClosedSession (Msg "Peer closed the session unexpectedly"))
    | _ ->
      let (b, buf) = ResultM.get (IOBuf.get_char buf) in
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

let send_message sock (msg: Message.t) =
  let open ResultM.InfixM in
  let wbuf = IOBuf.clear wbuf
  and lbuf = IOBuf.clear lbuf in
  
  let wbuf = ResultM.get (Mcodec.encode_msg msg wbuf >>> IOBuf.flip) in

  let len = IOBuf.limit wbuf in
  let lbuf = ResultM.get (Tcodec.encode_vle (Vle.of_int len) lbuf >>> IOBuf.flip) in
  
  let%lwt n = Znet.send sock lbuf in
  Znet.send sock wbuf
  

let send_scout sock =
  let%lwt _ = Lwt_log.debug "send_scout\n" in
  let msg = Message.Scout (Scout.create (Vle.of_int 1) []) in send_message sock msg

let send_open  sock =
  let%lwt _ = Lwt_log.debug "send_open\n" in
  let msg = Message.Open (Open.create version pid lease Locators.empty Properties.empty)
  in send_message sock msg

let send_close sock =
  let%lwt _ = Lwt_log.debug "send_close\n" in
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
  let open ResultM.InfixM in  
  let sn = Conduit.next_usn default_conduit in   
  let buf = ResultM.get (Tcodec.encode_string data (IOBuf.clear dbuf)
  >>> IOBuf.flip) in
  let msg = Message.StreamData (StreamData.create (false,false) sn rid None buf) in
  send_message sock msg
  

let produce_message sock cmd =
  match cmd with
  | Command.Cmd msg -> (
    match msg with
    | "scout" -> send_scout sock
    | "close" -> send_close sock
    | "open" -> send_open sock
    | _ ->
      let%lwt _ = Lwt_io.printf "[Error: The message <%s> is unkown]\n" msg in
      return 0)

  | Command.CmdIArgs (msg, xs) -> (
      match msg with
      | "dpub" -> send_declare_pub sock (List.hd xs)
      | "dsub" -> send_declare_sub sock (List.hd xs)
      | _ ->
        let%lwt _ = Lwt_io.printf "[Error: The message <%s> is unkown]\n" msg in
        return 0)

  | Command.CmdSArgs (msg, xs) -> (
      match msg with
      | "pub" ->
        let rid = Vle.of_string (List.hd xs) in
        let data = List.hd @@ List.tl @@ xs in
        send_stream_data sock rid data
        | _ ->
          let%lwt _ = Lwt_io.printf "[Error: The message <%s> is unkown]\n" msg in
          return 0)

  | Command.NoCmd -> return 0


let rec run_encode_loop sock =
  let%lwt _ = Lwt_log.debug "[Starting run_encode_loop]\n"  in
  let _ = Lwt_io.printf ">> "  in
  let%lwt msg = Lwt_io.read_line Lwt_io.stdin in
  let%lwt _ = produce_message sock (Command.of_string msg) in
  run_encode_loop sock


let process_incoming_message = function
  | Message.StreamData dmsg ->
    let rid = StreamData.id dmsg in
    let buf = StreamData.payload dmsg in
    let (data, buf) = ResultM.get @@ Tcodec.decode_string  buf in
    let%lwt _ = Lwt_io.printf "\n[received data rid: %Ld payload: %s]\n>> " rid data in
    return_true
  | msg ->
    let%lwt _ = Lwt_io.printf "\n[received: %s]\n>> " (Message.to_string msg) in
    return_true


let rec run_decode_loop sock =
  let%lwt _ = Lwt_log.debug "[Starting run_decode_loop]\n"  in
  let r = try%lwt
    let%lwt len = get_message_length sock rbuf in
    let%lwt n = Lwt_bytes.recv sock (IOBuf.to_bytes rbuf) 0 len [] in
    let rbuf = ResultM.get @@ IOBuf.set_position 0 rbuf in
    let rbuf = ResultM.get @@ IOBuf.set_limit len rbuf in
    let%lwt _ = Lwt_log.debug @@ Printf.sprintf "tx-received: " ^ (IOBuf.to_string rbuf) ^ "\n" in
    let (msg, rbuf) = ResultM.get @@ Mcodec.decode_msg rbuf in
    let%lwt c = process_incoming_message msg in
    return sock
  with
  | _ ->
    let%lwt _ = Lwt_log.debug "Connection close by peer" in
    try%lwt
      let%lwt _ = Lwt_unix.close sock in
      fail @@ ZError Error.(ClosedSession NoMsg)
    with
    | _ ->
      fail @@ ZError Error.(ClosedSession NoMsg)
  in r >>= run_decode_loop

let () =
  let addr, port  = get_args () in
  let open Lwt_unix in
  let sock = socket PF_INET SOCK_STREAM 0 in
  let _ = setsockopt sock SO_REUSEADDR true in
  let _ = setsockopt sock TCP_NODELAY true in
  let saddr = ADDR_INET (Unix.inet_addr_of_string addr, port) in
  let _ = connect sock  saddr in

  Lwt_main.run @@ Lwt.join [run_encode_loop sock; run_decode_loop sock >>= fun _ -> return_unit]
