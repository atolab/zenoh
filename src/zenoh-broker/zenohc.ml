open Lwt
open Lwt.Infix
open Zenoh

let dbuf = Result.get @@ IOBuf.create 1024

let pid = let open Result in
  get (do_
      ; buf <-- IOBuf.create 16
      ; buf <-- IOBuf.put_string buf "zenohc"
      ; IOBuf.flip buf)

let lease = 0L
let version = Char.chr 0x01


let default_conduit = Conduit.make 0

module Command = struct
  type t = Cmd of string | CmdIArgs of string * int list | CmdSArgs of string * string list | NoCmd

  let of_string s =
    match (String.split_on_char ' ' s |> List.filter (fun x -> x != "")) with
    | [] ->  NoCmd
    | [a] -> Cmd a
    | h::tl when h = "pub" -> CmdSArgs (h, tl)
    | a::tl  -> CmdIArgs (a, tl |> (List.map (int_of_string)))

end


let lbuf = Result.get @@ IOBuf.create 16
let wbuf = Result.get @@ IOBuf.create 8192
let rbuf = Result.get @@ IOBuf.create 8192

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
  let send_len_msg  lbuf dbuf dlen =
    let lenp = Lwt_bytes.send sock (IOBuf.to_bytes lbuf) 0 (IOBuf.get_limit lbuf) [] in
    let _ = lenp >>= (fun len ->  Lwt_bytes.send sock (IOBuf.to_bytes dbuf) 0 dlen []) in Result.ok ()
  in

  let _ =
    (Result.do_
    ; wbuf <-- IOBuf.clear wbuf
    ; lbuf <-- IOBuf.clear lbuf
    ; wbuf <-- write_msg wbuf msg
    ; wbuf <-- IOBuf.flip wbuf
    ; len <-- return (IOBuf.get_limit wbuf)
    ; lbuf <-- IOBuf.put_vle lbuf (Vle.of_int len)
    ; lbuf <-- IOBuf.flip lbuf
    ; () ; ignore_result @@ Lwt_io.printf "[send_message: sent %d bytes]\n" len
    ; () ; Lwt.ignore_result @@ Lwt_log.debug @@ Printf.sprintf "tx-send: " ^ (IOBuf.to_string wbuf) ^ "\n"
    ; send_len_msg lbuf wbuf len)
  in return_unit

let send_scout sock =
  (* ignore_result (Lwt_io.print "send_scout\n") ; *)
  let msg = Message.Scout (Scout.create (Vle.of_int 1) []) in send_message sock msg

let send_open  sock =
  (* ignore_result (Lwt_io.print "send_open\n") ; *)
  let msg = Message.Open (Open.create version pid lease Locators.empty Properties.empty) in send_message sock msg

let send_close sock =
  (* ignore_result (Lwt_io.print "send_close\n") ; *)
  let msg = Message.Close (Close.create pid (Char.chr 1)) in send_message sock msg

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
  let open Result in
  let buf = Result.get @@
    (do_
    ; buf <-- IOBuf.clear dbuf
    ; buf <-- IOBuf.put_string buf data
    ; IOBuf.flip buf) in
  let sn =  Conduit.next_usn default_conduit in
  let msg =  Message.StreamData (StreamData.create (false,false) sn rid None buf) in
  send_message sock msg


let produce_message sock cmd =
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
  let _ = Lwt_io.printf ">> "  in
  (Lwt_io.read_line Lwt_io.stdin)
  >>= (fun msg -> produce_message sock (Command.of_string msg))
  >>= (fun _ -> run_write_loop sock)

let get_message_length sock buf =
  let rec extract_length buf v bc =
    (Lwt_bytes.recv sock buf 0 1 []) >>=
    (fun n ->
       if n != 1 then (Lwt.return 0)
       else
         begin
           let c = int_of_char @@ (Lwt_bytes.get buf 0) in
            if c <= 0x7f then Lwt.return (v lor (c lsl (bc * 7)))
            else extract_length buf (v lor ((c land 0x7f) lsl bc)) (bc + 1)
          end
    ) in extract_length buf 0 0

let process_incoming_message = function
  | Message.StreamData dmsg ->
    let rid = StreamData.id dmsg in
    let (data, _) = Result.get @@ IOBuf.get_string @@ StreamData.payload dmsg in
    let _ = Lwt_io.printf "\n[received data rid: %Ld payload: %s]\n>>\n" rid data in  ()
  | msg -> let _ = Lwt_io.printf "\n[received: %s]\n>>\n" (Message.to_string msg) in  ()

let rec run_read_loop sock continue =
  if continue then
    (get_message_length sock (IOBuf.to_bytes rbuf))
    >>= (fun len ->
        ignore_result @@ Lwt_io.printf "[Received message of %d bytes]\n" len ;
        if len = 0 then Lwt_unix.close sock >>= (fun _ -> return_false)
        else
          Lwt_bytes.recv sock (IOBuf.to_bytes rbuf) 0 len []
          >>= (fun _ ->
              ignore(
                Result.(do_
                       ; rbuf <-- IOBuf.set_position rbuf 0
                       ; rbuf <-- IOBuf.set_limit rbuf len
                       ; () ; Lwt.ignore_result @@ Lwt_log.debug @@ Printf.sprintf "tx-received: " ^ (IOBuf.to_string rbuf) ^ "\n"
                       ; (msg, rbuf) <-- read_msg rbuf
                       ; return @@ process_incoming_message msg

              )) ; return_true
            ))
    >>= (run_read_loop sock)
  else return_unit

let () =
  let addr, port  = get_args () in
  let open Lwt_unix in
  let sock = socket PF_INET SOCK_STREAM 0 in
  let saddr = ADDR_INET (Unix.inet_addr_of_string addr, port) in
  let _ = connect sock  saddr in

  Lwt_main.run @@ Lwt.join [run_write_loop sock; run_read_loop sock true]
