open Lwt
open Lwt.Infix
open Zenoh
open Ztypes
open Marshaller
open Netbuf
open Session

let udp_id = 0x00
let tcp_id = 0x01
let ble_id = 0x02


module Tcp = struct

  type callback = Session.t -> Zenoh.Message.t -> Zenoh.Message.t list

  type t = { tx_id: int;
             socket: Lwt_unix.file_descr;
             locator: Lwt_unix.sockaddr;
             mutable sessions : Session.t list;
             mutable listener : callback;
             buf_len : int
           }



  let backlog = 32

  let create tx_id locator listener buf_len =
    let open Lwt_unix in
    let sock = socket PF_INET SOCK_STREAM 0 in
    let _ = setsockopt sock SO_REUSEADDR true in
    let _ = setsockopt sock TCP_NODELAY true in
    let _ = bind sock locator in
    let _ = listen sock backlog in
    { tx_id;
      socket = sock;
      locator;
      sessions = [];
      listener;
      buf_len}


  let send s msg  =
    ignore_result (Lwt_io.printf ">> sending message: %s\n" @@ Message.to_string msg ) ;
    let open Netbuf in

    ignore ( Result.do_
           ; buf <-- IOBuf.clear Session.(s.wbuf)
           ; buf <-- write_msg buf msg
           ; buf <-- IOBuf.flip buf
           ; wlbuf <-- IOBuf.clear s.wlenbuf
           ; wlbuf <-- IOBuf.put_vle wlbuf  @@ Vle.of_int @@ IOBuf.get_limit  buf
           ; wlbuf <-- IOBuf.flip wlbuf
           ; () ; Lwt.ignore_result @@ Lwt_log.debug @@ Printf.sprintf "tx-send: " ^ (IOBuf.to_string buf) ^ "\n"
           ; () ; ignore_result (Lwt_io.printf "[Sending message of %d bytes]\n" @@ IOBuf.get_limit wlbuf)
           ; iovs <-- return [IOBuf.to_io_vec wlbuf; IOBuf.to_io_vec buf]
           ; () ; Lwt.ignore_result @@ Lwt_bytes.send_msg s.socket iovs []
           ; return ())
           (* ; () ; ignore_result (Lwt_bytes.send s.socket (IOBuf.to_bytes wlbuf) 0 (IOBuf.get_limit wlbuf) [])
           ; () ; let _ = Lwt_bytes.send s.socket (IOBuf.to_bytes buf) 0 (IOBuf.get_limit buf) [] in return () ) *)
  ; return_unit



  let maybe_send s smsg = match smsg with
    | Some msg -> send s msg
    | _ ->  return_unit


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

  let string_of_sockaddr = function
    | Lwt_unix.ADDR_UNIX s -> s
    | Lwt_unix.ADDR_INET (ip, port) -> (Unix.string_of_inet_addr ip) ^ (string_of_int port)


  let close_session (tx : t) (s : Session.t) =
    let (cs, xs) = List.partition Session.(fun i -> i.sid = s.sid) tx.sessions in
    tx.sessions <- xs ;
    List.iter Session.(fun c -> ignore (Lwt_unix.close c.socket) ) cs ;
    return_unit

  let handle_session tx (s: Session.t) =
    let rec serve_session continue =
      if continue then begin
      (get_message_length s.socket s.rlenbuf)
      >>= (fun len ->
          ignore_result @@ Lwt_log.debug (Printf.sprintf "Received message of %d bytes" len) ;
          if len <= 0 then
            Lwt_log.warning (Printf.sprintf "Received zero sized frame, closing session %Ld" s.sid)
            >>= (fun _ -> close_session tx s) >>= (fun _ -> return_false)
          else
            let r =
              Result.(
                or_else
                (do_
                     ; buf <-- IOBuf.clear s.rbuf
                     ; () ; let _ =  Lwt_bytes.recv s.socket (IOBuf.to_bytes buf) 0 len [] in () ;
                     ; buf <-- IOBuf.set_limit buf len
                     ; () ; Lwt.ignore_result @@ Lwt_log.debug @@ Printf.sprintf "tx-received: " ^ (IOBuf.to_string buf) ^ "\n"
                     ; (msg, buf) <-- read_msg buf
                     ; () ;  (tx.listener s msg) |> List.iter (fun m -> ignore_result @@ send s m) ; Result.ok ())
                      (fun e ->
                         let _ = Lwt_log.warning "Received garbled messages, closing session" in
                         let _ = close_session tx s in Result.fail e))
            in if Result.is_ok r then return_true else return_false
        )
      >>= serve_session
    end else return_unit
    in serve_session true


  let accept_connection tx conn =
    let fd, addr = conn in
    let _ = Lwt_log.debug ("Incoming connection from: " ^ (string_of_sockaddr addr)) in
    let session = Session.make tcp_id fd addr tx.buf_len in
    session.close <- (fun () -> close_session tx session) ;
    session.send <- (fun msg -> send session msg) ;
    tx.sessions <- session :: tx.sessions ;
    let _ = Lwt.on_failure (handle_session tx session)  (fun e -> Lwt_log.ign_error (Printexc.to_string e)) in
    Lwt_log.debug ("New Transport Session with Id = " ^ (SessionId.to_string session.sid))  >>= return


  let run_loop tx =
    let rec serve () =
      Lwt_unix.accept tx.socket >>= (fun c -> accept_connection tx c) >>= serve
    in serve

end
