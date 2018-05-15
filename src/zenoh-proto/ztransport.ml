open Lwt
open Lwt.Infix
open Ztypes
open Ziobuf
open Zmessage
open Zsession
open Zmarshaller

let udp_id = 0x00
let tcp_id = 0x01
let ble_id = 0x02


module Tcp = struct

  (** Should change this to use an algebraic data type that
      would allow us to notify connection being closed.
      Curretnly we could propagate a fictuous close...
      But that would cause more work then necessary due
      to the close being sent back.
  *)
  type callback = Session.t -> Message.t -> Message.t list
  type close_handler = Session.t -> unit

  type t = { tx_id: int;
             socket: Lwt_unix.file_descr;
             locator: Lwt_unix.sockaddr;
             mutable sessions : Session.t list;
             listener : callback;
             handle_close : close_handler;
             buf_len : int
           }

  let backlog = 32

  let create tx_id locator listener handle_close buf_len =
    let open Lwt_unix in
    let sock = socket PF_INET SOCK_STREAM 0 in
    let _ = setsockopt sock SO_REUSEADDR true in
    let _ = setsockopt sock TCP_NODELAY true in
    let _ = bind sock locator in
    let _ = listen sock backlog in
    return {
      tx_id;
      socket = sock;
      locator;
      sessions = [];
      listener;
      handle_close;
      buf_len}


  let send s msg  =
    let open IOBuf in
    let%lwt _ = Lwt_log.debug (Printf.sprintf ">> sending message: %s\n" @@ Message.to_string msg ) in
    let%lwt buf =IOBuf.clear Session.(s.wbuf) in
    let%lwt buf = Marshaller.write_msg buf msg in
    let%lwt buf =IOBuf.flip buf in
    let%lwt wlbuf = IOBuf.clear s.wlenbuf in
    let%lwt wlbuf = IOBuf.put_vle wlbuf  @@ Vle.of_int @@ IOBuf.get_limit  buf in
    let%lwt wlbuf = IOBuf.flip wlbuf in
    let%lwt rs = IOBuf.send_vec s.socket [wlbuf; buf] in
    let%lwt _ = Lwt_log.debug (Printf.sprintf ">>>>>>> Done sending message: %s <<<<<<\n" @@ Message.to_string msg ) in
    return_unit



  let maybe_send s smsg = match smsg with
    | Some msg -> send s msg
    | _ ->  return_unit


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

  let string_of_sockaddr = function
    | Lwt_unix.ADDR_UNIX s -> s
    | Lwt_unix.ADDR_INET (ip, port) -> (Unix.string_of_inet_addr ip) ^ (string_of_int port)


  let close_session (tx : t) (s : Session.t) =
    let (cs, xs) = List.partition Session.(fun i -> i.sid = s.sid) tx.sessions in
    tx.sessions <- xs ;
    List.iter Session.(fun c -> ignore (Lwt_unix.close c.socket) ) cs ;
    tx.handle_close s ;

    return_unit

  let handle_session tx (s: Session.t) =
    let rec serve_session () =
      let%lwt _ = Lwt_log.debug (Printf.sprintf "======== Transport handling session %Ld" s.sid) in
      let%lwt r = match%lwt get_message_length s.socket s.rlenbuf with
      | 0 ->
        let%lwt _ = Lwt_log.debug (Printf.sprintf "Received zero sized frame, closing session %Ld" s.sid) in
        let%lwt _ = close_session tx s in
        Lwt.fail @@ ZError Error.(ClosedSession NoMsg)

      | len ->
          let%lwt _ = Lwt_log.debug (Printf.sprintf "Received message of %d bytes" len) in
          let%lwt buf = IOBuf.reset_with s.rbuf 0 len in
          let%lwt _ = IOBuf.recv s.socket buf in
          let%lwt _ = Lwt_log.debug @@ Printf.sprintf "tx-received: " ^ (IOBuf.to_string buf) ^ "\n" in
          let%lwt (msg, buf) = Marshaller.read_msg buf in
          let replies = tx.listener s msg in
          let rec send_loop = function
            | [] -> return_unit
            | h::tl ->
              let%lwt _ = send s h in
              send_loop tl
          in
          let%lwt _ = send_loop replies
          in
          let%lwt _ = Lwt_log.debug "Message Handled successfully!\n" in
          return_unit
      in serve_session ()
    in serve_session ()

  let accept_connection tx conn =
    let fd, addr = conn in
    let _ = Lwt_log.debug ("Incoming connection from: " ^ (string_of_sockaddr addr)) in
    let%lwt session = Session.make tcp_id fd addr tx.buf_len in
    session.close <- (fun () -> close_session tx session) ;
    session.send <- (fun msg -> send session msg) ;
    tx.sessions <- session :: tx.sessions ;
    let _ = Lwt.on_failure (handle_session tx session)  (fun e -> Lwt_log.ign_error (Printexc.to_string e)) in
    Lwt_log.debug ("New Transport Session with Id = " ^ (SessionId.to_string session.sid))  >>= return


  let run_loop tx =
    let rec serve () =
      let%lwt tx = tx in
      let%lwt connection = Lwt_unix.accept tx.socket in
      let%lwt _ = accept_connection tx connection in
      serve ()
    in serve
end
