open Ztypes
open Ziobuf
open Zmessage
open Zsession
open Mcodec
open Apero
open Lwt.Infix
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
    Lwt.return {
      tx_id;
      socket = sock;
      locator;
      sessions = [];
      listener;
      handle_close;
      buf_len}


  let send s msg  =
    let  open Apero.ResultM.InfixM in
    let open IOBuf in
    let%lwt _ = Logs_lwt.debug (fun m -> m  ">> sending message: %s\n" @@ Message.to_string msg ) in
    let buf = IOBuf.clear Session.(s.wbuf) in
    
    ResultM.try_get 
      ~run:(Znet.send_vec s.socket)
      ~fail_with:(fun e -> Lwt.fail (ZError e))
      ~on:(Mcodec.encode_msg msg buf
      >>> IOBuf.flip
      >>= (fun buf -> 
        let wlbuf = IOBuf.clear s.wlenbuf in        
        Tcodec.encode_vle (Vle.of_int @@ IOBuf.limit buf) wlbuf  
        >>> IOBuf.flip
        >>= fun (wlbuf) -> ResultM.return ([wlbuf; buf])))

    


  let maybe_send s smsg = match smsg with
    | Some msg -> send s msg
    | _ ->  Lwt.return 0


  let get_message_length sock buf =
    let rec extract_length v bc buf =       
      ResultM.try_get 
        ~on:(IOBuf.reset_with 0 1 buf)
        ~run:(fun buf ->
          match%lwt Znet.recv sock buf with
          | 0 -> Lwt.fail @@ ZError Error.(ClosedSession (Msg "Peer closed the session unexpectedly"))
          | _ ->
            ResultM.try_get 
              ~on:(IOBuf.get_char buf)
              ~run:(fun (b, buf) -> 
                match int_of_char b with
                | c when c <= 0x7f -> Lwt.return (v lor (c lsl (bc * 7)))
                | c  -> extract_length  (v lor ((c land 0x7f) lsl bc)) (bc + 1) buf)
              ~fail_with:(fun e -> Lwt.fail @@ ZError e))
            
        ~fail_with:(fun e -> Lwt.fail @@ ZError e)
        
    in extract_length 0 0 buf 


  let string_of_sockaddr = function
    | Lwt_unix.ADDR_UNIX s -> s
    | Lwt_unix.ADDR_INET (ip, port) -> (Unix.string_of_inet_addr ip) ^ (string_of_int port)


  let close_session (tx : t) (s : Session.t) =
    let (cs, xs) = List.partition Session.(fun i -> i.sid = s.sid) tx.sessions in
    tx.sessions <- xs ;
    List.iter Session.(fun c ->
        let _ = try%lwt
          Lwt_unix.close c.socket
        with
        | _ -> Lwt.return_unit
        in ()
      ) cs ;
    tx.handle_close s ;

    Lwt.return_unit
  
  let handle_session tx (s: Session.t) =
    let rec serve_session () = 
      let%lwt _ = Logs_lwt.debug (fun m -> m "Looping to serve session %Ld" s.sid) in     
      let r = try%lwt
        let%lwt _ = Logs_lwt.debug (fun m -> m "======== Transport handling session %Ld" s.sid) in      
        let%lwt len = get_message_length s.socket s.rlenbuf in
        let%lwt _ = Logs_lwt.debug (fun m -> m  "Received message of %d bytes" len) in
        let%lwt rbuf = lwt_of_result @@ IOBuf.reset_with 0 len s.rbuf in
        let%lwt _ = Znet.recv s.socket rbuf in
        let%lwt _ = Logs_lwt.debug (fun m ->  m "tx-received: %s\n" (IOBuf.to_string rbuf)) in
        let%lwt (msg, buf) = match Mcodec.decode_msg rbuf with 
        | Ok (m,b) -> Lwt.return (m,b)
        | Error e -> 
          let%lwt _ = Logs_lwt.debug (fun m -> m "%s" (Error.show_e e)) in 
          Lwt.fail (ZError e)
        in        
        let replies = tx.listener s msg in
        let rec send_loop = function
            | [] -> Lwt.return_unit
            | h::tl ->
              let%lwt _ = send s h in
              send_loop tl
        in
          let%lwt _ = send_loop replies in
          let%lwt _ = Logs_lwt.debug (fun m -> m "Message Handled successfully!\n") in           
          Lwt.return_unit          
      with
      |_ ->
        let%lwt _ = Lwt_log.debug (Printf.sprintf "Received zero sized frame, closing session %Ld" s.sid) in
        let%lwt _ = close_session tx s in
        Lwt.fail @@ ZError Error.(ClosedSession (Msg "received zero sized message"))      
      in r >>= serve_session
      
  in serve_session ()       

  let accept_connection tx conn =
    let fd, addr = conn in
    let%lwt _ = Logs_lwt.debug (fun m -> m "Incoming connection from: %s"  (string_of_sockaddr addr)) in
    let%lwt session = Session.make tcp_id fd addr tx.buf_len in
    session.close <- (fun () -> close_session tx session) ;
    session.send <- (fun msg -> Lwt.bind (send session msg) (fun _ -> Lwt.return_unit)) ;
    tx.sessions <- session :: tx.sessions ;
    let _ = Lwt.on_failure (handle_session tx session)  (fun e -> Lwt_log.ign_error (Printexc.to_string e)) in

    let%lwt _ = Logs_lwt.debug (fun m ->  m "New Transport Session with Id = %s"  (SessionId.to_string session.sid)) in
    Lwt.return_unit


  let run_loop tx =
    let rec serve () =
      let%lwt tx = tx in
      let%lwt connection = Lwt_unix.accept tx.socket in
      let%lwt _ = accept_connection tx connection in
      serve ()
    in serve
end
