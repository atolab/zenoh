open Lwt
open Lwt.Infix
open Netbuf
open Zenoh
open Ztypes
open Marshaller

module Tcp = struct

  type transport_id = int
  type session_id = transport_id * int64
  type callback = session_id -> Zenoh.Message.t -> unit Lwt.t

  type t = { tx_id: transport_id;
             socket: Lwt_unix.file_descr;
             locator: Lwt_unix.sockaddr;
             mutable next_sid : int64;
             mutable sessions : (session_id * Lwt_unix.file_descr) list;
             mutable listener : callback;
             rbuf: IOBuf.t;
             wbuf: IOBuf.t;
             rlenbuf: Lwt_bytes.t;
             wlenbuf: Lwt_bytes.t
           }

  module Error = struct
    type e =
      | UnknownSessionId of session_id
      | ClosedSession of session_id
      | IOError
  end

  module Result : sig include  Monad.ResultS with type e = Error.e end = Monad.ResultM(Error)

  let backlog = 32

  let create tx_id locator rbuf wbuf listener =
    let open Lwt_unix in
    let sock = socket PF_INET SOCK_STREAM 0 in
    let _ = setsockopt sock SO_REUSEADDR true in
    let _ = setsockopt sock TCP_NODELAY true in
    let _ = bind sock locator in
    let _ = listen sock backlog in
    { tx_id;
      socket = sock;
      locator;
      next_sid = 0L;
      sessions = [];
      listener;
      rbuf;
      wbuf;
      rlenbuf = Lwt_bytes.create 16;
      wlenbuf = Lwt_bytes.create 16    }

  let get_message_length tx sock =
    let buf =  tx.rlenbuf in
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


  let handle_connection tx sock addr sid =
    let buf = tx.rbuf in
    let rec serve_connection () =
      get_message_length tx sock >>= (fun len ->
          if len <= 0 then Lwt_unix.close sock
          else
            let _ = Lwt_log.info @@ "Received " ^ (string_of_int len) ^ " bytes"  in
            Lwt_bytes.recv sock (IOBuf.to_bytes buf) 0 len [] >>= (fun rb ->
                let msg = IOBuf.Result.do_
                        ; buf <-- IOBuf.set_limit buf len
                        ; read_msg buf
                in match msg with
                | Ok (m, buf) ->  tx.listener sid m  >>= serve_connection
                | _ ->
                  tx.sessions <- List.filter (fun (id, _) -> id != sid) tx.sessions ;
                  Lwt_log.error ("Received Invalid Message closing session " ^ (Int64.to_string (snd sid)))  >>= (fun _ -> Lwt_unix.close sock)

              )
        )
    in serve_connection ()

  let string_of_sockaddr = function
    | Lwt_unix.ADDR_UNIX s -> s
    | Lwt_unix.ADDR_INET (ip, port) -> (Unix.string_of_inet_addr ip) ^ (string_of_int port)

  let accept_connection tx conn =
    let fd, addr = conn in
    let _ = Lwt_log.info ("Incoming connection from: " ^ (string_of_sockaddr addr)) in
    let sid = (tx.tx_id, tx.next_sid) in
    tx.sessions <- (sid , fd) :: tx.sessions ;
    tx.next_sid <- Int64.add tx.next_sid 1L ;
    let _ = Lwt.on_failure (handle_connection tx fd addr sid)  (fun e -> Lwt_log.ign_error (Printexc.to_string e)) in
    Lwt_log.info ("New Transport Session with Id = " ^ (Int64.to_string (snd sid)))  >>= return

  let send tx sid msg  =
    match List.find_opt (fun (id, _) -> id = sid) tx.sessions with
    | Some (id, sock) ->
      let buf = tx.wbuf in
      let lbuf = IOBuf.Result.get @@ IOBuf.from_bytes tx.wlenbuf in
      let _ = IOBuf.Result.(do_
      ; buf  <-- IOBuf.clear buf
      ; lbuf <-- IOBuf.clear lbuf
      ; buf  <-- write_msg buf msg
      ; return tx)
      in Result.ok tx (* in IO2TX_Result.lift_e r (fun _ -> Error.IOError ) *)
    | None -> Result.fail @@ Error.IOError

  let run_loop tx =
    let rec serve () =
      Lwt_unix.accept tx.socket >>= (fun c -> accept_connection tx c) >>= serve
    in serve


end
