open Lwt
open Netbuf
open Zenoh
open Apero
open Ztypes

let framing_buf_len = 16

module SessionId :  sig
  include (module type of Int64)
  val next_id : unit -> t
end = struct
  include Int64
  let session_count = ref 0L

  let next_id () =
    let r = !session_count in  session_count := add !session_count 1L ; r
end

module Session : sig
  type t = {
    tx_id : int;
    sid : SessionId.t;
    socket : Lwt_unix.file_descr;
    remote_addr : Lwt_unix.sockaddr;
    rbuf: IOBuf.t;
    wbuf: IOBuf.t;
    rlenbuf: Lwt_bytes.t;
    wlenbuf: IOBuf.t;
    mutable close : unit -> unit Lwt.t;
    mutable send : Message.t -> unit Lwt.t
  }
  val make : int -> Lwt_unix.file_descr -> Lwt_unix.sockaddr -> int -> t

end = struct
  type t = {
    tx_id : int;
    sid : SessionId.t;
    socket : Lwt_unix.file_descr;
    remote_addr : Lwt_unix.sockaddr;
    rbuf: IOBuf.t;
    wbuf: IOBuf.t;
    rlenbuf: Lwt_bytes.t;
    wlenbuf: IOBuf.t;
    mutable close : unit -> unit Lwt.t;
    mutable send : Message.t -> unit Lwt.t
  }

  let make tx_id socket remote_addr buf_len  = {
    tx_id;
    sid = SessionId.next_id ();
    socket;
    remote_addr;
    rbuf = Result.get @@ IOBuf.create buf_len;
    wbuf = Result.get @@ IOBuf.create buf_len;
    rlenbuf = Lwt_bytes.create framing_buf_len;
    wlenbuf = Result.get @@ IOBuf.create framing_buf_len;
    close = (fun () -> return_unit);
    send = (fun _ -> return_unit)
  }
end
