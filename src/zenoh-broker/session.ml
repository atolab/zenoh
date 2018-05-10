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


module InChannel : sig
  type t
  val create : Vle.t -> t
  val rsn : t -> Vle.t
  val usn : t -> Vle.t
  val update_rsn : t -> Vle.t -> unit
  val update_usn : t -> Vle.t -> unit
  val sn_max : t -> Vle.t
end = struct
  type t = { sn_max: Vle.t; mutable rsn : Vle.t; mutable usn : Vle.t }

  let create sn_max = {sn_max; rsn = 0L ; usn = 0L }
  let rsn c = c.rsn
  let usn c = c.usn
  let update_rsn c sn = c.rsn <- sn
  let update_usn c sn = c.usn <- sn
  let sn_max c = c.sn_max
end

module OutChannel : sig
  type t
  val create : Vle.t -> t
  val rsn : t -> Vle.t
  val usn : t -> Vle.t
  val sn_max : t -> Vle.t
  val next_rsn : t -> Vle.t
  val next_usn : t -> Vle.t

end = struct
  type t = {sn_max: Vle.t; mutable rsn : Vle.t; mutable usn : Vle.t}
  let create sn_max  = { sn_max; rsn = 0L; usn = 0L }
  let rsn c = c.rsn
  let usn c = c.usn
  let sn_max c = c.sn_max

  let next_rsn c =
    let n = c.rsn in
    c.rsn <- Vle.add c.rsn 1L ; n

  let next_usn c =
    let n = c.usn in
    c.usn <- Vle.add c.usn 1L ; n

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
    mutable send : Message.t -> unit Lwt.t;
    ic : InChannel.t;
    oc : OutChannel.t
  }
  val make : int -> Lwt_unix.file_descr -> Lwt_unix.sockaddr -> int -> t
  val in_channel : t -> InChannel.t
  val out_channel : t -> OutChannel.t

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
    mutable send : Message.t -> unit Lwt.t;
    ic : InChannel.t;
    oc : OutChannel.t
  }

  let make tx_id socket remote_addr buf_len  =
    let ic = InChannel.create Int64.(shift_left 1L 16) in
    let oc = OutChannel.create Int64.(shift_left 1L 16) in
    {
      tx_id;
      sid = SessionId.next_id ();
      socket;
      remote_addr;
      rbuf = Result.get @@ IOBuf.create buf_len;
      wbuf = Result.get @@ IOBuf.create buf_len;
      rlenbuf = Lwt_bytes.create framing_buf_len;
      wlenbuf = Result.get @@ IOBuf.create framing_buf_len;
      close = (fun () -> return_unit);
      send = (fun _ -> return_unit);
      ic;
      oc;
    }

  let in_channel s = s.ic
  let out_channel s = s.oc

end
