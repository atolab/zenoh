open Iobuf


(** I/O related functions *)

val read : Lwt_unix.file_descr -> IOBuf.t -> int Lwt.t
(** [read] at most (limit -pos) bytes out of the file descriptior in to
    the IOBuf. Returns the  actual number of bytes read. *)


val write : Lwt_unix.file_descr -> IOBuf.t -> int Lwt.t
(** [write]  the bytes between {e pos} and {e limit}. Returns the number
    of bytes actually written. *)

val recv : ?flags:Unix.msg_flag list -> Lwt_unix.file_descr -> IOBuf.t -> int Lwt.t
(** [recv] receives at most (limit -pos) bytes out of the file descriptior
    in to the IOBuf. Returns the  actual number of bytes received.  *)

val send : ?flags:Unix.msg_flag list -> Lwt_unix.file_descr -> IOBuf.t -> int Lwt.t
(** [send] send the bytes between {e pos} and {e limit}. Returns the number
    of bytes actually sent. *)

val recvfrom : ?flags:Unix.msg_flag list -> Lwt_unix.file_descr -> IOBuf.t -> (int * Unix.sockaddr) Lwt.t

val sendto : ?flags:Unix.msg_flag list -> Lwt_unix.file_descr -> IOBuf.t -> Unix.sockaddr -> int Lwt.t

val recv_vec : Lwt_unix.file_descr -> IOBuf.t list -> (int * Unix.file_descr list) Lwt.t

val send_vec : Lwt_unix.file_descr -> IOBuf.t list -> int Lwt.t
