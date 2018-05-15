open Apero
open Ztypes


(** The buffer used by zenoh used for I/O. This buffer has a position, a limit
    a capacity and potentially a mark. At any point in time the following
    invariant will hold 0 <= mark <= pos <= limit <= capacity.

    A buffer's limit is the index of the first element that should not
    be read or written.

*)

module IOBuf : sig
  type t

  val create : int -> t Lwt.t
  (** [create] allocates a new IOBuf  of the given capacity. *)

  val to_bytes : t -> Lwt_bytes.t
  (** [to_bytes] provides the [Lwt_bytes.t] representation for this buffer so that
      it can be used for I/O such as sockets, etc... This buffer should be
      considered as read-only.
  *)

  val from_bytes : Lwt_bytes.t -> t Lwt.t
  (** [from_bytes] creates an IOBuf by wrapping the provided [Lwt_bytes.t].
      The capacity for the IOBuf will be set to the buffer length.
  *)

  val flip : t -> t Lwt.t
  (** [flip] sets the limit to the current position and the position to zero. *)

  val clear : t -> t Lwt.t
  (** [clear] sets the position to zero and the limit to the capacity. *)

  val rewind : t -> t Lwt.t
  (** [rewind] makes the buffer ready to be read again by setting the position
      to zero and keeping the limit as it is.
  *)

  val get_position : t -> int
  val set_position : t -> int -> t Lwt.t

  val get_limit : t -> int
  val set_limit : t -> int -> t Lwt.t

  val capacity : t -> int

  val mark : t -> t Lwt.t
  val reset : t -> t Lwt.t
  val reset_with : t -> int -> int -> t Lwt.t

  val put_char : t -> char -> t Lwt.t
  val get_char : t -> (char * t) Lwt.t

  val put_vle : t -> Vle.t -> t Lwt.t
  val get_vle : t -> (Vle.t * t) Lwt.t

  val put_string : t -> string -> t Lwt.t

  val get_string : t -> (string * t) Lwt.t

  val put_io_buf : t -> t -> t Lwt.t

  val get_io_buf : t -> (t * t) Lwt.t

  val blit_from_bytes : Lwt_bytes.t -> int -> t -> int -> t Lwt.t

  val blit : t -> t -> t Lwt.t

  val to_io_vec : t -> Lwt_bytes.io_vector

  val to_string : t -> string

(** I/O related functions *)


  val read : Lwt_unix.file_descr -> t -> int Lwt.t
(** [read] at most (limit -pos) bytes out of the file descriptior in to
    the IOBuf. Returns the  actual number of bytes read. *)


  val write : Lwt_unix.file_descr -> t -> int Lwt.t
(** [write]  the bytes between {e pos} and {e limit}. Returns the number
    of bytes actually written. *)

  val recv : ?flags:Unix.msg_flag list -> Lwt_unix.file_descr -> t -> int Lwt.t
(** [recv] receives at most (limit -pos) bytes out of the file descriptior
    in to the IOBuf. Returns the  actual number of bytes received.  *)

  val send : ?flags:Unix.msg_flag list -> Lwt_unix.file_descr -> t -> int Lwt.t
  (** [send] send the bytes between {e pos} and {e limit}. Returns the number
      of bytes actually sent. *)

  val recvfrom : ?flags:Unix.msg_flag list -> Lwt_unix.file_descr -> t -> (int * Unix.sockaddr) Lwt.t

  val sendto : ?flags:Unix.msg_flag list -> Lwt_unix.file_descr -> t -> Unix.sockaddr -> int Lwt.t

  type io_vector

  val io_vector : t -> io_vector

  val recv_vec : Lwt_unix.file_descr -> t list -> (int * Unix.file_descr list) Lwt.t

  val send_vec : Lwt_unix.file_descr -> t list -> int Lwt.t


end
