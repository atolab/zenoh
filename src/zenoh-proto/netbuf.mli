open Ztypes
open Zenoh_pervasives

(** The buffer used by zenoh used for I/O. This buffer has a position, a limit
    a capacity and potentially a mark. At any point in time the following
    invariant will hold 0 <= mark <= pos <= limit <= capacity.

    A buffer's limit is the index of the first element that should not
    be read or written.

*)

module IOBuf : sig
  type t

  type error =
    | InvalidFormat
    | InvalidPosition
    | InvalidLimit
    | OutOfRangeVle of int64 * int (** Provide the prefix than can be represented and the total number of bytes *)
    | OutOfRangeGet of int * int
    | OutOfRangePut of int * int


  val create : int -> (t, error) result
  (** [create] allocates a new IOBuf  of the given capacity. *)

  val to_bytes : t -> Lwt_bytes.t
  (** [to_bytes] provides the [Lwt_bytes.t] representation for this buffer so that
      it can be used for I/O such as sockets, etc... This buffer should be
      considered as read-only.
  *)

  val from_bytes : Lwt_bytes.t -> (t, error) result
  (** [from_bytes] creates an IOBuf by wrapping the provided [Lwt_bytes.t].
      The capacity for the IOBuf will be set to the buffer length.
  *)

  val flip : t -> (t, error) result
  (** [flip] sets the limit to the current position and the position to zero. *)

  val clear : t -> (t, error) result
  (** [clear] sets the position to zero and the limit to the capacity. *)

  val rewind : t -> (t, error) result
  (** [rewind] makes the buffer ready to be read again by setting the position
      to zero and keeping the limit as it is.
  *)

  val get_position : t -> int
  val set_position : t -> int -> (t, error) result

  val get_limit : t -> int
  val set_limit : t -> int -> (t, error) result

  val capacity : t -> int

  val mark : t -> (t, error) result
  val reset : t -> (t, error) result

  val put_char : t -> char -> (t, error) result
  val get_char : t -> (char * t, error) result

  val put_vle : t -> Vle.t -> (t, error) result
  val get_vle : t -> (Vle.t * t, error) result

  val put_string : t -> string -> (t, error) result

  val get_string : t -> (string * t, error) result

  val blit_from_bytes : Lwt_bytes.t -> int -> t -> int -> (t, error) result

  val blit : t -> t -> (t, error) result

end
