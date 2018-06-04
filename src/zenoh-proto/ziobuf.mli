open Apero
open Ztypes


(** The buffer used by zenoh used for I/O. This buffer has a position, a limit
    a capacity and potentially a mark. At any point in time the following
    invariant will hold 0 <= mark <= pos <= limit <= capacity.

    A buffer's limit is the index of the first element that should not
    be read or written.

    This buffer was designed for performance, additionally the only way 
    a method may fail is if it violates one of the invariants for the 
    type and in this case an exception is raised as this would cleary 
    represent a bug in the application using the buffer. *)

module IOBuf : sig  
  type t      

  val create : int -> t
  (** [create] allocates a new IOBuf  of the given capacity. *)

  val to_bytes : t -> Lwt_bytes.t
  (** [to_bytes] provides the [Lwt_bytes.t] representation for this buffer so that
      it can be used for I/O such as sockets, etc... This buffer should be
      considered as read-only. *)

  val from_bytes : Lwt_bytes.t -> t 
  (** [from_bytes] creates an IOBuf by wrapping the provided [Lwt_bytes.t].
      The capacity for the IOBuf will be set to the buffer length. *)

  val flip : t -> t 
  (** [flip] sets the limit to the current position and the position to zero. *)

  val clear : t -> t
  (** [clear] sets the position to zero and the limit to the capacity. *)

  val rewind : t -> t
  (** [rewind] makes the buffer ready to be read again by setting the position
      to zero and keeping the limit as it is. *)

  val position : t -> int
  val set_position : int -> t -> (t, Error.e) result 

  val limit : t -> int
  val set_limit : int -> t ->  (t, Error.e) result 

  val capacity : t -> int

  (** remaining bytes to read/write overflowing *)
  val available : t -> int

  val mark : t -> t
  val reset : t -> t
  val reset_with : int -> int -> t -> (t, Error.e) result 

  val put_char : char -> t ->  (t, Error.e) result 
  val get_char : t -> ((char * t), Error.e) result 

  val blit_from_bytes : Lwt_bytes.t -> int -> int ->  t -> (t, Error.e) result 

  val blit_to_bytes : int -> t ->  ((Lwt_bytes.t * t), Error.e) result 

  val put_string : string -> t -> (t, Error.e) result 
  
  val get_string : int -> t -> (t, Error.e) result 

  val put_buf : t -> t -> (t, Error.e) result 

  val get_buf : int -> t -> (t*t, Error.e) result 

  val to_string : t -> string

  type io_vector = Lwt_bytes.io_vector

  val to_io_vector : t -> io_vector

end
