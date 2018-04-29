open Ztypes
open Zenoh_pervasives

module IOBuf : sig
  type t

  type error =
  | InvalidFormat
  | OutOfRangeVle
  | OutOfRangeGet of int * int
  | OutOfRangePut of int * int

  val create : int -> (t, error) result
  val flip: t -> (t, error) result
  val clear: t -> (t, error) result

  val put_char : t -> char -> (t, error) result
  val get_char : t -> (char * t, error) result

  val put_vle : t -> Vle.t -> (t, error) result
  val get_vle : t -> (Vle.t * t, error) result

  val put_bytes : t -> bytes -> (t, error) result

  val put_buf : t -> t -> (t, error) result

end
