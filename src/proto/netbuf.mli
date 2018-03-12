open Ztypes

module IOBuf : sig
  type t

  val create : int -> t
  val put_char : t -> char -> t
  val put_vle : t -> Vle.t -> t
  val put_bytes : t -> bytes -> t

  val put_buf : t -> t -> t

end
