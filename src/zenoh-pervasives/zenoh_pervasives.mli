type ('a, 'b) result = ('a, 'b) Pervasives.result = Ok of 'a | Error of 'b

module Result : sig

  type ('a, 'b) t = ('a, 'b) result

  val ok : 'a -> ('a, 'b) result
  val error : 'b -> ('a, 'b) result

  val return : 'a -> ('a, 'b) result
  val fail : 'b -> ('a, 'b) result

  val bind : ('a, 'b) result -> ('a -> ('c, 'b) result) -> ('c, 'b) result

  val map : ('a -> 'c) -> ('a, 'b) result -> ('c, 'b) result

  val join : (('a, 'b) result, 'b) result -> ('a, 'b) result

  val ( >>= ) : ('a, 'b) result -> ('a -> ('c, 'b) result) -> ('c, 'b) result

  val ( >>| ) : ('a, 'b) result -> ('a -> 'c) -> ('c, 'b) result

  module Infix : sig
    val ( >>= ) : ('a, 'b) result -> ('a -> ('c, 'b) result) -> ('c, 'b) result
    val ( >>| ) : ('a, 'b) result -> ('a -> 'c) -> ('c, 'b) result
  end
end

val apply_n : 'a -> ('a -> 'b) -> int -> 'b list
