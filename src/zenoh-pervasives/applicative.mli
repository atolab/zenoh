(** Applicative functors -- derived from ocaml-monadic *)

module type Base =
sig
  type 'a m

  val return : 'a -> 'a m
  val (<*>) : ('a -> 'b) m -> 'a m -> 'b m
end


module type Applicative =
sig
  include Base

  val lift1 : ('a -> 'b) -> 'a m -> 'b m
  val lift2 : ('a -> 'b -> 'c) -> 'a m -> 'b m -> 'c m
  val lift3 : ('a -> 'b -> 'c -> 'd) -> 'a m -> 'b m -> 'c m -> 'd m
  val lift4 : ('a -> 'b -> 'c -> 'd -> 'e) -> 'a m -> 'b m -> 'c m -> 'd m -> 'e m

  (** Alias for lift1. *)
  val (<$>) : ('a -> 'b) -> 'a m -> 'b m

  val sequence : 'a m list -> 'a list m
  val map_a : ('a -> 'b m) -> 'a list -> 'b list m
  val (<*) : 'a m -> 'b m -> 'a m
  val (>*) : 'a m -> 'b m -> 'b m
end

module Make(A : Base) : Applicative with type 'a m = 'a A.m

module Transform(A : Base)(Inner : Base) : Base with type 'a m = 'a Inner.m A.m
