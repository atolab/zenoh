(** Monads -- derived from ocaml-monads library *)
open Applicative

module type Monoid =
sig
  type t
  val zero : unit -> t
  val plus : t -> t -> t
end

module type MonadBase = sig
  type 'a m

  val return : 'a -> 'a m

  val bind : 'a m -> ('a -> 'b m) -> 'b m

end

(** Monads with additional monoid structure. *)
module type BasePlus =
sig
  include MonadBase
  val zero : unit -> 'a m
  val plus : 'a m -> 'a m -> 'a m

  (** null x implies that x is zero. If you do not want to or cannot
      answer whether a given x is zero, then null x should be false. I have
      provided this so that streams can be implemented more efficiently. *)
  val null : 'a m -> bool
end

module type Monad =
sig
  include MonadBase
  include Applicative with type 'a m := 'a m

  val (>>=) : 'a m -> ('a -> 'b m) -> 'b m
  val (>=>) : ('a -> 'b m) -> ('b -> 'c m) -> ('a -> 'c m)
  val (<=<) : ('b -> 'c m) -> ('a -> 'b m) -> ('a -> 'c m)
  val join  : 'a m m -> 'a m
  val filter_m : ('a -> bool m) -> 'a list -> 'a list m
  val onlyif : bool -> unit m -> unit m
  val unless : bool -> unit m -> unit m
  val ignore : 'a m -> unit m
end

(** Library functions for monads with additional monoid structure. *)
module type MonadPlus =
sig
  include BasePlus
  include Monad with type 'a m := 'a m

  val filter : ('a -> bool) -> 'a m -> 'a m
  val of_list : 'a list -> 'a m
  val sum  : 'a list m -> 'a m
  val msum : 'a m list -> 'a m
  val guard : bool -> unit m

  (** Generalises matrix transposition. This will loop infinitely if
      {! BasePlus.null} cannot answer [true] for [zero]es. *)
  val transpose : 'a list m -> 'a m list

end

(** {6 Library Creation} *)
module Make(M : MonadBase) : Monad with type 'a m = 'a M.m

module MakePlus (M : BasePlus) : MonadPlus with type 'a m = 'a M.m

module ListM : MonadPlus with type 'a m = 'a list

module Option : MonadPlus with type 'a m = 'a option

module type ResultS = sig
  type e
  include  Monad with type 'a m = ('a, e) result
  val ok : 'a -> 'a m
  val fail : e -> 'a m
end
(* 
module type ResultXchg = sig
  type e

  val xchg : ('a, 'b) result ->  ('b -> ('a, e) result) -> ('a, 'c) result
end

module ResultX (RA : ResultS) (RB : ResultS) : ResultXchg with type e := RA.e *)


module ResultM (E : sig type e end ) : ResultS with type e = E.e
(* sig
  include  Monad with type 'a m = ('a, E.e) result
  val ok : 'a -> 'a m
  val fail : E.e -> 'a m
end *)

module Retry(E : sig
    type e
    type arg
    type tag
    val defaultError : e
  end) :
sig
  include MonadPlus
  type 'a err = Error  of (E.tag * (E.arg -> 'a m)) list * E.e
              | Result of 'a
  val throw     : E.e -> 'a m
  val catch     : 'a m -> (E.e -> 'a m) -> 'a m
  val add_retry : E.tag -> (E.arg -> 'a m) -> 'a m -> 'a m
  val run_retry : 'a m -> 'a err
end

(** For the incorruptible programmer:

    A continuation of type 'a has type [('a -> 'r) -> 'r]

    The first argument to this continuation is a final value of type 'r which
    depends on some intermediate value of type 'a. In other words, we can
    understand the continuation as a result of type 'r which depends on the {i
    future} of an intermediate value.

    The function [return] just {i throws} its return value to the future in
    order to produce the final result. The [bind] intercedes in the future,
    inserting a computation.

    Call-with-current-continuation allows one to effectively reflect into one's own
    future. That is, callCC is a computation which depends on another computation
    taking the future as a first-class value. One can store this future, and at any
    time, throw it a return value to reinstate it.

    If you are into the Curry-Howard Isomorphism,
    call-with-current-continuation has a type which corresponds to a law of
    classical logic (Pierce's Law). Writing your code in the continuation
    monad corresponds to embedding classical logic intuitionistically. Allowing
    callCC corresponds to assuming a classical hypothesis.
*)
module Continuation(T : sig type r end) :
sig
  include Monad with type 'a m = ('a -> T.r) -> T.r

  val callCC : (('a -> 'r m) -> 'a m) -> 'a m
end

module Reader(T : sig type t end) :
sig
  include Monad

  val read : T.t m
  val run  : T.t -> 'a m -> 'a
end

module Writer(M : Monoid) :
sig
  include Monad

  val listen : 'a m -> (M.t * 'a) m
  val run   : 'a m -> M.t * 'a
  val write : M.t -> unit m
end

module State(T : sig type s end) :
sig
  include Monad

  val read   : T.s m
  val write  : T.s -> unit m
  val modify : (T.s -> T.s) -> unit m
  val run    : 'a m -> T.s -> (T.s * 'a)
  val eval    : 'a m -> T.s -> 'a
end

module Error(E : sig type e val defaultError : e end) :
sig
  type 'a err = Error  of E.e
              | Result of 'a
  include MonadPlus

  val throw : E.e -> 'a m
  val catch : 'a m -> (E.e -> 'a m) -> 'a m
  val run_error : 'a m -> 'a err
end
