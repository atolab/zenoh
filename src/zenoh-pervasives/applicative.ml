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

  val (<$>) : ('a -> 'b) -> 'a m -> 'b m

  val sequence : 'a m list -> 'a list m
  val map_a : ('a -> 'b m) -> 'a list -> 'b list m

  val (<*) : 'a m -> 'b m -> 'a m
  val (>*) : 'a m -> 'b m -> 'b m
end

module Make(A : Base) =
struct
  include A

  let (<$>) f x = return f <*> x

  let lift1 f x       = f <$> x
  let lift2 f x y     = f <$> x <*> y
  let lift3 f x y z   = f <$> x <*> y <*> z
  let lift4 f x y z w = f <$> x <*> y <*> z <*> w

  let (<*) x y = lift2 (fun x _ -> x) x y
  let (>*) x y = lift2 (fun _ y -> y) x y

  let rec sequence = function
    | []    -> return []
    | m::ms -> lift2 (fun x xs -> x :: xs) m (sequence ms)

  let map_a f xs = sequence (List.map f xs)
end

module Transform(A : Base)(Inner : Base) =
struct
  module A = Make(A)
  type 'a m = 'a Inner.m A.m
  let return x = A.return (Inner.return x)
  let (<*>) f x = A.lift2 Inner.(<*>) f x
end
