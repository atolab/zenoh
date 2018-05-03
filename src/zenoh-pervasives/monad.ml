open Zenoh_pervasives

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

module type BasePlus =
sig
  include MonadBase
  val zero : unit -> 'a m
  val plus : 'a m -> 'a m -> 'a m
  val null : 'a m -> bool
end


module type Monad =
sig
  include MonadBase
  include Applicative.Applicative with type 'a m := 'a m

  val (>>=)    : 'a m -> ('a -> 'b m) -> 'b m
  val (>=>) : ('a -> 'b m) -> ('b -> 'c m) -> ('a -> 'c m)
  val (<=<) : ('b -> 'c m) -> ('a -> 'b m) -> ('a -> 'c m)
  val join     : 'a m m -> 'a m
  val filter_m : ('a -> bool m) -> 'a list -> 'a list m
  val onlyif : bool -> unit m -> unit m
  val unless : bool -> unit m -> unit m
  val ignore : 'a m -> unit m
end

module type MonadPlus =
sig
  include BasePlus
  include Monad with type 'a m := 'a m
  val filter  : ('a -> bool) -> 'a m -> 'a m
  val of_list : 'a list -> 'a m
  val sum       : 'a list m -> 'a m
  val msum      : 'a m list -> 'a m
  val guard     : bool -> unit m
  val transpose : 'a list m -> 'a m list
end


module Make(M : MonadBase) =
struct
  include M

  let (>>=)       = bind
  let (>=>) g f x = g x >>= f
  let (<=<) f g x = g x >>= f

  let lift1 f x     = x >>= fun x -> return (f x)
  let lift2 f x y   = x >>= fun x -> lift1 (f x) y

  module Ap =
    Applicative.Make(struct
      include M
      let (<*>) f x  = lift2 (fun f x -> f x) f x
    end)

  include (Ap : Applicative.Applicative with type 'a m := 'a m)

  let join     m = m >>= (fun x -> x)

  let filter_m p =
    let rec loop m = function
      | []      -> lift1 List.rev m
      | (x::xs) -> loop (p x >>=
                         fun b -> m >>=
                         fun ys -> return (if b then (x::ys) else ys)) xs in
    loop (return [])

  let ignore m   = lift1 (fun _ -> ()) m
  let onlyif b m = if b then m else return ()
  let unless b m = if b then return () else m
end

module MakePlus(M : BasePlus) =
struct
  include Make(M)
  let zero ()  = M.zero ()
  let plus     = M.plus
  let null     = M.null
  let filter p xs =
    xs >>= fun x -> if p x then return x else zero ()
  let of_list xs = List.fold_left
      (fun x y -> plus x (return y))
      (zero  ()) xs
  let sum xs =
    xs >>=
    (fun xs -> List.fold_right plus (List.map return xs) (zero ()))
  let msum xs = List.fold_left plus (zero ()) xs

  let guard b = if b then return () else zero ()

  let rec transpose xs =
    let hds = sum (lift1 (ZList.take 1) xs) in
    if null hds then [] else
      hds :: transpose (lift1 (ZList.drop 1) xs)
end

module Option =
  MakePlus(struct
    type 'a m = 'a option
    let return x  = Some x
    let bind x f  = match x with
        None   -> None
      | Some y -> f y
    let zero ()   = None
    let plus x y  =
      match x,y with
        None, x        -> x
      | x, None        -> x
      | Some x, Some _ -> Some x
    let null  = function
      | None -> true
      | _ -> false
  end)

(* module ResultM (E : sig type e end ) : MonadPlus with type 'a m = ('a, E.e) result *)

module type ResultS = sig
  type e
  include  Monad with type 'a m = ('a, e) result
  val ok : 'a -> 'a m
  val fail : e -> 'a m
end

(* module type ResultXchg = sig
  type e
  val xchg : ('a, 'b) result ->  ('b -> ('a, e) result) -> ('a, e) result
end *)

module ResultX (RA : ResultS) (RB : ResultS) = struct
  let lift_e r f = match r with
    | RA.(Ok v) -> RB.ok v
    | RA.(Error e) -> RB.fail (f e)
end

module ResultM (E: sig type e end) = struct
  type e = E.e

  include Make(struct
      type 'a m = ('a, E.e) result
      let return x = Ok x
      let bind x f = match x with
          Error  e -> Error e
        | Ok x -> f x
      let null x   = match x with
          Error _  -> true
        | _        -> false
    end )

  let ok = return

  let fail e = Error e

end

module OptionT(M : MonadBase) =
struct
  module M = Make(M)
  include Make(struct
      type 'a m = 'a option M.m
      let return x  = M.return (Some x)
      let bind xs f =
        M.bind xs
          (function
              None   -> M.return None
            | Some x -> f x)
    end)
  let lift x = M.lift1 (fun x -> Some x) x
end

module Error(E : sig type e val defaultError : e end) =
struct
  type 'a err = Error  of E.e
              | Result of 'a
  include MakePlus(struct
      type 'a m = 'a err
      let return x = Result x
      let bind x f = match x with
          Error  e -> Error e
        | Result x -> f x
      let zero ()  = Error E.defaultError
      let plus x y = match x with
          Error  _ -> y
        | Result x -> Result x
      let null x   = match x with
          Error _  -> true
        | _        -> false
    end)

  let throw e = Error e

  let catch x handler =
    match x with
      Error e  -> handler e
    | Result x -> return x

  let run_error err = err
end

module ErrorT(E : sig type e val defaultError : e end)(M : MonadBase) =
struct
  type 'a err = Error  of E.e
              | Result of 'a
  module M = Make(M)
  include MakePlus(struct
      type 'a m = 'a err M.m
      let return x = M.return (Result x)
      let bind x f =
        M.bind x (function
            | Result x -> f x
            | Error e  -> M.return (Error e))
      let zero () = M.return (Error E.defaultError)
      let plus x y =
        M.bind x (function
            | Error _ -> y
            | x       -> M.return x)
      let null _ = false
    end)

  let lift x  = M.lift1 (fun x -> Result x) x
  let throw e = M.return (Error e)
  let catch x handler =
    M.bind x (function
        | Error e -> handler e
        | x       -> M.return x)
  let run_error err = err
end

module Retry(E : sig
    type e
    type arg
    type tag
    val defaultError : e
  end) =
struct
  type 'a err = Error  of (E.tag * (E.arg -> 'a err)) list * E.e
              | Result of 'a
  include MakePlus(
    struct
      type 'a m = 'a err
      let return x = Result x
      let rec bind x f = match x with
        | Result x -> f x
        | Error (retries,e) ->
          Error (List.map (fun (t,r) -> t,fun arg -> bind (r arg) f) retries,e)
      let zero ()  = Error ([],E.defaultError)
      let plus x y = match x with
          Error  _ -> y
        | Result x -> Result x
      let null x   = match x with
          Error _  -> true
        | _        -> false
    end)

  let add_retry tag retry = function
    | Error (retries,e) -> Error ((tag,retry)::retries,e)
    | x                 -> x
  let throw e = Error ([],e)

  let catch x handler =
    match x with
    | Error (_,e) -> handler e
    | Result x    -> return x

  let run_retry err = err
end

module Continuation(T : sig type r end) = struct
  include Make(struct
      type 'a m = ('a -> T.r) -> T.r

      let return x k = k x
      let bind c f k =
        c (fun x -> (f x) k)
    end)
  let callCC kk k =
    kk (fun x -> fun _ -> k x) k
end


module State(T : sig type s end) =
struct
  include Make(struct
      type 'a m = T.s -> (T.s * 'a)
      let return x s  = (s, x)
      let bind xf f s =
        let s',x = xf s in
        f x s'
    end)
  let read s    = (s,s)
  let write x _ = (x,())
  let run x s   = x s
  let eval x s  = snd (x s)
  let modify f  = bind read (fun s -> write (f s))
end

module StateT(T : sig type s end)(M : MonadBase) =
struct
  module M = Make(M)
  include Make(struct
      type 'a m = T.s -> (T.s * 'a) M.m
      let return x s  = M.return (s, x)
      let bind xf f s =
        M.bind (xf s) (fun (s',x) -> (f x) s')
    end)
  let read  s   = M.return (s,s)
  let write x s = M.return (x,())
  let modify f  = bind read (fun s -> write (f s))
  let run   x s = x s
  let eval  x s = M.lift1 snd (x s)
  let lift x s  = M.lift1 (fun x -> (s,x)) x
end

module Reader(M : sig type t end) = struct
  include Make(struct
      type 'a m = M.t -> 'a
      let return x _ = x
      let bind r f e =
        f (r e) e
    end)
  let read     e = e
  let run e x    = x e
end

module Writer(M : Monoid) = struct
  include Make(struct
      type 'a m = M.t * 'a
      let return x     = M.zero (), x
      let bind (m,x) f =
        let m',y = f x in
        M.plus m m', y
    end)
  let listen (x,y) = (x,(x,y))
  let run (x,y)    = (x,y)
  let write x      = (x,())
end

module WriterT(Mon : Monoid)(M : MonadBase) =
struct
  module M = Make(M)
  module W = Writer(Mon)
  type t = Mon.t
  include Make(struct
      module WM = Make(W)
      type 'a m = 'a W.m M.m

      let return   x = M.return (W.return x)
      let bind x f =
        M.bind x (fun x ->
            let v,x = W.run x in
            M.lift1 (WM.lift2
                       (fun () y -> y)
                       (W.write v))
              (f x))
    end)

  let listen x = M.lift1 W.listen x
  let write x = M.return (W.write x)
  let run xs = M.lift1 (fun x -> let (v,x) = W.run x in
                         v,x) xs
  let lift x = M.lift1 W.return x
end


module ListM =
  MakePlus(struct
    type 'a m = 'a list
    let return x  = [x]
    let bind xs f = List.concat (List.map f xs)
    let zero () = []
    let plus xs ys = xs @ ys
    let null = function
        [] -> true
      | _  -> false
  end)
