type ('a, 'b) result = ('a, 'b) Pervasives.result = Ok of 'a | Error of 'b

module Result = struct
  type ('a, 'b) t = ('a, 'b) result
  let ok v = Ok v
  let error e = Error e

  let return = ok
  let fail = error

  let bind v f = match v with Ok v -> f v | Error _ as e -> e
  let map f v = match v with Ok v -> Ok (f v) | Error _ as e -> e
  let join r = match r with Ok v -> v | Error _ as e -> e
  let ( >>= ) = bind
  let ( >>| ) v f = match v with Ok v -> Ok (f v) | Error _ as e -> e

  module Infix = struct
    let ( >>= ) = ( >>= )
    let ( >>| ) = ( >>| )
  end
end

(* val apply_n : 'a -> ('a -> 'b) -> int -> 'a list *)


let apply_n (t : 'a) (f : 'a -> 'b)  (n: int) =
  let rec loop_n n xs =
    if n = 1 then xs
    else loop_n (n-1) ((f t) :: xs)
  in loop_n n []


module ZList = struct
  let drop n xs =
    let rec adrop n xs = match n with
      | 0 -> xs
      | _ -> match xs with
        | [] -> []
        | _::tl -> adrop (n-1) tl
    in adrop n xs

  let take n xs =
    let rec atake n xs ys = match n with
      | 0 -> List.rev ys
      | _ -> match xs with
        | [] -> List.rev ys
        | h::tl -> atake (n-1) tl  (h::ys)
    in atake n xs []

end
