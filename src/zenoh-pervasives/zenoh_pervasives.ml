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
