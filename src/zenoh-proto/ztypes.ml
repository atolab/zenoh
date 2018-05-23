
module Error = struct
  type kind = NoMsg | Msg of string | Code of int | Pos of (string * int * int * int) | Loc of string

  type e = ..
  type e +=
    | OutOfBounds of kind
    | OutOfRange of kind
    | IOError of kind
    | ClosedSession of kind
    | InvalidFormat of kind
    | ProtocolError of kind
    | InvalidSession of kind
    | NotImplemented
    | ErrorStack of e list
end

exception ZError of Error.e

(* module Result = Apero.Result.Make(Error) *)


module Vle = struct
  include Int64

  let of_char =
    let open Apero in
    Int64.of_int <.> int_of_char

  let byte_mask =  0x7fL
  let more_bytes_flag = 0x80L
  let shift_len = 7
  let max_bits = 64
  let max_bytes = 10
end
