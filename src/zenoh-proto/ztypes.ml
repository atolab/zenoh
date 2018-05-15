
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

module Result = Apero.Result.Make(Error)


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

  let to_list v =
    let to_list_negative v =
      let rec to_list_negative_rec v xs n =
        if n < max_bytes then
          begin
            let mv = Int64.logand v byte_mask in
            let sv = Int64.shift_right v shift_len in
            to_list_negative_rec sv (mv::xs) (n+1)
          end
        else List.rev (1L :: xs)
      in to_list_negative_rec v [] 1
    in
    let to_list_positive v =
      let rec to_list_positive_rec v xs =
        if v <= byte_mask then List.rev (v::xs)
        else
          begin
            let mv = Int64.logand v byte_mask in
            let sv = Int64.shift_right v shift_len in
            to_list_positive_rec sv (mv::xs)
          end
      in to_list_positive_rec v []
    in
    if v >= Int64.zero then to_list_positive v
    else to_list_negative v


  let from_list xs =
    if List.length xs > max_bytes then Result.fail Error.(OutOfRange NoMsg)
    else
      begin
        let rec from_list_rec v xs n =
          if n <= max_bytes then
            match xs with
            | y::ys ->
              let nv = Int64.logor (Int64.shift_left y (n* shift_len)) v in
              from_list_rec nv ys (n+1)
            | [] -> Result.ok v
          else Result.fail Error.(OutOfRange NoMsg)
        in from_list_rec Int64.zero xs 0
      end
end
