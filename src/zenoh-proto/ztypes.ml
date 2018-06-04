open Apero.InfixM
open Apero.ResultM
open Apero.ResultM.InfixM

module Error = struct
  type kind = NoMsg | Msg of string | Code of int | Pos of (string * int * int * int) | Loc of string [@@deriving show]

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
    [@@deriving show]
end

exception ZError of Error.e [@@deriving show]


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


let read1_spec log p1 c buf =   
  let open Apero.ResultM in
  let open Apero.ResultM.InfixM in
  log ;
  p1 buf 
  >>= (fun (a1, buf) -> 
      return ((c a1),buf))

let read2_spec log p1 p2 c buf =   
  log ;
  p1 buf 
  >>= (fun (a1, buf) -> 
    p2 buf 
    >>= (fun (a2, buf) ->
      return ((c a1 a2),buf)))

let read3_spec log p1 p2 p3 c buf = 
  log ;
  p1 buf 
  >>= (fun (a1, buf) -> 
    p2 buf 
    >>= (fun (a2, buf) ->
      p3 buf 
      >>= (fun (a3, buf) ->
      return ((c a1 a2 a3),buf))))

let read4_spec log p1 p2 p3 p4 c buf = 
  log ;
  p1 buf 
  >>= (fun (a1, buf) -> 
    p2 buf 
    >>= (fun (a2, buf) ->
      p3 buf 
      >>= (fun (a3, buf) ->
        p4 buf 
        >>= (fun (a4, buf) ->
        return ((c a1 a2 a3 a4),buf)))))

let read5_spec log p1 p2 p3 p4 p5 c buf = 
  log ;
  p1 buf 
  >>= (fun (a1, buf) -> 
    p2 buf 
    >>= (fun (a2, buf) ->
      p3 buf 
      >>= (fun (a3, buf) ->
        p4 buf 
        >>= (fun (a4, buf) ->
          p5 buf 
          >>= (fun (a5, buf) ->
        return ((c a1 a2 a3 a4 a5),buf))))))
