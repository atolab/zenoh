open Lwt
open Apero
open Message
open Channel 

let framing_buf_len = 16

module SessionId :  sig
  include (module type of Int64)
  val next_id : unit -> t
end = struct
  include Int64
  let session_count = ref 0L

  let next_id () =
    let r = !session_count in  session_count := add !session_count 1L ; r
end

