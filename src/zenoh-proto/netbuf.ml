open Zenoh_pervasives
open Ztypes

module IOBuf = struct
  type t = {
    buffer : Lwt_bytes.t;
    pos: int;
    limit : int;
    capacity: int;
  }

  type error =
    | InvalidFormat
    | OutOfRangeVle
    | OutOfRangeGet of int * int
    | OutOfRangePut of int * int

  let create len = Result.ok { buffer = Lwt_bytes.create len;  pos = 0; limit = len; capacity = len }

  let flip buf = Result.ok {buf with limit = buf.pos; pos = 0}

  let clear buf = Result.ok {buf with limit = buf.capacity; pos = 0}

  let put_char buf c =
    if (buf.pos < buf.limit -1 ) then
      begin
        Lwt_bytes.set buf.buffer buf.pos c
        ; Result.ok { buf with pos = buf.pos + 1}
      end
    else
      Result.error @@ OutOfRangePut (buf.pos, buf.limit)

  let get_char buf =
    if (buf.pos < buf.limit) then
      begin
        let c = Lwt_bytes.get buf.buffer buf.pos in
        Result.ok (c, {buf with pos = buf.pos+1})
      end
    else Result.error @@ OutOfRangeGet (buf.pos, buf.limit)

  let put_vle buf v =
    let to_char l = char_of_int @@ Int64.to_int l in
    let rec put_negative_vle_rec buf v n =
      if n < Vle.max_bytes then
        begin
          let mv = Int64.logor Vle.more_bytes_flag (Int64.logand v Vle.byte_mask) in
          Result.do_
          ; b <-- (put_char buf  (to_char mv))
          ; sv <-- return (Int64.shift_right v Vle.shift_len)
          ; put_negative_vle_rec b sv (n+1)
        end
      else
        put_char buf  (to_char 1L)
    in
    let rec put_positive_vle_rec buf v =
      if v <= Vle.byte_mask then put_char buf (to_char v)
      else
        begin
          let mv = Int64.logor Vle.more_bytes_flag @@ Int64.logand v Vle.byte_mask in
          Result.do_
          ; b <-- (put_char buf @@ to_char mv)
          ; sv <-- return @@ Int64.shift_right v Vle.shift_len
          ; put_positive_vle_rec b sv
        end
    in
    if v < 0L then put_negative_vle_rec buf v 1
    else put_positive_vle_rec buf v


  let put_bytes buf _ = Result.ok buf

  let put_buf dest _ = Result.ok dest
end
