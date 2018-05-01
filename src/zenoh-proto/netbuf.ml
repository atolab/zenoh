open Zenoh_pervasives
open Ztypes

module IOBuf = struct
  type t = {
    buffer : Lwt_bytes.t;
    pos: int;
    limit : int;
    capacity: int;
    mark : int
  }

  type error =
    | InvalidFormat
    | InvalidPosition
    | InvalidLimit
    | OutOfRangeVle of int64 * int
    | OutOfRangeGet of int * int
    | OutOfRangePut of int * int

  let create len = Result.ok { buffer = Lwt_bytes.create len;  pos = 0; limit = len; capacity = len; mark = 0 }

  let to_bytes buf = buf.buffer

  let from_bytes bs =
    let len = Lwt_bytes.length bs in
    Result.ok { buffer = bs; pos =  0; limit = len; capacity = len; mark = 0 }

  let flip buf = Result.ok { buf with limit = buf.pos; pos = 0 }

  let clear buf = Result.ok { buf with limit = buf.capacity; pos = 0 }

  let rewind buf = Result.ok { buf with pos = 0 }

  let capacity buf = buf.capacity

  let mark buf = Result.ok { buf with mark = buf.pos }

  let reset buf = Result.ok { buf with pos = buf.mark; mark = 0 }


  let get_position buf = buf.pos

  let set_position buf pos =
    if pos >=0 && pos <= buf.limit
    then Result.ok { buf with pos = buf.pos }
    else Result.error InvalidPosition

  let get_limit buf = buf.limit

  let set_limit buf lim =
    if lim >= buf.pos && lim <= buf.capacity
    then Result.ok { buf with limit = lim}
    else Result.error InvalidLimit


  let put_char buf c =
    if buf.pos < buf.limit then
      begin
        Lwt_bytes.set buf.buffer buf.pos c
        ; Result.ok { buf with pos = buf.pos + 1}
      end
    else
      Result.error @@ OutOfRangePut (buf.pos, buf.limit)

  let get_char buf =
    if buf.pos < buf.limit then
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

  let get_vle buf =
    let from_char c = Vle.of_int (int_of_char c) in
    let masked_from_char c = Vle.logand Vle.byte_mask  (Vle.of_int (int_of_char c)) in
    let merge v c n = Vle.logor v (Vle.shift_left c (n * Vle.shift_len)) in
    let rec get_vle_rec buf v n =
      if n < Vle.max_bytes then
        begin
          Result.do_
          ; (c, buf) <-- get_char buf
          ; if (from_char c) <= Vle.byte_mask then return ((merge v (masked_from_char c) n), buf)
            else get_vle_rec buf (merge v (masked_from_char c) n) (n+1)
        end
      else
        begin
          let rec skip buf k  =
            Result.do_
            ; (c, buf) <-- get_char buf
            ; if from_char c <= Vle.byte_mask then Result.error @@ OutOfRangeVle (v, k) else skip buf (k+1)
          in skip buf n
        end
    in get_vle_rec buf 0L 0

  (* val put_string : t -> string -> (t, error) result

  val get_string : t -> (string * t, error) result *)

  let put_string buf s =
    let len = String.length s in
    Result.do_
    ; buf <-- put_vle buf @@ Vle.of_int len
    ; () ; Lwt_bytes.blit_from_bytes (Bytes.of_string s) 0 buf.buffer buf.pos len
    ; return { buf with pos = buf.pos + len }

  let get_string buf =
    Result.do_
    ; (vlen, buf) <-- get_vle buf
    ; len <-- return @@ Vle.to_int vlen
    ; s <-- return @@ Bytes.create len
    ; () ; Lwt_bytes.blit_to_bytes buf.buffer buf.pos s 0 len
    ; Result.ok (Bytes.to_string s, {buf with pos = buf.pos + len })

  let blit_from_bytes bs ofs buf len =
    if buf.pos + len < buf.limit then
      begin
        Lwt_bytes.blit bs ofs buf.buffer buf.pos len
        ; Result.ok { buf with pos = buf.pos + len }
        end
    else
      Result.error @@ OutOfRangePut (buf.pos + len, buf.limit)


  let blit src dst =
    let len = src.limit - src.pos in
    if dst.pos + len < dst.limit then
      begin
        Lwt_bytes.blit src.buffer src.pos dst.buffer dst.pos len
        ; Result.ok { dst with pos = dst.pos + len }
      end
    else
      Result.error @@ OutOfRangePut (dst.pos + len, dst.limit)

end
