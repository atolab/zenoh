open Apero
open Ztypes

module IOBuf = struct

  type t = {
    buffer : Lwt_bytes.t;
    pos: int;
    limit : int;
    capacity: int;
    mark : int
  }

  let to_string buf =
    let rec hexa_print idx =
      if idx < buf.limit then (Printf.sprintf "%x:" @@ int_of_char @@ Lwt_bytes.get buf.buffer idx ) ^ (hexa_print (idx+1)) else ""
    in "(pos: " ^ (string_of_int buf.pos) ^ ", limit: "^ (string_of_int buf.limit) ^ " content: " ^ (hexa_print 0)

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
    then Result.ok { buf with pos = pos }
    else Result.fail Error.(OutOfBounds NoMsg)

  let get_limit buf = buf.limit

  let set_limit buf lim =
    if lim >= buf.pos && lim <= buf.capacity
    then Result.ok { buf with limit = lim}
    else Result.fail Error.(OutOfBounds NoMsg)


  let put_char buf c =
    if buf.pos < buf.limit then
      begin
        Lwt_bytes.set buf.buffer buf.pos c
        ; Result.ok { buf with pos = buf.pos + 1}
      end
    else
      Result.fail Error.(OutOfBounds NoMsg)

  let get_char buf =
    if buf.pos < buf.limit then
      begin
        let c = Lwt_bytes.get buf.buffer buf.pos in
        Result.ok (c, {buf with pos = buf.pos+1})
      end
    else Result.fail Error.(OutOfBounds NoMsg)

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
            ; if from_char c <= Vle.byte_mask then Result.fail Error.(OutOfBounds NoMsg) else skip buf (k+1)
          in skip buf n
        end
    in get_vle_rec buf 0L 0

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
      Result.fail Error.(OutOfBounds NoMsg)


  let blit src dst =
    let len = src.limit - src.pos in
    if dst.pos + len < dst.limit then
      begin
        Lwt_bytes.blit src.buffer src.pos dst.buffer dst.pos len ;
        let r = { dst with pos = dst.pos + len } in
        Result.ok r
      end
    else
      Result.fail Error.(OutOfBounds NoMsg)

  let put_io_buf dst src  =
    if src.limit - src.pos  < dst.limit - dst.pos then
      begin
        let open Result in
        (do_
        ; dst <-- put_vle dst @@ (Vle.of_int @@ (src.limit - src.pos))
        ; blit src dst )
      end
    else  Result.fail Error.(OutOfBounds NoMsg)

  let get_io_buf buf =
    let open Result in
    (do_
    ; (len, buf) <-- get_vle buf
    ; dst <-- create (Vle.to_int len)
    ; () ; Lwt_bytes.blit buf.buffer (buf.pos) dst.buffer 0 (Vle.to_int len)
    ; buf <-- set_position buf (buf.pos + (Vle.to_int len))
    ; dst <-- set_limit dst (Vle.to_int len)
    ; return (dst, buf))

  let to_io_vec buf =
    Lwt_bytes.{ iov_buffer = buf.buffer; iov_offset = buf.pos; iov_length = buf.limit; }

  let read sock buf = Lwt_bytes.read sock (to_bytes buf) (get_position buf) (get_limit buf)

  let write sock buf = Lwt_bytes.write sock (to_bytes buf) (get_position buf) (get_limit buf)

  let recv ?(flags=[]) sock buf = Lwt_bytes.recv sock (to_bytes buf) (get_position buf) (get_limit buf) flags

  let send ?(flags=[]) sock buf = Lwt_bytes.send sock (to_bytes buf) (get_position buf) (get_limit buf) flags

  let recvfrom ?(flags=[]) sock buf = Lwt_bytes.recvfrom sock (to_bytes buf) (get_position buf) (get_limit buf) flags

  let sendto ?(flags=[]) sock buf addr = Lwt_bytes.sendto sock (to_bytes buf) (get_position buf) (get_limit buf) flags addr

  type io_vector = Lwt_bytes.io_vector

  let io_vector buf =
    Lwt_bytes.{ iov_buffer = buf.buffer; iov_offset = buf.pos; iov_length = buf.limit; }

  let recv_vec sock iovec = Lwt_bytes.recv_msg sock iovec

  let send_vec sock iovec = Lwt_bytes.send_msg sock iovec []

end
