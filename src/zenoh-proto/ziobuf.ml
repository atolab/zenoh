open Apero
open Ztypes
open Lwt

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

  let create len = return { buffer = Lwt_bytes.create len;  pos = 0; limit = len; capacity = len; mark = 0 }

  let to_bytes buf = buf.buffer

  let from_bytes bs =
    let len = Lwt_bytes.length bs in
    return { buffer = bs; pos =  0; limit = len; capacity = len; mark = 0 }

  let flip buf = return { buf with limit = buf.pos; pos = 0 }

  let clear buf = return { buf with limit = buf.capacity; pos = 0 }

  let rewind buf = return { buf with pos = 0 }

  let capacity buf = buf.capacity

  let mark buf = return { buf with mark = buf.pos }

  let reset buf = return { buf with pos = buf.mark; mark = 0 }

  let get_position buf = buf.pos

  let set_position buf pos =
    if pos >=0 && pos <= buf.limit
    then return { buf with pos = pos }
    else fail @@ ZError Error.(OutOfBounds NoMsg)

  let get_limit buf = buf.limit

  let set_limit buf lim =
    if lim >= buf.pos && lim <= buf.capacity
    then return { buf with limit = lim}
    else fail @@ ZError Error.(OutOfBounds NoMsg)

  let reset_with buf pos lim =
    let%lwt b = set_position buf pos in set_limit buf lim


  let put_char buf c =
    if buf.pos < buf.limit then
      begin
        Lwt_bytes.set buf.buffer buf.pos c
      ; return { buf with pos = buf.pos + 1}
      end
    else
      fail @@ ZError Error.(OutOfBounds NoMsg)

  let get_char buf =
    if buf.pos < buf.limit then
      begin
        let c = Lwt_bytes.get buf.buffer buf.pos in
        return (c, {buf with pos = buf.pos+1})
      end
    else fail @@ ZError Error.(OutOfBounds NoMsg)

  let put_vle buf v =
    let to_char l = char_of_int @@ Int64.to_int l in
    let rec put_negative_vle_rec buf v n =
      if n < Vle.max_bytes then
        begin
          let mv = Int64.logor Vle.more_bytes_flag (Int64.logand v Vle.byte_mask) in
          let%lwt b = (put_char buf  (to_char mv)) in
          let sv = (Int64.shift_right v Vle.shift_len) in
          put_negative_vle_rec b sv (n+1)
        end
      else
        put_char buf  (to_char 1L)
    in
    let rec put_positive_vle_rec buf v =
      if v <= Vle.byte_mask then put_char buf (to_char v)
      else
        begin
          let mv = Int64.logor Vle.more_bytes_flag @@ Int64.logand v Vle.byte_mask in
          let%lwt b = (put_char buf @@ to_char mv) in
          let sv = Int64.shift_right v Vle.shift_len in
          put_positive_vle_rec b sv
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
          let%lwt (c, buf) = get_char buf in
          if (from_char c) <= Vle.byte_mask then return ((merge v (masked_from_char c) n), buf)
          else get_vle_rec buf (merge v (masked_from_char c) n) (n+1)
        end
      else
        begin
          let rec skip buf k  =
            let%lwt (c, buf) = get_char buf in
            if from_char c <= Vle.byte_mask then fail @@ ZError Error.(OutOfBounds NoMsg)
            else skip buf (k+1)
          in skip buf n
        end
    in get_vle_rec buf 0L 0

  let put_string buf s =
    let len = String.length s in
    let%lwt buf = put_vle buf @@ Vle.of_int len in
    Lwt_bytes.blit_from_bytes (Bytes.of_string s) 0 buf.buffer buf.pos len
    ; return { buf with pos = buf.pos + len }

  let get_string buf =
    let%lwt (vlen, buf) = get_vle buf in
    let len =  Vle.to_int vlen in
    let s = Bytes.create len in
    Lwt_bytes.blit_to_bytes buf.buffer buf.pos s 0 len
    ; return (Bytes.to_string s, {buf with pos = buf.pos + len })

  let blit_from_bytes bs ofs buf len =
    if buf.pos + len < buf.limit then
      begin
        Lwt_bytes.blit bs ofs buf.buffer buf.pos len
      ; return { buf with pos = buf.pos + len }
      end
    else
      fail @@ ZError Error.(OutOfBounds NoMsg)


  let blit src dst =
    let len = src.limit - src.pos in
    if dst.pos + len < dst.limit then
      begin
        Lwt_bytes.blit src.buffer src.pos dst.buffer dst.pos len ;
        let r = { dst with pos = dst.pos + len } in
        return r
      end
    else
      fail @@ ZError Error.(OutOfBounds NoMsg)

  let put_io_buf dst src  =
    if src.limit - src.pos  < dst.limit - dst.pos then
      begin
        let%lwt dst = put_vle dst @@ (Vle.of_int @@ (src.limit - src.pos)) in
         blit src dst
      end
    else  fail @@ ZError Error.(OutOfBounds NoMsg)

  let get_io_buf buf =
    let%lwt (len, buf) = get_vle buf  in
    let%lwt dst = create (Vle.to_int len) in
    Lwt_bytes.blit buf.buffer (buf.pos) dst.buffer 0 (Vle.to_int len)
    ; let%lwt buf = set_position buf (buf.pos + (Vle.to_int len)) in
    let%lwt dst = set_limit dst (Vle.to_int len) in
      return (dst, buf)

  let to_io_vec buf =
    Lwt_bytes.{ iov_buffer = buf.buffer; iov_offset = buf.pos; iov_length = buf.limit; }

  let read sock buf = Lwt_bytes.read sock (to_bytes buf) (get_position buf) (get_limit buf)

  let write sock buf = Lwt_bytes.write sock (to_bytes buf) (get_position buf) (get_limit buf)

  let recv ?(flags=[]) sock buf =
    match%lwt Lwt_bytes.recv sock (to_bytes buf) (get_position buf) (get_limit buf) flags with
    | 0 -> fail @@ ZError Error.(ClosedSession NoMsg)
    | n -> return n


  let send ?(flags=[]) sock buf = Lwt_bytes.send sock (to_bytes buf) (get_position buf) (get_limit buf) flags

  let recvfrom ?(flags=[]) sock buf =
    match%lwt Lwt_bytes.recvfrom sock (to_bytes buf) (get_position buf) (get_limit buf) flags with
    | (0, _) -> fail @@ ZError Error.(ClosedSession NoMsg)
    | _ as r -> return r

  let sendto ?(flags=[]) sock buf addr = Lwt_bytes.sendto sock (to_bytes buf) (get_position buf) (get_limit buf) flags addr

  type io_vector = Lwt_bytes.io_vector

  let io_vector buf =
    Lwt_bytes.{ iov_buffer = buf.buffer; iov_offset = buf.pos; iov_length = buf.limit; }

  let recv_vec sock bs =
    let iovec = List.map (fun buf -> io_vector buf) bs in
    match%lwt Lwt_bytes.recv_msg sock iovec with
    | (0, _) -> fail @@ ZError Error.(ClosedSession NoMsg)
    | _ as r -> return r

  let send_vec sock bs =
    let iovec = List.map (fun buf -> io_vector buf) bs in
    Lwt_bytes.send_msg sock iovec []

end
