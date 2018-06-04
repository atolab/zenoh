open Ztypes
open Apero.ResultM
open Apero.ResultM.InfixM

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
      if idx < buf.limit then 
        (Printf.sprintf "%x:" @@ int_of_char @@ Lwt_bytes.get buf.buffer idx ) ^ (hexa_print (idx+1)) 
      else ""
    in "(pos: " ^ (string_of_int buf.pos) ^ ", limit: "^ (string_of_int buf.limit) ^ " content: " ^ (hexa_print 0)

  let create len =  { buffer = Lwt_bytes.create len;  pos = 0; limit = len; capacity = len; mark = 0 }

  let to_bytes buf = buf.buffer

  let from_bytes bs =
    let len = Lwt_bytes.length bs in
    { buffer = bs; pos =  0; limit = len; capacity = len; mark = 0 }

  let flip buf = { buf with limit = buf.pos; pos = 0 }

  let clear buf = { buf with limit = buf.capacity; pos = 0 }

  let rewind buf =  { buf with pos = 0 }

  let capacity buf = buf.capacity

  let mark buf = { buf with mark = buf.pos }

  let reset buf =  { buf with pos = buf.mark; mark = 0 }

  let position buf = buf.pos

  let set_position pos buf  =
    if pos >=0 && pos <= buf.limit
    then  return { buf with pos = pos }
    else fail Error.(OutOfBounds NoMsg)

  let limit buf = buf.limit

  let available buf = (buf.limit - buf.pos)

  let set_limit lim buf  =
    if lim >= buf.pos && lim <= buf.capacity
    then return { buf with limit = lim}
    else fail Error.(OutOfBounds NoMsg)

  let reset_with pos lim buf =
    set_position pos buf
    >>= (set_limit lim)

  let put_char c buf =
    if buf.pos < buf.limit then
      begin
        Lwt_bytes.set buf.buffer buf.pos c
      ; return { buf with pos = buf.pos + 1}
      end
    else
      fail Error.(OutOfBounds NoMsg)

  let get_char buf =
    if buf.pos < buf.limit then
      begin
        let c = Lwt_bytes.get buf.buffer buf.pos in
        return (c, {buf with pos = buf.pos+1})
      end
    else fail Error.(OutOfBounds NoMsg)

  let blit_from_bytes bs ofs len  buf =
    if buf.pos + len < buf.limit then
      begin
        Lwt_bytes.blit bs ofs buf.buffer buf.pos len
      ; return { buf with pos = buf.pos + len }
      end
    else
      fail Error.(OutOfBounds NoMsg)

  let blit_to_bytes n buf = 
    if n <= available buf then 
      begin 
        let bs = Lwt_bytes.create n in         
        Lwt_bytes.blit buf.buffer (position buf) bs 0 n 
        ; return (bs, { buf with pos = buf.pos + n })
      end
    else 
      fail Error.(OutOfBounds NoMsg)

  (** Copies  [b.limit - b.pos] bytes from the [src] into [buf]*)
  let put_buf src buf  =
    let len = src.limit - src.pos in
    if  len <= (available buf) then
      begin
        Lwt_bytes.blit src.buffer src.pos buf.buffer buf.pos len ;
        return { buf with pos = buf.pos + len}
      end
    else
      fail Error.(OutOfBounds NoMsg)
    
  type io_vector = Lwt_bytes.io_vector

  let get_buf len buf = 
    if len <= available buf then 
      let dst = create len in 
      Lwt_bytes.blit buf.buffer buf.pos dst.buffer 0 len ;
      return ({buf with pos = buf.pos + len}, dst)
    else 
      fail Error.(OutOfBounds NoMsg)
  let to_io_vector buf =
    Lwt_bytes.{ iov_buffer = buf.buffer; iov_offset = buf.pos; iov_length = buf.limit; }

  let put_string s buf =     
    let len = String.length s in
    if len <= available buf then
      begin
        Lwt_bytes.blit_from_bytes (Bytes.of_string s) 0 buf.buffer buf.pos len;
        return { buf with pos = buf.pos + len }
      end 
    else 
      fail Error.(OutOfBounds NoMsg)

  
  let get_string len buf = 
    if len <= available buf then
      begin
        let s = Bytes.create len in
        Lwt_bytes.blit_to_bytes buf.buffer buf.pos s 0 len;
        return { buf with pos = buf.pos + len }
      end 
    else 
      fail Error.(OutOfBounds NoMsg)
end
