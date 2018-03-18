(* open Ztypes *)

module IOBuf = struct
  type t = {
    buffer : bytes;
    limit : int;
  }

  let create len = { buffer = Bytes.create len;  limit = 0 }

  let put_char buf c =
    Bytes.set buf.buffer buf.limit c ;
    { buffer = buf.buffer; limit = buf.limit + 1 }

  let put_vle buf _ = buf

  let put_bytes buf _ = buf

  let put_buf dest _ = dest
end
