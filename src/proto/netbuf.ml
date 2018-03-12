open Ztypes

module IOBuf = struct
  type t = {
    buffer : bytes;
    limit : int;
  }

  let create len = { buffer = Bytes.create len;  limit = 0 }

  let put_char buf c =
    Bytes.set buf.buffer buf.limit c ;
    { buffer = buf.buffer; limit = buf.limit + 1 }

  let put_vle buf v = buf

  let put_bytes buf bs = buf

  let put_buf dest src = dest 
end
