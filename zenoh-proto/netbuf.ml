(* open Ztypes *)

module IOBuf = struct
  type t = {
    buffer : Lwt_bytes.t;
    limit : int;
  }

  let create len = { buffer = Lwt_bytes.create len;  limit = 0 }

  let put_char buf c =
    Lwt_bytes.set buf.buffer buf.limit c ;
    {buf with limit = buf.limit+1}


  let put_vle buf _ = buf
    
  let put_bytes buf _ = buf

  let put_buf dest _ = dest
end
