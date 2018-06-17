open Ztypes
open Iobuf
open Lwt

let read sock buf = Lwt_bytes.read sock (IOBuf.to_bytes buf) (IOBuf.position buf) (IOBuf.limit buf)

let write sock buf = Lwt_bytes.write sock (IOBuf.to_bytes buf) (IOBuf.position buf) (IOBuf.limit buf)

let recv ?(flags=[]) sock buf =
  match%lwt Lwt_bytes.recv sock (IOBuf.to_bytes buf) (IOBuf.position buf) (IOBuf.limit buf) flags with
  | 0 -> fail @@ ZError Error.(ClosedSession NoMsg)
  | n -> return n


let send ?(flags=[]) sock buf = 
  Lwt_bytes.send sock (IOBuf.to_bytes buf) (IOBuf.position buf) (IOBuf.limit buf) flags

let recvfrom ?(flags=[]) sock buf =
  match%lwt Lwt_bytes.recvfrom sock (IOBuf.to_bytes buf) (IOBuf.position buf) (IOBuf.limit buf) flags with
  | (0, _) -> fail @@ ZError Error.(ClosedSession NoMsg)
  | _ as r -> return r

let sendto ?(flags=[]) sock buf addr = 
  Lwt_bytes.sendto sock (IOBuf.to_bytes buf) (IOBuf.position buf) (IOBuf.limit buf) flags addr


  let recv_vec sock bs =
    let iovec = List.map (fun buf -> IOBuf.to_io_vector buf) bs in
    match%lwt Lwt_bytes.recv_msg sock iovec with
    | (0, _) -> fail @@ ZError Error.(ClosedSession NoMsg)
    | _ as r -> return r

  let send_vec sock bs =
    let iovec = List.map (fun buf -> IOBuf.to_io_vector buf) bs in
    Lwt_bytes.send_msg sock iovec []
