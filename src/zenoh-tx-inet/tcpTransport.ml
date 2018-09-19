open Apero
open Apero_net 

module ZTcpTransport = NetServiceTcp.Make(MVar_lwt)
module ZTcpConfig = NetServiceTcp.TcpConfig



let ztcp_read_frame sock () =
  let lbuf = IOBuf.create 16 in 
  let%lwt len = Net.read_vle sock lbuf in          
  let buf = IOBuf.create (Vle.to_int len) in 
  let%lwt _ = Net.read sock buf in 
  match Mcodec.decode_frame buf with 
  | Ok (frame, _) -> Lwt.return frame
  | Error e -> Lwt.fail @@ Exception e
  
let ztcp_write_frane sock frame = (sock, frame)
