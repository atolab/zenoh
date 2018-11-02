open Apero
open Apero_net 
(* open Zenoh_proto *)

module ZTcpTransport = NetServiceTcp.Make(MVar_lwt)
module ZTcpConfig = NetServiceTcp.TcpConfig

(* TODO: the functions below should be implemented to really deal
   with frames of arbitrary lenght and do that efficiently. 
   One approach could be to use the provide buffer if the frame 
   to read/write fits, and otherwise to switch to another buffer *)

(*   
let run_ztcp_svc buf_size reader writer (svc:ZTcpTransport.t) (engine: ProtocolEngine.t) (sex: TxSession.t) = 
  let rbuf = IOBuf.create buf_size in 
  let wbuf = IOBuf.create buf_size in
  let socket = (TxSession.socket sex) in
  let zreader = reader  rbuf socket in 
  let zwriter = writer wbuf socket  in
  let push = ProtocolEngine.event_push engine in 
  fun () ->
    Lwt.bind 
      @@ zreader ()
      @@ fun frame -> Frame.to_list
       *)


