open Zetransport
open Zlocator
open Lwt

module type IpTransportConfig = sig 
  val name : string
  val id : Int64Id.t
  val locators :  Locators.t
  val buf_len : int
end

module TcpTransport = struct

  module Make (C: IpTransportConfig) = struct 
    let info = Transport.Info.make "tcp" 0 true Transport.Info.Stream None
    let stop () = return_unit
    
    let start () = return_unit
    let stop () = return_unit        
    let engine f = ()    
    let listen loc = return (Transport.Session.Id.create C.id (Int64Id.next_id ()))
    let connect loc = return (Transport.Session.Id.create C.id (Int64Id.next_id ()))
    
    let close sid = return_unit        
    let process evt = return_unit
    
    let send sid frame = return_unit
    let send_to loc frame = return_unit
    let session_info sid = None    
  end
end
