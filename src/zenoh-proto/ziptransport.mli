open Zetransport
open Zlocator

module type IpTransportConfig = sig 
  val name : string
  val id : Int64Id.t
  val locators :  Locators.t
  val buf_len : int
end

module TcpTransport : sig 
  module Make (C: IpTransportConfig) : Transport.S
end
