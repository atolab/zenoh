open Apero_net
open Transport


module type TcpTransportConfig = sig 
  val name : string
  val id : Transport.Id.t
  val locators :  TcpLocator.t list
  (** The list of locators at which connection will be accepted.  This is useful
      for multihomed machines that want to deal with traffic coming from different
      interfaces *)
  val backlog : int
  val bufsize : int
  val channel_bound : int
end

module TcpTransport : sig 
  module Make (C: TcpTransportConfig) : Transport.S
end

(* module UdpTransport : sig 
  module Make (C: IpTransportConfig) : Transport.S
end *)

val makeTcpTransport : ?bufsize:int -> ?backlog:int -> ?channel_bound:int -> TcpLocator.t list ->  (module Transport.S) Lwt.t

(* val makeUdpTransport : ?bufsize:int -> UdpLocator.t  -> (module Transport.S) Lwt.t *)
