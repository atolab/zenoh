open Endpoint

module IpLocator : sig 
  module type S = sig
    type t 
    val make : IpEndpoint.t -> t
    val protocol : string
    val endpoint : t -> IpEndpoint.t
    val of_string : string -> t option
    val to_string : t -> string 
    val compare : t -> t -> int
    
  end

  module Make (T: sig val protocol : string end) : S

end 

module UdpLocator : IpLocator.S
module TcpLocator : IpLocator.S
