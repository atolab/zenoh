open Zendpoint
open Apero

module IpLocator = struct
  module type S = sig
    type t 
    val make : IpEndpoint.t -> t
    val protocol : string
    val endpoint : t -> IpEndpoint.t
    val of_string : string -> t option
    val to_string : t -> string 
    val compare : t -> t -> int
  end

  module Make (T: sig val protocol : string end) = struct
    type t = IpEndpoint.t
    let make endpoint = endpoint 
    let protocol = T.protocol
    let endpoint loc = loc
    let compare a b = 0
    let of_string s = 
    let open OptionM.Infix in  
    String.index_opt s '/' 
    >>= (fun idx ->          
          let proto = String.sub s 0 idx in          
          if proto = T.protocol then
            begin
              let nidx = idx + 1 in 
              let len = String.length s in
              let ep = String.sub s nidx (len - nidx) in               
              IpEndpoint.of_string ep
            end 
          else None)
      
    let to_string ep = Printf.sprintf "%s/%s" T.protocol (IpEndpoint.to_string ep)
  end

end 

module UdpLocator = IpLocator.Make(struct let protocol = "udp" end)
module TcpLocator = IpLocator.Make(struct let protocol = "tcp" end)
