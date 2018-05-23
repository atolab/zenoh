open Zendpoint
open Apero

module TransportProtoInfo : sig 
  type kind = Packet| Stream  
  type t
  val make : string -> int -> bool -> kind -> int option -> t
  val name : t -> string
  val id : t -> int
  val reliable : t -> bool
  val kind : t -> kind
  val mtu : t -> int option
  val compare : t -> t -> int
end = struct 
  type kind = Packet | Stream
  type t = { name : string; id : int; reliable : bool; kind : kind ; mtu : int option } 

  let make name id reliable kind mtu = {name; id; reliable; kind; mtu}
  let name tp = tp.name 
  let id tp = tp.id
  let reliable tp = tp.reliable
  let kind tp = tp.kind
  let mtu tp = tp.mtu
  let compare a b = compare a b
end

module IpTransportProto : sig
  type t
  val tcp : t
  val udp : t
  val transports : TransportProtoInfo.t list
  val name : t -> string
  val id : t -> int
  val supports : string -> bool  
  val compare : t -> t -> int
  val info : t -> TransportProtoInfo.t
  val of_string : string -> t option
  val to_string : t -> string
end = struct 
  type t = TransportProtoInfo.t
  let tcp = TransportProtoInfo.make "tcp" 0 true TransportProtoInfo.Stream None 
  let udp = TransportProtoInfo.make "udp" 1 true TransportProtoInfo.Stream (Some 65536)
  let transports = [tcp; udp]
  let name tx = TransportProtoInfo.name tx
  let id tx = TransportProtoInfo.id tx
  let supports p = match List.find_opt (fun tx -> p = TransportProtoInfo.name tx) transports with 
  | Some _ -> true | None -> false
  let compare a b = TransportProtoInfo.compare a b
  let info tp = tp
  let of_string s =match s with 
  | "udp" -> Some udp
  | "tcp" -> Some tcp
  | _ -> None
  let to_string tpi = TransportProtoInfo.name tpi
end

module IpLocator : sig 
  type t 
  val make : IpTransportProto.t -> IpEndpoint.t -> t
  val transport_proto : t -> IpTransportProto.t
  val endpoint : t -> IpEndpoint.t
  val of_string : string -> t option
  val to_string : t -> string 
  val compare : t -> t -> int
end = struct 
  type t = { transport : IpTransportProto.t; endpoint : IpEndpoint.t }
  let make transport endpoint = {transport; endpoint}
  let transport_proto loc = loc.transport
  let endpoint loc = loc.endpoint

  let compare a b = 0
  let of_string s = 
  let open OptionM.Infix in  
  String.index_opt s '/' 
  >>= (fun idx ->
        let p = String.sub s 0 idx in
        let nidx = idx + 1 in 
        let len = String.length s in
        let ep = String.sub s nidx (len - nidx) in 
        IpTransportProto.of_string p
        >>= (fun proto -> 
             IpEndpoint.of_string ep 
             >>= (fun endpoint -> Some (make proto endpoint))))
    

  let to_string ep = Printf.sprintf "%s/%s" (IpTransportProto.to_string ep.transport) (IpEndpoint.to_string ep.endpoint)
end 

module Locator = struct
  (** This will become a union time once we'll support non IP-based transpots *)
  type t = IpLocator.t
    
  let to_string = IpLocator.to_string  

  let of_string = IpLocator.of_string
    
end

module Locators = struct
  type t = Locator.t list
  let empty = []
  let singleton l = [l]
  let add l ls = l::ls
  let length ls = List.length ls
end
