open Ztypes

(**  Module representing an IP endpoint. *)
module IpEndpoint = struct
  type t = { addr : Lwt_unix.inet_addr ; port : int }
  
  let addr e = e.addr

  let port e = e.port 

  let to_sockaddr e = Lwt_unix.ADDR_INET (e.addr, e.port)

  let of_sockaddr = function 
  | Lwt_unix.ADDR_INET (a, p) -> {addr = a; port = p}
  | _ -> raise @@ ZError Error.InvalidAddress


  let of_string s =     
    let ridx = String.rindex s ':' in
    let len = String.length s in
    let saddr = String.sub s 0 ridx in
    let idx = ridx + 1 in
    let port = int_of_string (String.sub s idx (len - idx)) in 
    Some { addr = Unix.inet_addr_of_string saddr ; port = port }
    
  let to_string e =
    let a = Unix.string_of_inet_addr e.addr in
    Printf.sprintf "%s:%d" a e.port

  let any port = { addr = Unix.inet_addr_any ; port = port }

  let any_v6 port = { addr = Unix.inet6_addr_any ; port = port }

  let with_port e = { e with port = 0 }

  let loopback port  = { addr = Unix.inet_addr_loopback ;  port = port }

  let loopback_v6 port  = { addr = Unix.inet6_addr_loopback ;  port = port }
  
  let is_ipv4 e = 
    let se = to_string e in 
    match String.index_opt se '.' with 
    | Some _ -> true 
    | None -> false

  let is_ipv6 e = not @@ is_ipv4 e

  let is_multicast e = 
    let is_multicast_v4 a = 
     let saddr = to_string a in 
      match (String.split_on_char '.' saddr) with
      | h::_ ->
        let a = int_of_string h  in
        if a >= 224 && a <= 239 then true else false
      | _ -> false
    in 
    let is_multicast_v6 a = 
      let saddr = to_string a in
      let idx = String.index saddr ':' in 
      let b = (int_of_string @@ String.sub saddr 0 idx) land 0xff00 in 
      if b = 0xff00 then true else false
    in
    if is_ipv4 e then is_multicast_v4 e 
    else is_multicast_v6 e
   
   let compare a b = compare (a.addr,a.port) (b.addr, b.port)

end



(** TODO: This needs to be implemented *)
module EthEndpoint = struct
  type t = { addr : Lwt_bytes.t }
end

(** TODO: This needs to be implemented *)
module BleEndpoint = struct
  type t = { addr : Lwt_bytes.t }
end

module Endpoint = struct
  type t = [ `IpEndpoint of IpEndpoint.t | `EthEndpoint of  EthEndpoint.t | `BleEndpoint of BleEndpoint.t]
end
