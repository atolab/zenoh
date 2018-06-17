open Endpoint
open Apero
open Iplocator


module Locator = struct
  (** This will become a union time once we'll support non IP-based transpots *)
  type t = 
  | UdpLocator of UdpLocator.t 
  | TcpLocator of TcpLocator.t
  
    
  let to_string = function 
  | UdpLocator ul -> UdpLocator.to_string ul
  | TcpLocator tl -> TcpLocator.to_string tl

  let of_string s = 
    let open OptionM.InfixM in 
    String.index_opt s '/'
    >>= (fun pos -> 
          match String.sub s 0 pos with 
          | "tcp" -> (TcpLocator.of_string s) >>= fun loc -> Some (TcpLocator loc)
          | "udp" -> (UdpLocator.of_string s) >>= fun loc -> Some (UdpLocator loc)
          | _  -> None)
  
  end

module Locators = struct
  type t = Locator.t list
  let empty = []
  let singleton l = [l]
  let add ls l = l::ls
  let length ls = List.length ls
  let to_list ls = ls
  let of_list ls = ls
  let to_string ls = List.to_string ls (fun l -> Locator.to_string l)
end
