open Apero
open Endpoint
open Iplocator


module Locator : sig 
  type t = UdpLocator of UdpLocator.t | TcpLocator of TcpLocator.t 
  val to_string : t -> string
  val of_string : string -> t option
end
  
module Locators : sig
  type t 
  val empty : t
  val singleton : Locator.t -> t
  val add : t -> Locator.t -> t
  val length : t -> int
  val to_list : t -> Locator.t list
  val of_list : Locator.t list -> t
  val to_string : t -> string
end
