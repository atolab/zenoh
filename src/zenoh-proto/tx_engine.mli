open Apero
open Ztypes
open Transport

module TransportEngine : sig 
  val register :  string -> (module Transport.S) -> unit
  val load : string -> ((module Transport.S), Error.e) result
  val resolve : string -> ((module Transport.S), Error.e) result
end