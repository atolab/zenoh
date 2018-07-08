open Apero
open Transport

module TransportEngine : sig 
  val register :  string -> (module Transport.S) -> unit
  val load : string -> ((module Transport.S), error) result
  val resolve : string -> ((module Transport.S), error) result
end