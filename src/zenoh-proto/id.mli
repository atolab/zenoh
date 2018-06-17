module type S = sig 
  type t 
  val zero : t
  val one : t
  val add : t -> t -> t 
  val next_id : unit -> t
  val compare : t -> t -> int  
  val equal : t -> t -> bool
  val show : t -> string
end 

module type IdSignature = sig
  type t 
  val zero : t 
  val one : t 
  val add : t -> t -> t  
  val equal : t -> t -> bool  
  val compare : t -> t -> int  
  val show : t -> string
end


module Make(T : IdSignature) : S