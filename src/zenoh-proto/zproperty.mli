open Ztypes
open Ziobuf

module Property : sig
  type t = Vle.t * IOBuf.t
  val create : Vle.t -> IOBuf.t -> t
  val id : t -> Vle.t
  val data: t -> IOBuf.t

end

module Properties : sig
  type t = Property.t list
  val empty : t
  val singleton : Property.t -> t
  val add : Property.t -> t -> t
  val find : (Property.t -> bool) -> t -> Property.t option
  val get : Vle.t -> t -> Property.t option
  val length : t -> int
  val of_list : Property.t list -> t
  val to_list : t -> Property.t list
end