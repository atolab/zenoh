module Frame : sig
  type t
  val empty : t
  val create : Message.t list -> t
  val add : Message.t -> t -> t
  val length : t -> int
  val to_list : t -> Message.t list
  val from_list : Message.t list -> t
end
