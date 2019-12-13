open Apero

module BeId = Id.Make(
  struct
   include String
   let of_string s = s
   let to_string s = s
  end)
(** backend id *)

module  StorageId = Id.Make(
  struct
   include String
   let of_string s = s
   let to_string s = s
  end)
(** storage id *)


