open Apero

module VleMap = Map.Make(Vle)

module ResName = struct 
  type t  = | Path of PathExpr.t | ID of Vle.t

  let compare name1 name2 = 
    match name1 with 
    | ID id1 -> (match name2 with 
      | ID id2 -> Vle.compare id1 id2
      | Path _ -> 1)
    | Path uri1 -> (match name2 with 
      | ID _ -> -1
      | Path uri2 -> PathExpr.compare uri1 uri2)

  let name_match name1 name2 =
    match name1 with 
    | ID id1 -> (match name2 with 
      | ID id2 -> id1 = id2
      | Path _ -> false)
    | Path uri1 -> (match name2 with 
      | ID _ -> false
      | Path uri2 -> PathExpr.intersect uri1 uri2)

  let to_string = function 
    | Path uri -> PathExpr.to_string uri 
    | ID id -> Vle.to_string id
end 

module ResMap = Map.Make(ResName)