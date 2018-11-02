open Apero

module VleMap = Map.Make(Vle)


module URI = struct

  let uri_match uri1 uri2 =
    let pattern_match uri pattern = 
      let expr = Str.regexp (pattern 
      |> Str.global_replace (Str.regexp "\\.") "\\."
      |> Str.global_replace (Str.regexp "\\*\\*") ".*"
      |> Str.global_replace (Str.regexp "\\([^\\.]\\)\\*") "\\1[^/]*"
      |> Str.global_replace (Str.regexp "^\\*") "[^/]*"
      |> Str.global_replace (Str.regexp "\\\\\\.\\*") "\\.[^/]*") in
      (Str.string_match expr uri 0) && (Str.match_end() = String.length uri) in
    (pattern_match uri1 uri2) || (pattern_match uri2 uri1)

end

module ResName = struct 
  type t  = | URI of string | ID of Vle.t

  let compare name1 name2 = 
    match name1 with 
    | ID id1 -> (match name2 with 
      | ID id2 -> Vle.compare id1 id2
      | URI _ -> 1)
    | URI uri1 -> (match name2 with 
      | ID _ -> -1
      | URI uri2 -> String.compare uri1 uri2)

  let name_match name1 name2 =
    match name1 with 
    | ID id1 -> (match name2 with 
      | ID id2 -> id1 = id2
      | URI _ -> false)
    | URI uri1 -> (match name2 with 
      | ID _ -> false
      | URI uri2 -> URI.uri_match uri1 uri2)

  let to_string = function 
    | URI uri -> uri 
    | ID id -> Vle.to_string id
end 

module ResMap = Map.Make(ResName)