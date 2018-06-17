open Ztypes
open Iobuf

module Property = struct
  type id_t = Vle.t
  type t = Vle.t * IOBuf.t
  let create id data = (id, data)
  let id p = fst p
  let data p = snd p

end

module Properties = struct
  type t = Property.t list
  let empty = []
  let singleton p = [p]
  let add p ps = p::ps
  let find f ps = List.find_opt f ps
  let get name ps = List.find_opt (fun (n, _) -> if n = name then true else false) ps
  let length ps = List.length ps
  let of_list xs = xs
  let to_list ps = ps
end
