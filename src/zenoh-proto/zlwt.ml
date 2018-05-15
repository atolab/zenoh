module LwtM = struct
  open Lwt.Infix
  let rec fold_m f b xs = match xs with
    | [] -> Lwt.return b
    | h::tl -> f b h >>= (fun b -> fold_m f b tl)
end
