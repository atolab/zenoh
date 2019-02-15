open Apero

module PropertyId = struct
  let maxConduits = 2L
  let snLen = 4L
  let reliability = 6L
  let authData = 12L
  let queryDest = 16L

  let nodeMask = 1L
  let storageDist = 3L
end

module T = Apero.KeyValueF.Make(Apero.Vle) (Abuf)
include T

let find_opt pid = List.find_opt (fun p -> Vle.to_int @@ key p = (Vle.to_int pid))

module NodeMask = struct 
  let make mask = make 
    PropertyId.nodeMask 
    (Abuf.create 32 |> fun buf -> Apero.encode_vle mask buf; buf)
  
  let mask p = value p |> Apero.decode_vle

  let find_opt = find_opt PropertyId.nodeMask
end 

module StorageDist = struct 
  let make dist = make 
    PropertyId.storageDist 
    (Abuf.create 32 |> fun buf -> Apero.encode_vle dist buf; buf)
  
  let dist p = value p |> Apero.decode_vle

  let find_opt = find_opt PropertyId.storageDist
end

module QueryDest = struct
  open Queries
  let make dest = make 
    PropertyId.queryDest 
    (match dest with 
      | Partial    -> Abuf.create 32 |> fun buf -> Apero.encode_vle 0L buf; buf
      | Complete q -> Abuf.create 32 |> fun buf -> Apero.encode_vle 1L buf; Apero.encode_vle (Vle.of_int q) buf; buf
      | All        -> Abuf.create 32 |> fun buf -> Apero.encode_vle 2L buf; buf)
  
  let dest p = 
    let buf = value p in 
    let destType = Apero.decode_vle buf in 
    match destType with 
    | 1L -> Complete (Apero.decode_vle buf |> Vle.to_int)
    | 2L -> All
    | _ -> Partial

  let find_opt = find_opt PropertyId.queryDest
end