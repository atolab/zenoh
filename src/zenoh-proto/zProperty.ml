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

module T = Apero.KeyValueF.Make(Apero.Vle) (Apero.IOBuf)
include T

let find_opt pid = List.find_opt (fun p -> Vle.to_int @@ key p = (Vle.to_int pid))

module NodeMask = struct 
  let make mask = make 
    PropertyId.nodeMask 
    (IOBuf.create 32 |> Apero.encode_vle mask |> Result.get |> IOBuf.flip)
  
  let mask p = value p |> Apero.decode_vle |> Result.get |> fst 

  let find_opt = find_opt PropertyId.nodeMask
end 

module StorageDist = struct 
  let make dist = make 
    PropertyId.storageDist 
    (IOBuf.create 32 |> Apero.encode_vle dist |> Result.get |> IOBuf.flip)
  
  let dist p = value p |> Apero.decode_vle |> Result.get |> fst 

  let find_opt = find_opt PropertyId.storageDist
end

module QueryDest = struct 
  type dest = 
  | Partial
  | Complete of int
  | All

  let make dest = make 
    PropertyId.queryDest 
    (match dest with 
      | Partial    -> IOBuf.create 32 |> Apero.encode_vle 0L |> Result.get |> IOBuf.flip
      | Complete q -> IOBuf.create 32 |> Apero.encode_vle 1L |> Result.get |> Apero.encode_vle (Vle.of_int q) |> Result.get|> IOBuf.flip
      | All        -> IOBuf.create 32 |> Apero.encode_vle 2L |> Result.get |> IOBuf.flip)
  
  let dest p = 
    let buf = value p in 
    let (destType, buf) = Apero.decode_vle buf |> Result.get in 
    match destType with 
    | 1L -> Complete (Apero.decode_vle buf |> Result.get |> fst |> Vle.to_int)
    | 2L -> All
    | _ -> Partial

  let find_opt = find_opt PropertyId.queryDest
end