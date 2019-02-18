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

module T = Apero.KeyValueF.Make(Apero.Vle) (Apero.MIOBuf)
include T

let find_opt pid = List.find_opt (fun p -> Vle.to_int @@ key p = (Vle.to_int pid))

module NodeMask = struct 
  let make mask = make 
    PropertyId.nodeMask 
    (let buf = MIOBuf.create 32 in 
    Apero.fast_encode_vle mask buf;
    MIOBuf.flip buf; buf)
  
  let mask p = value p |> Apero.fast_decode_vle 

  let find_opt = find_opt PropertyId.nodeMask
end 

module StorageDist = struct 
  let make dist = make 
    PropertyId.storageDist 
    (let buf = MIOBuf.create 32 in 
    Apero.fast_encode_vle dist buf;
    MIOBuf.flip buf; buf)
  
  let dist p = value p |> Apero.fast_decode_vle 

  let find_opt = find_opt PropertyId.storageDist
end

module QueryDest = struct
  open Queries
  let make dest = make 
    PropertyId.queryDest 
    (let buf = MIOBuf.create 32 in  
    match dest with 
    | Partial    ->         
      Apero.fast_encode_vle 0L buf; 
      MIOBuf.flip buf;
      buf
    | Complete q -> 
      Apero.encode_vle 1L buf;
      Apero.encode_vle (Vle.of_int q) buf;
      MIOBuf.flip buf; buf
    | All -> 
      Apero.fast_encode_vle 2L buf;
      MIOBuf.flip buf;
      buf)
  
  let dest p = 
    let buf = value p in 
    let destType = Apero.fast_decode_vle buf in 
    match destType with 
    | 1L -> Complete  (Vle.to_int @@ Apero.fast_decode_vle buf)
    | 2L -> All
    | _ -> Partial

  let find_opt = find_opt PropertyId.queryDest
end