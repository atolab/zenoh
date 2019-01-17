open Apero

module PropertyId = struct
  let maxConduits = 2L
  let snLen = 4L
  let reliability = 6L
  let authData = 12L

  let nodeMask = 1L
  let storageDist = 3L
end

module T = Apero.KeyValueF.Make(Apero.Vle) (Apero.IOBuf)
include T

let find_opt pid = List.find_opt (fun p -> Vle.to_int @@ key p = (Vle.to_int pid))

module NodeMask = struct 
  let make mask = make 
    PropertyId.nodeMask 
    (IOBuf.create 32 |> IOBuf.put_char mask |> Result.get |> IOBuf.flip)
  
  let mask p = value p |> IOBuf.get_char |> Result.get |> fst 

  let find_opt = find_opt PropertyId.nodeMask
end 

module StorageDist = struct 
  let make dist = make 
    PropertyId.storageDist 
    (IOBuf.create 32 |> Apero.encode_vle dist |> Result.get |> IOBuf.flip)
  
  let dist p = value p |> Apero.decode_vle |> Result.get |> fst 

  let find_opt = find_opt PropertyId.storageDist
end