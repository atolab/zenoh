open Apero
open Ztypes

module PropertyId = struct
  let maxConduits = 2L
  let snLen = 4L
  let reliability = 6L
  let authData = 12L
  let queryDest = 16L

  let nodeMask = 1L
  let storageDist = 3L

  let user = 0x50L
  let password = 0x51L
end

module T = Apero.KeyValueF.Make(Apero.Vle) (Abuf)
include T

let find_opt pid = List.find_opt (fun p -> Vle.to_int @@ key p = (Vle.to_int pid))

let on_value f p = 
  let buf = value p in 
  Abuf.mark_r_pos buf; 
  let res = f buf in
  Abuf.reset_r_pos buf;
  res

module NodeMask = struct 
  let make mask = make 
    PropertyId.nodeMask 
    (Abuf.create 32 |> fun buf -> Apero.fast_encode_vle mask buf; buf)
  
  let mask = on_value Apero.fast_decode_vle

  let find_opt = find_opt PropertyId.nodeMask
end 

module StorageDist = struct 
  let make dist = make 
    PropertyId.storageDist 
    (Abuf.create 32 |> fun buf -> Apero.fast_encode_vle dist buf; buf)
  
  let dist = on_value Apero.fast_decode_vle

  let find_opt = find_opt PropertyId.storageDist
end

module QueryDest = struct
  let make dest = make 
    PropertyId.queryDest 
    (match dest with 
      | Partial    -> Abuf.create 32 |> fun buf -> Apero.fast_encode_vle 0L buf; buf
      | Complete q -> Abuf.create 32 |> fun buf -> Apero.fast_encode_vle 1L buf; Apero.fast_encode_vle (Vle.of_int q) buf; buf
      | All        -> Abuf.create 32 |> fun buf -> Apero.fast_encode_vle 2L buf; buf)

  let dest = on_value @@ fun buf ->
    Apero.fast_decode_vle buf |> function
    | 1L -> Complete (Apero.fast_decode_vle buf |> Vle.to_int)
    | 2L -> All
    | _ -> Partial

  let find_opt = find_opt PropertyId.queryDest
end

module User = struct 
  let make name = make 
    PropertyId.user 
    (Abuf.create (String.length name) |> fun buf -> Abuf.write_bytes (Bytes.unsafe_of_string name) buf; buf)
  
  let name = on_value (fun buf -> Abuf.read_bytes (Abuf.readable_bytes buf) buf |> Bytes.unsafe_to_string) 

  let find_opt = find_opt PropertyId.user
end

module Password = struct 
  let make phrase = make 
    PropertyId.password 
    (Abuf.create (String.length phrase) |> fun buf -> Abuf.write_bytes (Bytes.unsafe_of_string phrase) buf; buf)
  
  let phrase = on_value (fun buf -> Abuf.read_bytes (Abuf.readable_bytes buf) buf |> Bytes.unsafe_to_string) 

  let find_opt = find_opt PropertyId.password
end