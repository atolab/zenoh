(* open Apero
open Apero.Result

let encode_properties ps =
  if ps = Properties.empty then return 
  else (encode_seq encode_property) ps 
  
let decode_properties = (decode_seq decode_property)  *)

(* let encode_properties = Apero.encode_properties *)
  
(* 
let decode_properties h buf =
  let open Message in 
  let open Apero in   
  let open Apero.Result.Infix in

  if Flags.(hasFlag h pFlag) then 
    begin 
      let ps = decode_properties buf in 
      ps >>= fun (ps, buf) -> Ok (Properties.of_list ps, buf)
    end
  else Result.return (Properties.empty, buf) 
 *)



(*   
let encode_properties ps =
  if ps = Properties.empty then return 
  else (encode_seq encode_property) ps 
  
let decode_properties = (decode_seq decode_property) 



*)

let decode_property buf =
  Apero.fast_decode_vle buf 
  |> (fun id -> 
      Apero.decode_buf buf 
      |> (fun data -> 
          ZProperty.make id data))
  
  

let encode_property (id, value) buf =  
  Apero.fast_encode_vle id buf;
  Apero.encode_buf value buf