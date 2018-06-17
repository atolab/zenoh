open Ztypes
open Property
open Message
open Apero.ResultM
open Tcodec

let encode_properties ps =
  if ps = Properties.empty then return 
  else (encode_seq encode_property) ps 
  
let decode_properties h  buf =
  if Flags.(hasFlag h pFlag) then (decode_seq decode_property) buf
  else return (Properties.empty, buf) 

