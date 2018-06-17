open Ztypes
open Iobuf
open Apero
open Apero.ResultM
open Apero.ResultM.InfixM
open Property
open Locator

let encode_vle v buf =
  let to_char l = char_of_int @@ Int64.to_int l in
  let rec put_negative_vle_rec  v n buf =
    if n < Vle.max_bytes then
      begin
        let mv = Int64.logor Vle.more_bytes_flag (Int64.logand v Vle.byte_mask) in
        let b = IOBuf.put_char (to_char mv) buf in
        let sv = (Int64.shift_right v Vle.shift_len) in
        b >>= put_negative_vle_rec sv (n+1)
      end
    else
      IOBuf.put_char (to_char 1L) buf
  in
    let rec put_positive_vle_rec v buf =
      if v <= Vle.byte_mask then IOBuf.put_char (to_char v) buf
      else
        begin
          let mv = Int64.logor Vle.more_bytes_flag @@ Int64.logand v Vle.byte_mask in
          let b = IOBuf.put_char (to_char mv) buf in
          let sv = Int64.shift_right v Vle.shift_len in
          b >>= put_positive_vle_rec sv
        end
  in
    if v < 0L then put_negative_vle_rec v 1 buf
    else put_positive_vle_rec v buf

let decode_vle buf =
  let from_char c = Vle.of_int (int_of_char c) in
  let masked_from_char c = Vle.logand Vle.byte_mask  (Vle.of_int (int_of_char c)) in
  let merge v c n = Vle.logor v (Vle.shift_left c (n * Vle.shift_len)) in
  let rec decode_vle_rec  v n buf =
    if n < Vle.max_bytes then
      begin
        IOBuf.get_char buf
        >>= (fun (c, buf) -> 
          if (from_char c) <= Vle.byte_mask then  return ((merge v (masked_from_char c) n), buf)
          else decode_vle_rec (merge v (masked_from_char c) n) (n+1) buf
        )        
        
      end
    else
      begin
        let rec skip k buf =
          IOBuf.get_char buf
          >>= (fun (c, buf)  -> 
          if from_char c <= Vle.byte_mask then fail Error.(OutOfBounds (Msg "vle out of bounds"))
          else skip (k+1) buf )
        in skip n buf
      end
  in decode_vle_rec 0L 0 buf

let encode_bytes src dst =
  Logs.debug (fun m -> m "Encoding Bytes");
  let n = IOBuf.available src in
  let m = IOBuf.available dst in
  if n <= m then
    begin
      encode_vle (Vle.of_int n) dst 
      >>= (IOBuf.put_buf src)      
    end
    else      
        fail Error.(OutOfBounds (Msg (Printf.sprintf "encode_bytes failed because of bounds error %d < %d" n m)))    

  let decode_bytes buf =
    decode_vle buf
    >>= (fun (len, buf) -> IOBuf.get_buf (Vle.to_int len) buf)
      
  

let encode_string s buf =
  let len = String.length s in
  let bs = Lwt_bytes.of_string s in
  encode_vle (Vle.of_int len) buf
  >>= (IOBuf.blit_from_bytes bs 0 len)
    
let decode_string buf =
  decode_vle buf
  >>= (fun (vlen, buf) -> 
    let len =  Vle.to_int vlen in    
    IOBuf.blit_to_bytes len buf 
    >>= (fun (bs, buf) -> return (Lwt_bytes.to_string bs, buf)))
    

let decode_seq read buf  =
  let rec get_remaining seq length buf =
    match length with
    | 0 -> return (seq, buf)
    | _ ->
      read buf 
      >>= (fun (value, buf) -> get_remaining (value :: seq) (length - 1) buf)
  in
  decode_vle buf
  >>= (fun (length, buf) ->
    let _ = Lwt_log.debug @@  (Printf.sprintf "Reading seq of %d elements" (Vle.to_int length)) in
    (get_remaining  [] (Vle.to_int length) buf))

let encode_seq write seq buf =
  let rec put_remaining seq  buf =
    match seq with
    | [] -> return buf
    | head :: rem -> 
        write head buf 
        >>= put_remaining rem 
  in
    (encode_vle (Vle.of_int (List.length seq)) buf)
    >>= put_remaining seq

let decode_property buf =
  decode_vle buf 
  >>= (fun (id, buf) -> 
      decode_bytes buf 
      >>= (fun (data, buf) -> 
      return (Property.create id data, buf)))
  
  

let encode_property (id, value) buf =  
  encode_vle id buf
  >>= encode_bytes value
  
let decode_locator buf = 
  decode_string buf 
  >>= (fun (s, buf) ->  return (Locator.of_string s, buf))

let encode_locator l = 
  Logs.debug (fun m -> m "Encoding Locator");
  encode_string (Locator.to_string l) 

let decode_locators buf =   
  decode_seq decode_locator buf
  >>= (fun (ols, buf) -> 
    let ls = Apero.OptionM.get @@ Apero.OptionM.flatten ols in 
    return (Locators.of_list ls,buf)
  )
  
let encode_locators ls buf = 
  Logs.debug (fun m -> m "Encoding Locators");
  let locs = Locators.to_list ls in
  encode_seq encode_locator locs buf


