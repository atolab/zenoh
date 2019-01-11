open Apero

let test_cases = 1000
let batch = 64

let encode_decode_char x =
  let open Result.Infix in  
  let buf = IOBuf.create 16 in 
  IOBuf.put_char x buf
  >>= fun buf -> 
    IOBuf.get_char (IOBuf.flip buf)
    >>= fun (c, buf) -> 
      let () = Printf.printf "written: %d read: %d\n" (int_of_char x) (int_of_char c) in 
      let () = Alcotest.(check char) "IOBuf write / read same character"  c x in 
      (Result.ok buf)
        

let encode_decode_char_test () =
  let rec run_test_loop n =
    if n <= 255 then
      begin
        let _  = encode_decode_char @@ char_of_int n in
        run_test_loop (n + 1)
      end
    else ()
  in
  print_endline "startint test"
  ; run_test_loop 0
(* 
let encode_decode_string () =
  let words = open_in "/usr/share/dict/words" in
  let rec rws buf n =
    if n < test_cases then
      begin
        let m = batch in
        let xs = apply_n words input_line m  in        
        let buf = IOBuf.clear buf in 
        Result.do_
        ; buf <-- 
        ; buf <-- encode_list buf xs
        ; buf <-- IOBuf.flip buf
        ; (ys, buf) <-- decode_list buf m []
        ; () ; let cs = (List.combine xs (List.rev ys)) in  cs |> List.iter (fun (w, r) ->  Printf.printf "w: %s, r: %s\n" w r) ; cs |> List.iter (fun (w,r) -> Alcotest.(check string) "IOBuf write / read same  string" w r)
        ; rws buf (n+1)
      end
    else Result.ok buf
  in
  let open Result in
  let _ = IOBuf.create (1024*1024) >>= fun buf -> rws buf 0 in ()

let encode_decode_vle w =
  Result.do_
  ; buf <-- (IOBuf.create 64)
  ; wbuf <-- (IOBuf.encode_vle buf w)
  ; rbuf <-- (IOBuf.flip wbuf)
  ; (r, buf) <-- (IOBuf.decode_vle rbuf)
  ; () ; Printf.printf "written: %Ld read: %Ld\n" w r
  ; () ; Alcotest.(check int64) "IOBuf write / read same  vle"  w r
  ; return buf

let encode_decode_vle_test () =
  let _ = Random.init (int_of_float @@ Unix.time ()) in
  let rec loop n =
    if n < test_cases then
      begin
        let u = Random.int64 Int64.max_int in
        let v = Random.int64 Int64.max_int in
        let d = Int64.sub u v in
        let nu = Int64.neg u in
        let nv = Int64.neg v in        
        let _ = encode_decode_vle @@ Int64.of_int n in
        let _ = encode_decode_vle u in
        let _ = encode_decode_vle v in
        let _ = encode_decode_vle d in
        let _ = encode_decode_vle nu in
        let _ = encode_decode_vle nv in
        loop @@ n +1
      end
    else ()
  in loop 0 *)


let test_iobuf = [
  "WR-Char" , `Quick, encode_decode_char_test
  (* "WR-Vle.t" , `Quick, encode_decode_vle_test; *)
  (* "WR-String", `Quick, encode_decode_string *)
]

(* Run it *)
let () =
  Alcotest.run "Netbuf Test" [
    "test_iobuf", test_iobuf;
  ]
