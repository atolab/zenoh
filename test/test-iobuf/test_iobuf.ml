open Zenoh_pervasives
open Netbuf

let test_cases = 100000

let write_read_char x =
  Result.do_
  ; buf <-- (IOBuf.create 16)
  ; wbuf <-- (IOBuf.put_char buf x)
  ; rbuf <-- (IOBuf.flip wbuf)
  ; (c, buf) <-- (IOBuf.get_char rbuf)
  ; () ; Printf.printf "written: %d read: %d\n" (int_of_char x) (int_of_char c)
  ; () ; Alcotest.(check char) "IOBuf write / read same character"  c x
  ; return buf

let write_read_char_test () =
  let rec run_test_loop n =
    if n <= 255 then
      begin
        let _  = write_read_char @@ char_of_int n in
        run_test_loop (n + 1)
      end
    else ()
  in
  print_endline "startint test"
  ; run_test_loop 0


let write_read_vle w =
  Result.do_
  ; buf <-- (IOBuf.create 64)
  ; wbuf <-- (IOBuf.put_vle buf w)
  ; rbuf <-- (IOBuf.flip wbuf)
  ; (r, buf) <-- (IOBuf.get_vle rbuf)
  ; () ; Printf.printf "written: %Ld read: %Ld\n" w r
  ; () ; Alcotest.(check int64) "IOBuf write / read same character"  w r
  ; return buf

let write_read_vle_test () =
  let _ = Random.init (int_of_float @@ Unix.time ()) in
  let rec loop n =
    if n < test_cases then
      begin
        let _ = write_read_vle @@ Random.int64 Int64.max_int in
        loop @@ n +1
      end
    else ()
  in loop 0


let test_iobuf = [
  "WR-Char" , `Quick, write_read_char_test;
  "WR-Vle.t" , `Quick, write_read_vle_test;
]

(* Run it *)
let () =
  Alcotest.run "Netbuf Test" [
    "test_iobuf", test_iobuf;
  ]
