open Zenoh_pervasives
open Netbuf

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

let test_iobuf = [
  "WR-Char" , `Quick, write_read_char_test;
]

(* Run it *)
let () =
  Alcotest.run "Netbuf Test" [
    "test_iobuf", test_iobuf;
  ]
