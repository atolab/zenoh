open Zenoh_pervasives
open Monad

open Netbuf

let test_cases = 1000
let batch = 64

module ErrorA = struct
  type e = ErrorAOne of int | ErrorATwo of int
end

module ErrorB = struct
  type e = ErrorBOne of int | ErrorBTwo of int
end

module ResultA = ResultM(ErrorA)
module ResultB = ResultM(ErrorB)

module type ResultTransformerSig = sig
  val transform : ('a, 'b) result ->  ('b -> ('a, 'c) result) -> ('a, 'c) result
end

module ResultTransformer (RA : ResultS) (RB : ResultS) = struct
  let transform r f = match r with
    | RA.(Ok v) -> RB.ok v
    | RA.(Error e) -> RB.fail (f e)
end

module RTAB =  ResultTransformer(ResultA)(ResultB)

let transform r f g = match r with
  | Ok v -> f v
  | Error e -> g e

let produce_result_a n =
  if n > 10 then ResultA.return 10 else ResultA.fail @@ ErrorA.ErrorAOne 10

let test_tresult_m () =
  let _ = RTAB.transform (produce_result_a 20) (fun _ -> ErrorB.ErrorBOne 10)
  in ()

let test_tresult_m () =
  let _ =
    match (ResultA.do_
          ; produce_result_a 20) with
    | Error (ErrorA.ErrorAOne v) -> ResultB.fail (ErrorB.ErrorBOne v)
    | Error (ErrorA.ErrorATwo v) -> ResultB.fail (ErrorB.ErrorBTwo v)
    | _ -> ResultB.fail (ErrorBOne 0)
  in ()

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

let write_read_string () =
  let words = open_in "/usr/share/dict/words" in
  let rec rws buf n =
    if n < test_cases then
      begin
        let m = batch in
        let xs = apply_n words input_line m  in
        let rec write_list buf xs =
          match xs with
          | h::tl ->
            Result.do_
            ; buf <-- (IOBuf.put_string buf h)
            ; write_list buf tl
          | [] -> Result.ok buf
        in
        let rec read_list buf n xs  =
          if n = 1 then Result.ok (xs, buf)
          else
            begin
              Result.do_
              ; (s, buf) <-- IOBuf.get_string buf
              ; read_list buf (n-1) (s::xs)
            end
        in
        Result.do_
        ; buf <-- IOBuf.clear buf
        ; buf <-- write_list buf xs
        ; buf <-- IOBuf.flip buf
        ; (ys, buf) <-- read_list buf m []
        ; () ; let cs = (List.combine xs (List.rev ys)) in  cs |> List.iter (fun (w, r) ->  Printf.printf "w: %s, r: %s\n" w r) ; cs |> List.iter (fun (w,r) -> Alcotest.(check string) "IOBuf write / read same  string" w r)
        ; rws buf (n+1)
      end
    else Result.ok buf
  in
  let open Result in
  let _ = IOBuf.create (1024*1024) >>= fun buf -> rws buf 0 in ()

let write_read_vle w =
  Result.do_
  ; buf <-- (IOBuf.create 64)
  ; wbuf <-- (IOBuf.put_vle buf w)
  ; rbuf <-- (IOBuf.flip wbuf)
  ; (r, buf) <-- (IOBuf.get_vle rbuf)
  ; () ; Printf.printf "written: %Ld read: %Ld\n" w r
  ; () ; Alcotest.(check int64) "IOBuf write / read same  vle"  w r
  ; return buf

let write_read_vle_test () =
  let _ = Random.init (int_of_float @@ Unix.time ()) in
  let rec loop n =
    if n < test_cases then
      begin
        let u = Random.int64 Int64.max_int in
        let v = Random.int64 Int64.max_int in
        let d = Int64.sub u v in
        let nu = Int64.neg u in
        let nv = Int64.neg v in
        let _ = write_read_vle @@ Int64.of_int n in
        let _ = write_read_vle u in
        let _ = write_read_vle v in
        let _ = write_read_vle d in
        let _ = write_read_vle nu in
        let _ = write_read_vle nv in
        loop @@ n +1
      end
    else ()
  in loop 0


let test_iobuf = [
  "WR-Char" , `Quick, write_read_char_test;
  "WR-Vle.t" , `Quick, write_read_vle_test;
  "WR-String", `Quick, write_read_string;
  (* "TResultM", `Quick, test_tresult_m; *)
]

(* Run it *)
let () =
  Alcotest.run "Netbuf Test" [
    "test_iobuf", test_iobuf;
  ]
