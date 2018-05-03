open Printf
open Zenoh_pervasives
open Netbuf
open Zenoh
open Ztypes
open Zenoh.Message

let read_seq buf read =
  let rec read_remaining buf seq length =
    match length with
    | 0 -> Result.ok (seq, buf)
    | _ ->
      Result.do_
      ; (value, buf) <-- read buf
      ; read_remaining buf (value :: seq) (length - 1) in
  Result.do_
  ; (length, buf) <-- IOBuf.get_vle buf
  ; read_remaining buf [] (Vle.to_int length)

let write_seq buf seq write =
  let rec write_remaining buf seq =
    match seq with
    | [] -> Result.ok buf
    | head :: rem ->
      Result.do_
      ; buf <-- write buf head
      ; write_remaining buf rem in
  Result.do_
  ; buf <-- IOBuf.put_vle buf (Vle.of_int (List.length seq))
  ; write_remaining buf seq

let read_byte_seq buf =
  Result.do_
  ; (length, buf2) <-- (IOBuf.get_vle buf)
  ; if (IOBuf.get_position buf2) + (Vle.to_int length) <= (IOBuf.get_limit buf2) then
    begin
      let int_length = Vle.to_int length in
      let result = Lwt_bytes.create int_length in
      Lwt_bytes.blit (IOBuf.to_bytes buf) (IOBuf.get_position buf) result 0 int_length;
      Result.do_
      ; buf <-- IOBuf.set_position buf ((IOBuf.get_position buf) + int_length)
      ; Result.ok (result, buf)
    end else Result.error (IOBuf.OutOfRangeGet (IOBuf.get_position buf, IOBuf.get_limit buf))

let write_byte_seq buf seq =
  let seq_length = Lwt_bytes.length seq in
  Result.do_
  ; buf <-- IOBuf.put_vle buf (Vle.of_int seq_length)
  ; IOBuf.blit_from_bytes seq 0 buf seq_length

let read_prop buf =
  Result.do_
  ; (id, buf) <-- IOBuf.get_vle buf
  ; (value, buf) <-- read_byte_seq buf
  ; Result.ok (Property.create id value, buf)

let write_prop buf prop =
  let (id, value) = prop in
  Result.do_
  ; buf <-- IOBuf.put_vle buf id
  ; write_byte_seq buf value

let read_prop_seq buf =
  read_seq buf read_prop

let write_prop_seq buf props =
  write_seq buf props write_prop

let read_locator buf =
  Result.do_
  ; (str, buf) <-- IOBuf.get_string buf
  ; Result.ok (Locator.from_string str, buf)

let write_locator buf locator =
  IOBuf.put_string buf (Locator.to_string locator)

let read_locator_seq buf =
  read_seq buf read_locator

let write_locator_seq buf locators =
  write_seq buf locators write_locator

let read_scout buf header =
  let open Scout in
  Result.do_
  ; (mask, buf) <-- IOBuf.get_vle buf
  ; match ((int_of_char header) land (int_of_char Flags.pFlag)) with
    | 0x00 -> Result.ok ({header=header; mask=mask; properties=[]}, buf)
    | _ -> Result.do_
           ; (props, buf) <-- read_prop_seq buf
           ; Result.ok ({header=header; mask=mask; properties=props}, buf)

let read_hello buf header =
  let open Hello in
  Result.do_
  ; (mask, buf) <-- IOBuf.get_vle buf
  ; (locators, buf) <-- read_locator_seq buf
  ; match ((int_of_char header) land (int_of_char Flags.pFlag)) with
    | 0x00 -> Result.ok ({header=header; mask=mask; locators=locators; properties=[]}, buf)
    | _ -> Result.do_
           ; (props, buf) <-- read_prop_seq buf
           ; Result.ok ({header=header; mask=mask; locators=locators; properties=props}, buf)

let read_open buf header =
  let open Open in
  Result.do_
  ; (version, buf) <-- IOBuf.get_char buf
  ; (pid, buf) <-- read_byte_seq buf
  ; (lease, buf) <-- IOBuf.get_vle buf
  ; (locators, buf) <-- read_locator_seq buf
  ; match ((int_of_char header) land (int_of_char Flags.pFlag)) with
    | 0x00 -> Result.ok ({header=header; version=version; pid=pid; lease=lease; locators=locators; properties=[]}, buf)
    | _ -> Result.do_
           ; (props, buf) <-- read_prop_seq buf
           ; Result.ok ({header=header; version=version; pid=pid; lease=lease; locators=locators; properties=props}, buf)

let read_accept buf header =
  let open Accept in
  Result.do_
  ; (opid, buf) <-- read_byte_seq buf
  ; (apid, buf) <-- read_byte_seq buf
  ; (lease, buf) <-- IOBuf.get_vle buf
  ; match ((int_of_char header) land (int_of_char Flags.pFlag)) with
    | 0x00 -> Result.ok ({header=header; opid=opid; apid=apid; lease=lease; properties=[]}, buf)
    | _ -> Result.do_
          ; (props, buf) <-- read_prop_seq buf
          ; Result.ok ({header=header; opid=opid; apid=apid; lease=lease; properties=props}, buf)

let read_close buf header =
  let open Close in
  Result.do_
  ; (pid, buf) <-- read_byte_seq buf
  ; (reason, buf) <-- IOBuf.get_char buf
  ; Result.ok ({header=header; pid=pid; reason=reason}, buf)

let read_msg buf =
  Result.do_
  ; (header, buf) <-- IOBuf.get_char buf
  ; match char_of_int (Header.mid (header)) with
    | id when id = MessageId.scoutId -> Result.do_; (msg, buf) <-- read_scout buf header; Result.ok (Scout(msg), buf)
    | id when id = MessageId.helloId -> Result.do_; (msg, buf) <-- read_hello buf header; Result.ok (Hello(msg), buf)
    | id when id = MessageId.openId -> Result.do_; (msg, buf) <-- read_open buf header; Result.ok (Open(msg), buf)
    | id when id = MessageId.acceptId -> Result.do_; (msg, buf) <-- read_accept buf header; Result.ok (Accept(msg), buf)
    | id when id = MessageId.closeId -> Result.do_; (msg, buf) <-- read_close buf header; Result.ok (Close(msg), buf)
    | _ -> Result.error (IOBuf.InvalidFormat)
