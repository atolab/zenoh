open Printf
open Zenoh
open Ztypes
open Zenoh.Message

let read_vle bs pos =
  let open Vle in
  let rec read_remaining bs pos vle =
    let c = int_of_char (Bytes.get bs pos) in
    let vle = logor (shift_left vle 7) (of_int (c land 0x7f)) in
    match (c land 0x80) with
    | 0x00 -> (vle, (pos + 1))
    | _ -> read_remaining bs (pos + 1) vle in
  read_remaining bs pos zero

let write_vle bs pos vle =
  let open Vle in
  let rec rec_write_vle bs pos vle =
    if vle = zero then pos
    else begin
      let pos = rec_write_vle bs pos (shift_right vle 7) in
      Bytes.set bs pos (Char.chr (((to_int vle) land 0x7f) lor 0x80));
      pos + 1
    end in
  let pos = rec_write_vle bs pos (shift_right vle 7) in
  Bytes.set bs pos (Char.chr ((to_int vle) land 0x7f));
  pos + 1

let read_seq bs pos read =
  let (length, pos) = read_vle bs pos in
  let rec read_remaining_props bs pos seq length =
    match length with
    | 0 -> (seq, pos)
    | _ -> let (value, pos) = read bs pos in
      read_remaining_props bs pos (value :: seq) (length - 1) in
  read_remaining_props bs pos [] (Vle.to_int length)

let write_seq bs pos seq write =
  let pos = write_vle bs pos (Vle.of_int (List.length seq)) in
  let rec write_remaining bs pos seq =
    match seq with
    | [] -> pos
    | head :: rem ->
      let pos = write bs pos head in
      write_remaining bs pos rem in
  write_remaining bs pos seq

let read_byte_seq bs pos =
  let open Vle in
  let (length, pos) = read_vle bs pos in
  (Bytes.sub bs (pos) (to_int length), pos + (to_int length))

let write_byte_seq bs pos seq =
  let open Vle in
  let seq_length = Bytes.length seq in
  let pos = write_vle bs pos (of_int seq_length) in
  Bytes.blit seq 0 bs pos seq_length;
  pos + seq_length

let read_prop bs pos =
  let (id, pos) = read_vle bs pos in
  let (value, pos) = read_byte_seq bs pos in
  (Property.create id value, pos)

let write_prop bs pos prop =
  let (id, value) = prop in
  let pos = write_vle bs pos id in
  write_byte_seq bs pos value

let read_prop_seq bs pos =
  read_seq bs pos read_prop

let write_prop_seq bs pos props =
  write_seq bs pos props write_prop

let read_string bs pos =
  let (str_bytes, pos) = read_byte_seq bs pos in
  (Bytes.to_string (str_bytes), pos)

let write_string bs pos s =
  write_byte_seq bs pos (Bytes.of_string s)

let read_locator bs pos =
  let (str, pos) = read_string bs pos in
  (Locator.from_string str, pos)

let write_locator bs pos locator =
  write_string bs pos (Locator.to_string locator)

let read_locator_seq bs pos =
  read_seq bs pos read_locator

let write_locator_seq bs pos locators =
  write_seq bs pos locators write_locator

let read_scout bs pos header =
  let open Scout in
  let (mask, pos) = read_vle bs (pos) in
  match ((int_of_char header) land (int_of_char Flags.pFlag)) with
  | 0x00 -> ({header=header; mask=mask; properties=[]}, pos)
  | _ -> let (props, pos) = read_prop_seq bs pos in
    ({header=header; mask=mask; properties=props}, pos)

let read_hello bs pos header =
  let open Hello in
  let (mask, pos) = read_vle bs (pos) in
  let (locators, pos) = read_locator_seq bs pos in
  match ((int_of_char header) land (int_of_char Flags.pFlag)) with
  | 0x00 -> ({header=header; mask=mask; locators=locators; properties=[]}, pos)
  | _ -> let (props, pos) = read_prop_seq bs pos in
    ({header=header; mask=mask; locators=locators; properties=props}, pos)

let read_open bs pos header =
  let open Open in
  let version = Bytes.get bs pos in
  let pos = pos + 1 in
  let (pid, pos) = read_byte_seq bs pos in
  let (lease, pos) = read_vle bs pos in
  let (locators, pos) = read_locator_seq bs pos in
  match ((int_of_char header) land (int_of_char Flags.pFlag)) with
  | 0x00 -> ({header=header; version=version; pid=pid; lease=lease; locators=locators; properties=[]}, pos)
  | _ -> let (props, pos) = read_prop_seq bs pos in
    ({header=header; version=version; pid=pid; lease=lease; locators=locators; properties=props}, pos)

let read_accept bs pos header =
  let open Accept in
  let (opid, pos) = read_byte_seq bs pos in
  let (apid, pos) = read_byte_seq bs pos in
  let (lease, pos) = read_vle bs pos in
  match ((int_of_char header) land (int_of_char Flags.pFlag)) with
  | 0x00 -> ({header=header; opid=opid; apid=apid; lease=lease; properties=[]}, pos)
  | _ -> let (props, _) = read_prop_seq bs pos in
    ({header=header; opid=opid; apid=apid; lease=lease; properties=props}, pos)

let read_close bs pos header =
  let open Close in
  let (pid, pos) = read_byte_seq bs pos in
  let reason = Bytes.get bs pos in
  let pos = pos + 1 in
  ({header=header; pid=pid; reason=reason}, pos)

let read_msg bs pos =
  let header = Bytes.get bs pos in
  match char_of_int (Header.mid (header)) with
  | id when id = MessageId.scoutId -> let (msg, pos) = read_scout bs (pos + 1) header in (Scout(msg), pos)
  | id when id = MessageId.helloId -> let (msg, pos) = read_hello bs (pos + 1) header in (Hello(msg), pos)
  | id when id = MessageId.openId -> let (msg, pos) = read_open bs (pos + 1) header in (Open(msg), pos)
  | id when id = MessageId.acceptId -> let (msg, pos) = read_accept bs (pos + 1) header in (Accept(msg), pos)
  | id when id = MessageId.closeId -> let (msg, pos) = read_close bs (pos + 1) header in (Close(msg), pos)
  | _ -> raise (Failure (sprintf "Unable to read message with header %02x" (Char.code header)))
