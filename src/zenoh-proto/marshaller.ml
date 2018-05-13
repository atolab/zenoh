open Apero
open Printf
open Netbuf
open Zenoh
open Ztypes
open Zenoh.Message

(** This operator should be generalised for the result type associated with the IOBuf *)


let (<>>=) r f = match r with
  | Ok (a, b) -> Result.ok (f a, b)
  | Error _ as e -> e

let (><>=) r f = match r with
  | Ok _ -> r
  | Error e  -> Result.fail @@ f e

let read_seq buf read =
  let open Result in
  let rec read_remaining buf seq length =
    match length with
    | 0 -> Result.ok (seq, buf)
    | _ ->
      (do_
      ; (value, buf) <-- read buf
      ; read_remaining buf (value :: seq) (length - 1))
  in
  (do_
  ; (length, buf) <-- IOBuf.get_vle buf
  ; () ; Lwt.ignore_result @@ Lwt_log.debug @@  (Printf.sprintf "Reading seq of %d elements" (Vle.to_int length))
  ; read_remaining buf [] (Vle.to_int length))

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


(* let get_io_buf buf =
  let open IOBuf in
  let open Result in
  (do_
  ; (len, buf) <-- get_vle buf
  ; () ; Lwt.ignore_result @@ Lwt_log.debug @@ Printf.sprintf "Reading IOBuf of %d bytes"  (Vle.to_int len)
  ; payload <-- create @@ Vle.to_int len
  ; buf <-- blit buf payload
  ; return (payload, buf)) *)

(* let put_io_buf buf iobuf =
  let open IOBuf in
  let open Result in
  (do_
  ; buf <-- put_vle buf @@ Vle.of_int @@ get_limit iobuf
  ; () ; Lwt.ignore_result @@ Lwt_log.debug @@ Printf.sprintf "Writing IOBuf of %d bytes"  (get_limit iobuf)
  ; buf <-- blit iobuf buf
  ; return buf) *)

let read_byte_seq buf =
  Result.do_
  ; (length, buf2) <-- (IOBuf.get_vle buf)
  ; () ; Lwt.ignore_result @@ Lwt_log.debug @@ Printf.sprintf "Reading IOBuf of %d bytes"  (Vle.to_int length)
  ; if (IOBuf.get_position buf2) + (Vle.to_int length) <= (IOBuf.get_limit buf2) then
    begin
      let int_length = Vle.to_int length in
      let result = Lwt_bytes.create int_length in
      Lwt_bytes.blit (IOBuf.to_bytes buf) (IOBuf.get_position buf) result 0 int_length;
      Result.do_
      ; buf <-- IOBuf.set_position buf ((IOBuf.get_position buf) + int_length)
      ; Result.ok (result, buf)
    end else Result.fail Error.(OutOfBounds  (Pos  __POS__) )

let write_byte_seq buf seq =
  let seq_length = Lwt_bytes.length seq in
  Result.do_
  ; buf <-- IOBuf.put_vle buf (Vle.of_int seq_length)
  ; IOBuf.blit_from_bytes seq 0 buf seq_length

let read_prop buf =
  let open Result in
  (do_
  ; (id, buf) <-- IOBuf.get_vle buf
  ; (data, buf) <-- IOBuf.get_io_buf buf
  ; return (Property.create id data, buf))

let write_prop buf prop =
  let (id, value) = prop in
  Result.do_
  ; buf <-- IOBuf.put_vle buf id
  ; IOBuf.put_io_buf buf value

let read_prop_seq buf =
  read_seq buf read_prop

let write_prop_seq buf props =
  write_seq buf props write_prop

let write_properties buf h ps =
  match ((int_of_char h) land (int_of_char Flags.pFlag)) with
  | 0 -> Result.ok buf
  | _ -> write_prop_seq buf ps

let read_properties buf h =
  let hasProps = Flags.hasFlag h Flags.pFlag in
  let _ = Lwt_log.debug @@ Printf.sprintf "Parsing Properties (%b)" hasProps in
  match hasProps with
  | true -> read_prop_seq buf
  | _  -> Result.ok ([], buf)


let read_locator buf =
  Result.do_
  ; (str, buf) <-- IOBuf.get_string buf
  ; Result.ok (Locator.of_string str, buf)

let write_locator buf locator =
  IOBuf.put_string buf (Locator.to_string locator)

let read_locator_seq buf =
  read_seq buf read_locator

let write_locator_seq buf locators =
  write_seq buf locators write_locator

let read_scout buf header =
  Result.do_
  ; () ; Lwt.ignore_result @@ Lwt_log.debug "Rading Scout"
  ; (mask, buf) <-- IOBuf.get_vle buf
  ; match ((int_of_char header) land (int_of_char Flags.pFlag)) with
    | 0x00 -> Result.ok (Scout.create mask Properties.empty, buf)
    | _ -> Result.do_
           ; (props, buf) <-- read_prop_seq buf
           ; Result.ok (Scout.create mask props, buf)

let write_scout buf scout =
  let open Scout in
  Result.do_
  ; () ; Lwt.ignore_result @@ Lwt_log.debug "Writring Scout"
  ; buf <-- IOBuf.put_char buf (header scout)
  ; buf <-- IOBuf.put_vle buf (mask scout)
  ; match ((int_of_char (header scout)) land (int_of_char Flags.pFlag)) with
  | 0x00 -> Result.ok buf
  | _ -> Result.do_
       ; buf <-- write_prop_seq buf (properties scout)
        ; Result.ok buf

let read_hello buf header =
  Result.do_
  ; () ; Lwt.ignore_result @@ Lwt_log.debug "Readings Hello"
  ; (mask, buf) <-- IOBuf.get_vle buf
  ; (locators, buf) <-- read_locator_seq buf
  ; match ((int_of_char header) land (int_of_char Flags.pFlag)) with
    | 0x00 -> Result.ok (Hello.create mask locators [], buf)
    | _ -> Result.do_
           ; (props, buf) <-- read_prop_seq buf
           ; Result.ok (Hello.create mask locators props, buf)

let write_hello buf hello =
  let open Hello in
  Result.do_
  ; () ; Lwt.ignore_result @@ Lwt_log.debug "Writing Hello"
  ; buf <-- IOBuf.put_char buf (header hello)
  ; buf <-- IOBuf.put_vle buf (mask hello)
  ; buf <-- write_locator_seq buf (locators hello)
  ; match ((int_of_char (header hello)) land (int_of_char Flags.pFlag)) with
  | 0x00 -> Result.ok buf
  | _ -> Result.do_
         ; buf <-- write_prop_seq buf (properties hello)
         ; Result.ok buf

let read_open buf header =
  Result.do_
  ; () ; Lwt.ignore_result @@ Lwt_log.debug "Reading Open"
  ; (version, buf) <-- IOBuf.get_char buf
  ; (pid, buf) <-- IOBuf.get_io_buf buf
  ; (lease, buf) <-- IOBuf.get_vle buf
  ; () ; Lwt.ignore_result @@ Lwt_log.debug @@ Printf.sprintf "Lease = %d" (Vle.to_int lease)
  ; (locs, buf) <-- read_locator_seq buf
  ; (ps, buf) <-- read_properties buf header
  ; return (Open.create version pid lease [] [], buf)

let write_open buf msg =
  let open Open in
  Result.do_
  ; () ; Lwt.ignore_result @@ Lwt_log.debug "Writing Open"
  ; buf <-- IOBuf.put_char buf (header msg)
  ; buf <-- IOBuf.put_char buf (version msg)
  ; buf <-- IOBuf.put_io_buf buf (pid msg)
  ; buf <-- IOBuf.put_vle buf (lease msg)
  ; buf <-- write_locator_seq buf (locators msg)
  ; write_properties buf (header msg) (properties msg)

let read_accept buf (header:char) =
  Result.do_
  ; () ; Lwt.ignore_result @@ Lwt_log.debug "Reading Accept"
  ; (opid, buf) <-- IOBuf.get_io_buf buf
  ; (apid, buf) <-- IOBuf.get_io_buf buf
  ; (lease, buf) <-- IOBuf.get_vle buf
  ; () ; Lwt.ignore_result @@ Lwt_log.debug @@ Printf.sprintf "Lease = %d" (Vle.to_int lease)
  ; match ((int_of_char header) land (int_of_char Flags.pFlag)) with
    | 0x00 -> Result.ok (Accept.create opid apid lease [], buf)
    | _ -> Result.do_
          ; (props, buf) <-- read_prop_seq buf
          ; Result.ok (Accept.create opid apid lease props, buf)

let write_accept buf accept =
  let open Accept in
  Result.do_
  ; () ; Lwt.ignore_result @@ Lwt_log.debug "Writing Accept"
  ; buf <-- IOBuf.put_char buf (header accept)
  ; buf <-- IOBuf.put_io_buf buf (opid accept)
  ; buf <-- IOBuf.put_io_buf buf (apid accept)
  ; buf <-- IOBuf.put_vle buf (lease accept)
  ; match ((int_of_char (header accept)) land (int_of_char Flags.pFlag)) with
    | 0x00 -> Result.ok buf
    | _ -> Result.do_
           ; buf <-- write_prop_seq buf (properties accept)
           ; Result.ok buf

let read_close buf header =
  Result.do_
  ; () ; Lwt.ignore_result @@ Lwt_log.debug "Reading Close"
  ; (pid, buf) <-- IOBuf.get_io_buf buf
  ; (reason, buf) <-- IOBuf.get_char buf
  ; Result.ok (Close.create pid reason, buf)

let write_close buf close =
  let open Close in
  Result.do_
  ; () ; Lwt.ignore_result @@ Lwt_log.debug "Writing Close"
  ; buf <-- IOBuf.put_char buf (header close)
  ; buf <-- IOBuf.put_io_buf buf (pid close)
  ; buf <-- IOBuf.put_char buf (reason close)
  ; Result.ok buf

let read_pub_decl buf h =
  let open PublisherDecl in
  (Result.do_
  ; () ; Lwt.ignore_result @@ Lwt_log.debug "Reading PubDeclaration"
  ; (rid, buf) <-- IOBuf.get_vle buf
  ; (ps, buf) <-- read_properties buf h
  ; return (Declaration.PublisherDecl (PublisherDecl.create rid ps), buf))

let write_pub_decl buf d =
  let open PublisherDecl in
  let open Result in
  (do_
  ; () ; Lwt.ignore_result @@ Lwt_log.debug "Writing PubDeclaration"
  ; buf <-- IOBuf.put_char buf (header d)
  ; buf <-- IOBuf.put_vle buf (rid d)
  ; write_properties buf (header d) (properties d))

let read_temporal_properties buf =
  let open Result in
  (do_
  ; () ; Lwt.ignore_result @@ Lwt_log.debug "Reading TemporalProperties"
  ; (origin, buf) <-- IOBuf.get_vle buf
  ; (period, buf) <-- IOBuf.get_vle buf
  ; (duration, buf) <-- IOBuf.get_vle buf
  ; return (TemporalProperties.create origin period duration, buf))

let write_temporal_properties buf stp =
  let open TemporalProperties in
  let open IOBuf in
  let open Result in
  match stp with
  | None -> return buf
  | Some tp -> (do_
               ; () ; Lwt.ignore_result @@ Lwt_log.debug "Writing Temporal"
               ; buf <-- put_vle buf (origin tp)
               ; buf <-- put_vle buf (period tp)
               ; put_vle buf (duration tp))

let read_sub_mode buf =
  let open Result in
  do_
  ; () ; Lwt.ignore_result @@ Lwt_log.debug "Reading SubMode"
  ; (id, buf) <-- IOBuf.get_char buf
  ; () ; Lwt.ignore_result @@ Lwt_log.debug @@ Printf.sprintf "SubMode.id = %d" (Header.mid id)
  ; match Flags.mid id with
    | a when a = SubscriptionModeId.pushModeId -> return (SubscriptionMode.PushMode, buf)
    | b when b = SubscriptionModeId.pullModeId -> return (SubscriptionMode.PullMode, buf)
    | c when c = SubscriptionModeId.periodicPushModeId ->
      (read_temporal_properties buf)
      >>= (fun (tp, buf) -> return (SubscriptionMode.PeriodicPushMode tp, buf))
    | d when d = SubscriptionModeId.periodicPullModeId ->
      (read_temporal_properties buf)
      >>= (fun (tp, buf) -> return (SubscriptionMode.PeriodicPullMode tp, buf))
    | _ -> fail Error.(OutOfBounds NoMsg)


let write_sub_mode buf m =
  let open SubscriptionMode in
  let open Result in
  (do_
  ; () ; Lwt.ignore_result @@ Lwt_log.debug "Writing SubMode"
  ; sid <-- return @@ (id m)
  ; buf <-- IOBuf.put_char buf sid
  ; write_temporal_properties buf (temporal_properties m))

let read_sub_decl buf h =
  let open SubscriberDecl in
  let open Result in
  (do_
  ; () ; Lwt.ignore_result @@ Lwt_log.debug "Reading SubDeclaration"
  ; (rid, buf) <-- IOBuf.get_vle buf
  ; (mode, buf) <-- read_sub_mode buf
  ; (ps, buf) <-- read_properties buf h
  ; return (Declaration.SubscriberDecl (SubscriberDecl.create rid mode ps), buf))

let write_sub_decl buf d =
  let open SubscriberDecl in
  let open Result in
  do_
  ; () ; Lwt.ignore_result @@ Lwt_log.debug "Writing SubDeclaration"
  ; buf <-- IOBuf.put_char buf (header d)
  ; buf <-- IOBuf.put_vle buf (rid d)
  ; buf <-- write_sub_mode buf (mode d)
  ; write_properties buf (header d) (properties d)

let read_commit_decl buf _ =
  let open IOBuf in
  let open Result in
  (do_
  ; (commit_id, buf) <-- get_char buf
  ; return ((Declaration.CommitDecl (CommitDecl.create commit_id)), buf))

let write_commit_decl buf cd =
  let open CommitDecl in
  let open IOBuf in
  let open Result in
  (do_
  ; () ; Lwt.ignore_result @@ Lwt_log.debug "Writing CommitDecl"
  ; buf <-- put_char buf (header cd)
  ; put_char buf (commit_id cd))

let read_result_decl buf _ =
  let open IOBuf in
  let open Result in
  (do_
  ; (commit_id, buf) <-- get_char buf
  ; (status, buf) <-- get_char buf
  ; if status = char_of_int 0 then
      return (Declaration.ResultDecl  (ResultDecl.create commit_id status None), buf)
    else
      get_vle buf <>>= (fun v -> Declaration.ResultDecl (ResultDecl.create commit_id status (Some v))))

let write_result_decl buf rd =
  let open ResultDecl in
  let open IOBuf in
  let open Result in
  (do_
  ; buf <-- put_char buf (header rd)
  ; buf <-- put_char buf (commit_id rd)
  ; buf <-- put_char buf (status rd)
  ; match (id rd) with | None -> return buf | Some v -> put_vle buf v)

let read_declaration buf =
  let open IOBuf in
  let open Result in
  (do_
  ; () ; Lwt.ignore_result @@ Lwt_log.debug "Reading Declaration"
  ; (header, buf) <-- get_char buf
  ; () ; Lwt.ignore_result @@ Lwt_log.debug @@ Printf.sprintf "Declaration Id = %d" (Header.mid header)
  ; match Flags.mid header with
  | p when p = DeclarationId.publisherDeclId -> read_pub_decl buf header
  | s when s = DeclarationId.subscriberDeclId -> read_sub_decl buf header
  | c when c = DeclarationId.commitDeclId -> read_commit_decl buf header
  | r when r = DeclarationId.resultDeclId -> read_result_decl buf header
  | _ -> fail Error.(NotImplemented))


let write_declaration buf (d: Declaration.t) =
  match d with
  | PublisherDecl pd -> write_pub_decl buf pd
  | SubscriberDecl sd -> write_sub_decl buf sd
  | CommitDecl cd -> write_commit_decl buf cd
  | ResultDecl rd -> write_result_decl buf rd
  | _ -> Result.fail Error.NotImplemented


let read_declarations buf =
  let open IOBuf in
  let open Result in

  let rec loop buf n ds =
    if n = 0 then return (ds, buf)
    else
        (do_
        ; (d, buf) <-- read_declaration buf
        ; (loop buf (n-1) (d::ds)))
  in
  (do_
  ; () ; Lwt.ignore_result @@ Lwt_log.debug "Reading Declarations"
  ; (len, buf) <-- get_vle buf
  ; () ; Lwt.ignore_result @@ Lwt_log.debug @@ Printf.sprintf "Parsing %Ld declarations" len
  ; loop buf (Vle.to_int len) [])

let write_declarations buf ds =
  let open Declare in
  let open IOBuf in
  let open Result in
  (do_
  ; () ; Lwt.ignore_result @@ Lwt_log.debug "Writing Declarations"
  ; buf <-- put_vle buf  @@ Vle.of_int @@ List.length @@ ds
  ; fold_m (fun b d -> write_declaration b d) buf ds)

let read_declare buf h =
  let open IOBuf in
  let open Result in
  (do_
  ; () ; Lwt.ignore_result @@ Lwt_log.debug "Reading Declare message"
  ; (sn, buf) <-- get_vle buf
  ; () ; Lwt.ignore_result @@ Lwt_log.debug @@ Printf.sprintf "Declare sn = %Ld" sn
  ; (ds, buf) <-- read_declarations buf
  ; return ((Declare.create sn ds (Flags.hasFlag h Flags.sFlag) (Flags.hasFlag h Flags.cFlag)), buf))

let write_declare buf decl =
  let open Declare in
  let open IOBuf in
  let open Result in
  (do_
  ; () ; Lwt.ignore_result @@ Lwt_log.debug "Writing Declare message"
  ; buf <-- put_char buf (header decl)
  ; buf <-- put_vle buf (sn decl)
  ; buf <-- write_declarations buf (declarations decl)
  ; return buf)

let read_stream_data buf h =
  let open IOBuf in
  let open Result in
  (do_
  ; (sn, buf) <-- get_vle buf
  ; (id, buf) <-- get_vle buf
  ; (prid, buf) <-- if Flags.hasFlag h Flags.aFlag then get_vle buf <>>= (fun v -> Some v) else return (None, buf)
  ; (payload, buf) <-- get_io_buf buf
  ; (r, s) <-- return ((Flags.hasFlag h Flags.sFlag), (Flags.hasFlag h Flags.rFlag))
  ; return  ((StreamData.create (r, s) sn id prid payload),buf))

let write_stream_data buf m =
  let open StreamData in
  let open IOBuf in
  let open Result in
  (do_
  ; buf <-- put_char buf @@ header m
  ; buf <-- put_vle buf @@ sn m
  ; buf <-- put_vle buf @@ id m
  ; buf <-- (match prid m with | None -> return buf | Some v -> put_vle buf v )
  ; put_io_buf buf @@ payload m)

let read_synch buf h =
  let open IOBuf in
  let open Result in
  (do_
  ; (sn, buf) <-- get_vle buf
  ; (s, r) <-- return Flags.(hasFlag h sFlag, hasFlag h rFlag)
  ; if Flags.hasFlag h Flags.uFlag then
      get_vle buf
      <>>= (fun c -> Synch.create (s,r) sn (Some c))
    else
      return (Synch.create (s,r) sn None, buf))

let write_synch buf m =
  let open Synch in
  let open IOBuf in
  let open Result in
  (do_
  ; buf <-- put_char buf @@ header m
  ; buf <-- put_vle buf @@ sn m
  ; match count m  with | None -> return buf | Some c -> put_vle buf c)

let read_ack_nack buf h =
  let open IOBuf in
  let open Result in
  (do_
  ; (sn, buf) <-- get_vle buf
  ; if Flags.(hasFlag h mFlag) then
      get_vle buf <>>= (fun m -> AckNack.create sn @@ Some m)
    else
      return (AckNack.create sn None, buf))

let write_ack_nack buf m =
  let open AckNack in
  let open IOBuf in
  let open Result in
  (do_
  ; buf <-- put_char buf @@ header m
  ; buf <-- put_vle buf @@ sn m
  ; match mask m with | None -> return buf | Some v -> put_vle buf v)

let read_msg buf =
  let open Result in
  (do_
  ; (header, buf) <-- IOBuf.get_char buf
  ; () ; Lwt.ignore_result @@ Lwt_log.debug (Printf.sprintf "Received message with id: %d\n" (Header.mid header))
  ; (match char_of_int (Header.mid (header)) with
     | id when id = MessageId.scoutId ->  (read_scout buf header) <>>= make_scout
     | id when id = MessageId.helloId ->  (read_hello buf header) <>>= make_hello
     | id when id = MessageId.openId ->  (read_open buf header) <>>= make_open
     | id when id = MessageId.acceptId -> (read_accept buf header) <>>= make_accept
     | id when id = MessageId.closeId ->  (read_close buf header) <>>= make_close
     | id when id = MessageId.declareId -> (read_declare buf header) <>>= make_declare
     | id when id = MessageId.sdataId ->  (read_stream_data buf header) <>>= make_stream_data
     | id when id = MessageId.synchId -> (read_synch buf header) <>>= make_synch
     | id when id = MessageId.ackNackId -> (read_ack_nack buf header) <>>= make_ack_nack
     | uid ->
       Lwt.ignore_result (Lwt_log.warning @@ Printf.sprintf "Received unknown message id: %d" (int_of_char uid))
       ; Result.fail Error.(InvalidFormat NoMsg)))


let write_msg buf msg =
  match msg with
  | Scout m -> write_scout buf m
  | Hello m -> write_hello buf m
  | Open m -> write_open buf m
  | Accept m -> write_accept buf m
  | Close m -> write_close buf m
  | Declare m -> write_declare buf m
  | StreamData m -> write_stream_data buf m
  | Synch m -> write_synch buf m
  | AckNack m -> write_ack_nack buf m
