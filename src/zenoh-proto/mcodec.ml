open Apero
open Apero.ResultM
open Apero.ResultM.InfixM
open Ztypes
open Locator
open Property
open Iobuf
open Message
open Lcodec
open Pcodec
open Dcodec
open Frame


type element =
  | Message of Message.t
  | Marker of Marker.t

let make_scout mask ps = Message (Scout (Scout.create mask ps))

let decode_scout header = 
  read2_spec
    (Logs.debug (fun m -> m "Reading Scout"))
    Tcodec.decode_vle 
    (Pcodec.decode_properties header) 
    make_scout

let encode_scout scout buf  =
  let open Scout in
  Logs.debug (fun m -> m "Writring Scout") ;
  IOBuf.put_char (header scout) buf
  >>= Tcodec.encode_vle (mask scout) 
  >>= encode_properties (properties scout)  

let make_hello mask ls ps = Message (Hello (Hello.create mask ls ps))

let decode_hello header =
  read3_spec
    (Logs.debug (fun m -> m "Readings Hello"))    
    Tcodec.decode_vle
    decode_locators     
    (decode_properties header)    
    make_hello

let encode_hello hello buf =
  let open Hello in
  Logs.debug (fun m -> m "Writing Hello") ;
  IOBuf.put_char (header hello) buf
  >>= Tcodec.encode_vle (mask hello)
  >>= encode_locators (locators hello)
  >>= encode_properties  (properties hello)

let make_open version pid lease locs ps = Message (Open (Open.create version pid lease locs ps))

let decode_open header =      
  (read5_spec 
    (Logs.debug (fun m -> m "Reading Open"))
    IOBuf.get_char
    Tcodec.decode_bytes
    Tcodec.decode_vle
    decode_locators
    (decode_properties header)
    make_open)
  

let encode_open msg buf =
  let open Open in
  Logs.debug (fun m -> m "Writing Open") ;
  match IOBuf.put_char (header msg) buf 
  >>= IOBuf.put_char (version msg)
  >>= Tcodec.encode_bytes (pid msg) 
  >>= Tcodec.encode_vle (lease msg)
  >>= Tcodec.encode_locators (locators msg)
  >>= Pcodec.encode_properties (properties msg) 
  with 
  | Ok v as b -> b
  | Error e as f ->
    Logs.debug (fun m -> m "Failed to encode Open: %s" (Error.show_e e)); f 


let make_accept opid apid lease ps = Message (Accept (Accept.create opid apid lease ps))
let decode_accept header =
  read4_spec 
    (Logs.debug (fun m -> m"Reading Accept"))
    Tcodec.decode_bytes
    Tcodec.decode_bytes
    Tcodec.decode_vle 
    (decode_properties header)
    make_accept  
  
let encode_accept accept buf =
  let open Accept in
  Logs.debug (fun m -> m "Writing Accept") ;
  IOBuf.put_char (header accept) buf
  >>= Tcodec.encode_bytes (opid accept)
  >>= Tcodec.encode_bytes (apid accept)
  >>= Tcodec.encode_vle (lease accept)
  >>= Pcodec.encode_properties (properties accept)

let make_close pid reason = Message (Close (Close.create pid reason))
let decode_close header = 
  read2_spec
    (Logs.debug (fun m -> m "Reading Close"))
    Tcodec.decode_bytes
    IOBuf.get_char
    make_close

let encode_close close buf =
  let open Close in
  Logs.debug (fun m -> m "Writing Close") ;
  IOBuf.put_char (header close) buf
  >>= Tcodec.encode_bytes (pid close) 
  >>= IOBuf.put_char (reason close)   

let decode_declaration buf =  
  Logs.debug (fun m -> m "Reading Declaration");
   IOBuf.get_char buf
   >>= (fun (header, buf) -> 
      Logs.debug (fun m -> m "Declaration Id = %d" (Header.mid header) );
      match Flags.mid header with
      | r when r = DeclarationId.resourceDeclId -> decode_res_decl header buf
      | p when p = DeclarationId.publisherDeclId -> decode_pub_decl header buf 
      | s when s = DeclarationId.subscriberDeclId -> decode_sub_decl header buf
      | s when s = DeclarationId.selectionDeclId -> decode_selection_decl header buf
      | b when b = DeclarationId.bindingDeclId -> decode_binding_decl header buf 
      | c when c = DeclarationId.commitDeclId -> decode_commit_decl buf 
      | r when r = DeclarationId.resultDeclId -> decode_result_decl  buf 
      | r when r = DeclarationId.forgetResourceDeclId -> decode_forget_res_decl buf
      | r when r = DeclarationId.forgetPublisherDeclId -> decode_forget_pub_decl  buf
      | r when r = DeclarationId.forgetSubscriberDeclId -> decode_forget_sub_decl  buf 
      | r when r = DeclarationId.forgetSelectionDeclId -> decode_forget_sel_decl buf 
      | _ -> fail Error.NotImplemented
   ) 
  
  


let encode_declaration (d: Declaration.t) buf=
  match d with
  | ResourceDecl rd -> encode_res_decl rd buf
  | PublisherDecl pd -> encode_pub_decl pd buf 
  | SubscriberDecl sd -> encode_sub_decl sd buf 
  | SelectionDecl sd -> encode_selection_decl sd buf
  | BindingDecl bd -> encode_bindind_decl bd buf 
  | CommitDecl cd -> encode_commit_decl cd buf 
  | ResultDecl rd -> encode_result_decl rd buf
  | ForgetResourceDecl frd -> encode_forget_res_decl  frd buf
  | ForgetPublisherDecl fpd -> encode_forget_pub_decl fpd buf 
  | ForgetSubscriberDecl fsd -> encode_forget_sub_decl fsd buf 
  | ForgetSelectionDecl fsd -> encode_forget_sel_decl fsd buf 


let decode_declarations buf = 
  let rec loop  n ds buf = 
    if n = 0 then return (ds, buf)
    else 
      decode_declaration buf 
      >>= (fun (d, buf) -> loop (n-1) (d::ds) buf)
  in 
    Logs.debug (fun m -> m "Reading Declarations");
    Tcodec.decode_vle buf 
    >>= (fun (len, buf) -> 
      Logs.debug (fun m -> m "Parsing %Ld declarations" len);
      loop (Vle.to_int len) [] buf)
    
let encode_declarations ds buf =
  let open Declare in
  Logs.debug (fun m -> m "Writing Declarations");  
  Tcodec.encode_vle  (Vle.of_int @@ List.length ds) buf
  >>= (fold_m (fun d b -> encode_declaration d b) ds)

let make_declare h sn ds = 
  Message (Declare (Declare.create ((Flags.hasFlag h Flags.sFlag), (Flags.hasFlag h Flags.cFlag)) sn ds))

let decode_declare header =
  read2_spec 
    (Logs.debug (fun m -> m "Reading Declare message"))
    Tcodec.decode_vle
    decode_declarations
    (make_declare header)

  
let encode_declare decl buf=
  let open Declare in  
  Logs.debug (fun m -> m "Writing Declare message");
  IOBuf.put_char (header decl) buf
  >>= Tcodec.encode_vle (sn decl)
  >>= encode_declarations (declarations decl)
  

let make_encode_data h sn resource payload = 
  let (s, r) = ((Flags.hasFlag h Flags.sFlag), (Flags.hasFlag h Flags.rFlag)) in
  Message (WriteData (WriteData.create (s, r) sn resource payload))

let decode_encode_data header =
  read3_spec 
    (Logs.debug (fun m -> m "Reading WriteData"))
    Tcodec.decode_vle
    Tcodec.decode_string
    Tcodec.decode_bytes
    (make_encode_data header)
  
let encode_encode_data m buf =
  let open WriteData in
  Logs.debug (fun m -> m "Writing WriteData");
  IOBuf.put_char (header m) buf 
  >>= Tcodec.encode_vle @@ sn m 
  >>= Tcodec.encode_string @@ resource m 
  >>= Tcodec.encode_bytes  @@ payload m

let decode_prid h buf = 
  if Flags.(hasFlag h aFlag) then
    Tcodec.decode_vle buf 
    >>= (fun (v, b) -> return (Some v, b))
  else return (None, buf)

let encode_prid  = function
  | None -> return
  | Some v -> Tcodec.encode_vle v

let make_stream_data h sn id prid payload = 
  let (s, r) = ((Flags.hasFlag h Flags.sFlag), (Flags.hasFlag h Flags.rFlag)) in
  Message (StreamData (StreamData.create (s, r) sn id prid payload))

let decode_stream_data header =
  read4_spec 
    (Logs.debug (fun m -> m "Reading StreamData"))
    Tcodec.decode_vle
    Tcodec.decode_vle
    (decode_prid header)
    Tcodec.decode_bytes
    (make_stream_data header)
 
let encode_stream_data m buf =
  let open StreamData in
  Logs.debug (fun m -> m "Writing StreamData");
  IOBuf.put_char (header m) buf
  >>= Tcodec.encode_vle @@ sn m
  >>= Tcodec.encode_vle @@ id m
  >>= (encode_prid @@ prid m)
  >>= Tcodec.encode_bytes @@ payload m

let decode_synch_count h buf = 
  if Flags.(hasFlag h uFlag) then 
    Tcodec.decode_vle buf
    >>= (fun (v, b) -> return (Some v,b))
  else 
    return (None, buf)
  
let make_synch h sn c= 
  let  (s, r) = Flags.(hasFlag h sFlag, hasFlag h rFlag) in
  Message (Synch (Synch.create (s,r) sn c))

let decode_synch header =
  read2_spec 
    (Logs.debug (fun m -> m "Reading Synch"))
    Tcodec.decode_vle
    (decode_synch_count header)
    (make_synch header)
    

let encode_synch m buf =
  let open Synch in  
  Logs.debug (fun m -> m "Writing Synch");
  IOBuf.put_char (header m)  buf
  >>= Tcodec.encode_vle @@ sn m
  >>= match count m  with
  | None -> return 
  | Some c -> Tcodec.encode_vle c
  

let make_ack sn m = Message (AckNack (AckNack.create sn m))

let decode_acknack_mask h buf =
  if Flags.(hasFlag h mFlag) then
    Tcodec.decode_vle buf
    >>= (fun (m, buf) -> return (Some m, buf))
  else return (None, buf)
    

let decode_ack_nack header =
  read2_spec 
    (Logs.debug (fun m -> m "Reading AckNack"))
    Tcodec.decode_vle
    (decode_acknack_mask header)
    make_ack

let encode_ack_nack m buf =
  let open AckNack in
  
  Logs.debug (fun m -> m "Writing AckNack");
  IOBuf.put_char (header m) buf
  >>= Tcodec.encode_vle (sn m)
  >>= match mask m with
    | None -> return 
    | Some v -> Tcodec.encode_vle v

let decode_keep_alive header buf =
  Logs.debug (fun m -> m "Reading KeepAlive");
  Tcodec.decode_bytes buf
  >>= (fun (pid, buf) -> return (Message (KeepAlive (KeepAlive.create pid)), buf))
  
let encode_keep_alive keep_alive buf =
  let open KeepAlive in  
  Logs.debug (fun m -> m "Writing KeepAlive");
  IOBuf.put_char (header keep_alive) buf
  >>= Tcodec.encode_bytes (pid keep_alive)

let decode_migrate_id h buf = 
  if Flags.(hasFlag h iFlag) then
    Tcodec.decode_vle buf
    >>= (fun (id, buf) -> return (Some id, buf))
  else return (None, buf)

let make_migrate ocid id rch_last_sn bech_last_sn =
  Message (Migrate (Migrate.create ocid id rch_last_sn bech_last_sn))

let decode_migrate header =
  read4_spec
    (Logs.debug (fun m -> m "Reading Migrate"))
    Tcodec.decode_vle
    (decode_migrate_id header)
    Tcodec.decode_vle 
    Tcodec.decode_vle 
    make_migrate

let encode_migrate m buf =
  let open Migrate in  
  Logs.debug (fun m -> m "Writing Migrate");
  IOBuf.put_char (header m) buf
  >>= Tcodec.encode_vle (ocid m)
  >>=  (match id m with
    | None -> return
    | Some id -> Tcodec.encode_vle id)
  >>= Tcodec.encode_vle @@ rch_last_sn m
  >>= Tcodec.encode_vle  @@ bech_last_sn m 
  

let decode_pull_max_samples header buf = 
  if Flags.(hasFlag header nFlag) then
    Tcodec.decode_vle buf
    >>= (fun (max_samples, buf) -> return (Some max_samples, buf))
  else return (None, buf)

let make_pull header sn id max_samples = 
  let (s, f) = ((Flags.hasFlag header Flags.sFlag), (Flags.hasFlag header Flags.fFlag)) in
  Message (Pull (Pull.create (s, f) sn id max_samples))

let decode_pull header =
  read3_spec
    (Logs.debug (fun m -> m "Reading Pull"))
    Tcodec.decode_vle
    Tcodec.decode_vle
    (decode_pull_max_samples header)
    (make_pull header)
  
let encode_pull m buf =
  let open Pull in  
  Logs.debug (fun m -> m "Writing Pull");
  IOBuf.put_char (header m) buf 
  >>= Tcodec.encode_vle @@ sn m
  >>=  Tcodec.encode_vle @@ id m
  >>=  match max_samples m with
    | None -> return 
    | Some max -> Tcodec.encode_vle max

let decode_ping_pong header buf =  
  Logs.debug (fun m -> m "Reading PingPong");
  let o = Flags.hasFlag header Flags.oFlag in
  Tcodec.decode_vle buf
  >>= (fun (hash, buf) ->     
    return (Message (PingPong (PingPong.create ~pong:o hash)), buf))
  
let encode_ping_pong  m buf=
  let open PingPong in
  Logs.debug (fun m -> m "Writing PingPong");
  IOBuf.put_char (header m) buf 
  >>= Tcodec.encode_vle @@ hash m  

let decode_compact_id header buf = 
  (* @AC: Olivier the conduit marker should always have a cid, that should not be 
         optional. The way in which it is encoded changes, but not the fact of 
         having an id... *)
  if Flags.(hasFlag header zFlag) then
    let flags = (int_of_char (Flags.flags header)) lsr Flags.mid_len in 
    let cid = Vle.of_int @@ (flags land 0x3) + 1 in 
    return (cid, buf)
  else 
    Tcodec.decode_vle buf
    >>= (fun (id, buf) -> return (id, buf))

let decode_conduit header buf = 
  Logs.debug (fun m -> m "Reading Conduit") ;
  decode_compact_id header buf 
  >>= fun (id, buf) -> return (Marker (ConduitMarker (ConduitMarker.create id)), buf)
  
let encode_conduit m buf = 
  let open ConduitMarker in
  Logs.debug (fun m -> m "Writing Conduit");
  IOBuf.put_char (header m) buf
  >>= match Flags.hasFlag (header m) Flags.zFlag with 
    | true -> return 
    | false -> Tcodec.encode_vle @@ id m
  

let decode_frag_num header buf = 
  if Flags.(hasFlag header nFlag) then
    Tcodec.decode_vle  buf 
    >>= fun (n, buf) -> return (Some n, buf)
  else 
    return (None, buf) 
  
let make_frag sn_base n = Marker (Frag (Frag.create sn_base n))  

let decode_frag header =   
  read2_spec 
    (Logs.debug (fun m -> m "Reading Frag"))
    Tcodec.decode_vle
    (decode_frag_num header)
    make_frag 
  
let encode_frag m buf = 
  let open Frag in  
  Logs.debug (fun m ->  m "Writing Frag");
  IOBuf.put_char (header m) buf
  >>= Tcodec.encode_vle @@ sn_base m
  >>= match n m with 
    | Some n -> Tcodec.encode_vle n
    | None -> return   

let decode_rspace header buf = 
  Logs.debug (fun m -> m "Reading ResourceSpace");
  decode_compact_id header buf 
  >>= fun (id, buf) -> return (Marker (RSpace (RSpace.create id)), buf)
  
let encode_rspace m buf = 
  let open RSpace in  
  Logs.debug (fun m -> m "Writing ResourceSpace");
  IOBuf.put_char (header m) buf 
  >>= if Flags.(hasFlag (header m) zFlag) then Tcodec.encode_vle (id m) 
      else return
  


let decode_element buf =
  IOBuf.get_char buf
  >>= (fun (header, buf) -> 
    Logs.debug (fun m -> m "Received message with id: %d\n" (Header.mid header));
    match char_of_int (Header.mid (header)) with
    | id when id = MessageId.scoutId ->  (decode_scout header buf) 
    | id when id = MessageId.helloId ->  (decode_hello header buf)
    | id when id = MessageId.openId ->  (decode_open header buf)
    | id when id = MessageId.acceptId -> (decode_accept header buf)
    | id when id = MessageId.closeId ->  (decode_close header buf)
    | id when id = MessageId.declareId -> (decode_declare header buf)
    | id when id = MessageId.wdataId ->  (decode_encode_data header buf)
    | id when id = MessageId.sdataId ->  (decode_stream_data header buf)
    | id when id = MessageId.synchId -> (decode_synch header buf)
    | id when id = MessageId.ackNackId -> (decode_ack_nack header buf)
    | id when id = MessageId.keepAliveId -> (decode_keep_alive header buf)
    | id when id = MessageId.migrateId -> (decode_migrate header buf)
    | id when id = MessageId.pullId -> (decode_pull header buf)
    | id when id = MessageId.pingPongId -> (decode_ping_pong header buf)
    | id when id = MessageId.conduitId -> (decode_conduit header buf)
    | id when id = MessageId.fragmetsId -> (decode_frag header buf)
    | id when id = MessageId.rSpaceId -> (decode_rspace header buf)
    | uid ->
      Logs.debug (fun m -> m "Received unknown message id: %d" (int_of_char uid));
      fail Error.UnknownMessageId)


let rec decode_msg_rec buf markers = 
  decode_element buf
  >>= (fun (elem, buf) ->
    match elem with 
    | Marker m -> decode_msg_rec buf (m :: markers)
    | Message m -> return (Message.with_markers m markers, buf))

let decode_msg buf = decode_msg_rec buf []

let encode_marker marker =
  let open Marker in
  match marker with
  | ConduitMarker c -> encode_conduit c
  | Frag c -> encode_frag c
  | RSpace c -> encode_rspace c

let encode_msg_element msg =
  let open Message in
  match msg with
  | Scout m -> encode_scout m
  | Hello m -> encode_hello m
  | Open m -> encode_open  m
  | Accept m -> encode_accept m
  | Close m -> encode_close  m
  | Declare m -> encode_declare m
  | WriteData m -> encode_encode_data m
  | StreamData m -> encode_stream_data m
  | Synch m -> encode_synch  m
  | AckNack m -> encode_ack_nack m
  | KeepAlive m -> encode_keep_alive  m
  | Migrate m -> encode_migrate  m
  | Pull m -> encode_pull m
  | PingPong m -> encode_ping_pong m
    

let encode_msg msg buf =
  let open Message in
  let rec encode_msg_wm msg markers buf =
    match markers with 
    | marker :: markers -> 
      encode_marker marker buf
      >>= encode_msg_wm msg markers
    | [] -> encode_msg_element msg buf
  in  encode_msg_wm msg (markers msg) buf


let decode_frame_length = Tcodec.decode_vle 

let encode_frame_length f = Tcodec.encode_vle (Vle.of_int @@ Frame.length f)

let decode_frame buf =
  let rec rloop n msgs buf =
    if n = 0 then return (msgs, buf)
    else
      decode_msg buf
      >>= (fun (msg, buf) -> rloop (n-1) (msg::msgs) buf)
      
  in
    decode_frame_length buf 
    >>= (fun (len, buf) -> 
      rloop  (Vle.to_int len) [] buf
      >>= fun (msgs, buf)  -> return (Frame.create msgs, buf))
    
let encode_frame f =  
  fold_m encode_msg (Frame.to_list f)