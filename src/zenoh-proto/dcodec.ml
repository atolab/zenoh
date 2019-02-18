open Apero
open Message
open Pcodec
open Block
(* open Pcodec *)


let encode_properties ps buf = 
  match ps with 
  | [] -> () 
  | ps -> (Apero.encode_seq encode_property) ps buf
  

let decode_properties h  buf =
  if Flags.(hasFlag h pFlag) then 
    decode_seq decode_property buf     
  else []


let make_res_decl rid resource ps = Declaration.ResourceDecl (ResourceDecl.create rid resource ps)

let decode_res_decl header buf=
  let v = fast_decode_vle buf in 
  let s = decode_string buf in 
  let ds = decode_properties header buf in 
  make_res_decl v s ds 
    
let encode_res_decl d buf =
  let open ResourceDecl in  
  MIOBuf.put_char (header d) buf;
  fast_encode_vle (rid d) buf;
  encode_string (resource d) buf;
  encode_properties (properties d) buf
  
let make_pub_decl rid ps = Declaration.PublisherDecl (PublisherDecl.create rid ps)

let decode_pub_decl header buf =   
  let v = fast_decode_vle buf in 
  let ps = decode_properties header buf in
  make_pub_decl v ps

let encode_pub_decl d buf =
  let open PublisherDecl in  
  let id = (rid d) in
  MIOBuf.put_char (header d) buf;
  fast_encode_vle id buf;
  encode_properties (properties d) buf

let make_temporal_properties origin period duration = TemporalProperty.create origin period duration

let decode_temporal_properties =
  read3_spec
    (Logs.debug (fun m -> m "Reading TemporalProperties"))
    decode_vle
    decode_vle
    decode_vle
    make_temporal_properties
  
let encode_temporal_properties stp buf =
  let open TemporalProperty in
  match stp with
  | None -> ()
  | Some tp ->
    Logs.debug (fun m -> m "Writing Temporal") ;
    fast_encode_vle (origin tp) buf;
    fast_encode_vle (period tp) buf;
    fast_encode_vle (duration tp) buf
    
let decode_sub_mode buf =
  match MIOBuf.get_char buf with   
  | id when  Flags.mid id = SubscriptionModeId.pushModeId ->
    SubscriptionMode.PushMode
  | id when  Flags.mid id =  SubscriptionModeId.pullModeId ->
    SubscriptionMode.PullMode
  | id when  Flags.mid id =  SubscriptionModeId.periodicPushModeId ->
    let tp = decode_temporal_properties buf in 
    SubscriptionMode.PeriodicPushMode tp
  | id when  Flags.mid id =  SubscriptionModeId.periodicPullModeId ->
    let tp = decode_temporal_properties buf in       
    SubscriptionMode.PeriodicPullMode tp      
  | _ -> raise @@ Apero.Exception `UnknownSubMode
  
let encode_sub_mode m buf =
  let open SubscriptionMode in  
  MIOBuf.put_char (id m) buf;
  encode_temporal_properties (temporal_properties m) buf

let make_sub_decl rid mode ps = Declaration.SubscriberDecl (SubscriberDecl.create rid mode ps)

let decode_sub_decl header =
  read3_spec
    (Logs.debug (fun m -> m "Reading SubDeclaration"))
    decode_vle
    decode_sub_mode
    (decode_properties header)
    make_sub_decl
  
let encode_sub_decl d buf =
  let open SubscriberDecl in 
  let id = (rid d) in 
  MIOBuf.put_char (header d) buf;
  fast_encode_vle id buf;
  encode_sub_mode (mode d) buf;
  encode_properties (properties d) buf  

let make_selection_decl h sid query ps = 
  Declaration.SelectionDecl (SelectionDecl.create sid query ps (Flags.(hasFlag h gFlag)))

let decode_selection_decl header = 
  read3_spec
    (Logs.debug (fun m -> m "Reading SelectionDeclaration"))
    decode_vle  
    decode_string
    (decode_properties header)
    (make_selection_decl header)
    
let encode_selection_decl d buf =
  let open SelectionDecl in
  Logs.debug (fun m -> m "Writing SelectionDeclaration");
  MIOBuf.put_char (header d) buf;
  fast_encode_vle (sid d) buf;
  encode_string (query d) buf;
  encode_properties (properties d) buf
  
let make_binding_decl h oldid newid = 
  Declaration.BindingDecl (BindingDecl.create oldid newid (Flags.(hasFlag h gFlag)))

let decode_binding_decl header =  
  read2_spec
    (Logs.debug (fun m -> m "Reading BindingDeclaration"))
    decode_vle
    decode_vle
    (make_binding_decl header)

let encode_bindind_decl d buf =
  let open BindingDecl in  
  MIOBuf.put_char (header d) buf;
  fast_encode_vle (old_id d) buf;
  fast_encode_vle (new_id d) buf

let make_commit_decl commit_id = (Declaration.CommitDecl (CommitDecl.create commit_id))

let decode_commit_decl = 
  read1_spec 
    (Logs.debug (fun m -> m "Reading Commit Declaration"))
    MIOBuf.get_char
    make_commit_decl
  
let encode_commit_decl cd buf =
  let open CommitDecl in  
  MIOBuf.put_char (header cd) buf;
  MIOBuf.put_char (commit_id cd) buf
  
let decode_result_decl buf =  
  let commit_id = MIOBuf.get_char buf in   
  match MIOBuf.get_char buf with 
  | status when status = char_of_int 0 ->
      Declaration.ResultDecl  (ResultDecl.create commit_id status None)
  | status ->
    let v = fast_decode_vle buf in 
      Declaration.ResultDecl (ResultDecl.create commit_id status (Some v))
        
  
let encode_result_decl rd buf =
  let open ResultDecl in  
  Logs.debug (fun m -> m "Writing Result Declaration") ;
  MIOBuf.put_char (header rd) buf;
  MIOBuf.put_char (commit_id rd) buf;
  MIOBuf.put_char (status rd) buf;  
  match (id rd) with 
  | None -> ()
  | Some v -> fast_encode_vle v buf

let decode_forget_res_decl buf =
  Logs.debug (fun m -> m "Reading ForgetResource Declaration");
  let rid = fast_decode_vle buf in 
  Declaration.ForgetResourceDecl (ForgetResourceDecl.create rid)
  
let encode_forget_res_decl frd buf =
  let open ForgetResourceDecl in
  MIOBuf.put_char (header frd) buf;
  fast_encode_vle (rid frd) buf

let decode_forget_pub_decl buf =
  let id = fast_decode_vle buf in 
  Declaration.ForgetPublisherDecl (ForgetPublisherDecl.create id)
  
let encode_forget_pub_decl fpd buf =
  let open ForgetPublisherDecl in
  MIOBuf.put_char (header fpd) buf;
  fast_encode_vle (id fpd) buf
  
let decode_forget_sub_decl buf =
  let id = fast_decode_vle buf in   
  Declaration.ForgetSubscriberDecl (ForgetSubscriberDecl.create id)
  
let encode_forget_sub_decl fsd buf =
  let open ForgetSubscriberDecl in  
  MIOBuf.put_char (header fsd) buf;
  fast_encode_vle (id fsd) buf

let decode_forget_sel_decl buf =
  let sid = fast_decode_vle buf in  
  Declaration.ForgetSelectionDecl (ForgetSelectionDecl.create sid)
    
let encode_forget_sel_decl fsd buf =
  let open ForgetSelectionDecl in
  MIOBuf.put_char (header fsd) buf;
  fast_encode_vle (sid fsd) buf

let make_storage_decl rid ps = Declaration.StorageDecl (StorageDecl.create rid ps)

let decode_storage_decl header = 
  read2_spec 
  (Logs.debug (fun m -> m "Reading StorageDeclaration"))
  decode_vle
  (decode_properties header)
  make_storage_decl

let encode_storage_decl d buf =
  let open StorageDecl in
  let id = (rid d) in
  MIOBuf.put_char (header d) buf;
  fast_encode_vle id buf;
  encode_properties (properties d) buf

let decode_forget_storage_decl buf =
  let id = fast_decode_vle buf in 
  Declaration.ForgetStorageDecl (ForgetStorageDecl.create id)
  
let encode_forget_storage_decl fsd buf =
  let open ForgetStorageDecl in  
  MIOBuf.put_char (header fsd) buf;
  fast_encode_vle (id fsd) buf
