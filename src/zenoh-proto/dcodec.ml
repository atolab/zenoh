open Apero
open Apero.ResultM
open Apero.ResultM.InfixM
open Ztypes
open Zproperty
open Pcodec
open Ziobuf
open Tcodec
open Zmessage
open Zmessage.Message
open Zmessage.Marker
open Zframe
let make_res_decl rid resource ps = Declaration.ResourceDecl (ResourceDecl.create rid resource ps)

let decode_res_decl header = 
  read3_spec 
    (Logs.debug (fun m -> m "Reading ResourceDeclaration"))
    Tcodec.decode_vle
    Tcodec.decode_string
    (decode_properties header)
    make_res_decl
    
let encode_res_decl d buf=
  let open ResourceDecl in
  Logs.debug (fun m -> m "Writing ResourceDeclaration") ;
  IOBuf.put_char (header d) buf 
  >>= Tcodec.encode_vle (rid d)
  >>= Tcodec.encode_string (resource d)
  >>= encode_properties (properties d)
  
let make_pub_decl rid ps = Declaration.PublisherDecl (PublisherDecl.create rid ps)

let decode_pub_decl header = 
  read2_spec 
  (Logs.debug (fun m -> m "Reading PubDeclaration"))
  Tcodec.decode_vle
  (decode_properties header)
  make_pub_decl

let encode_pub_decl d buf =
  let open PublisherDecl in
  Logs.debug (fun m -> m "Writing PubDeclaration") ;
  let id = (rid d) in
  Logs.debug (fun m -> m  "Writing PubDeclaration for rid = %Ld" id) ;
  IOBuf.put_char (header d) buf
  >>= Tcodec.encode_vle id 
  >>= encode_properties (properties d)

let make_temporal_properties origin period duration = TemporalProperties.create origin period duration

let decode_temporal_properties =
  read3_spec
    (Logs.debug (fun m -> m "Reading TemporalProperties"))
    Tcodec.decode_vle
    Tcodec.decode_vle
    Tcodec.decode_vle
    make_temporal_properties
  
let encode_temporal_properties stp buf =
  let open TemporalProperties in
  match stp with
  | None -> return buf
  | Some tp ->
    Logs.debug (fun m -> m "Writing Temporal") ;
    Tcodec.encode_vle (origin tp) buf 
    >>= Tcodec.encode_vle (period tp)
    >>= Tcodec.encode_vle (duration tp)
    
let decode_sub_mode buf =
  Logs.debug (fun m -> m "Reading SubMode") ;
  IOBuf.get_char buf 
  >>= (function 
      | (id, buf) when  Flags.mid id = SubscriptionModeId.pushModeId ->
        return (SubscriptionMode.PushMode, buf)

      | (id, buf) when  Flags.mid id =  SubscriptionModeId.pullModeId ->
        return (SubscriptionMode.PullMode, buf)

      | (id, buf) when  Flags.mid id =  SubscriptionModeId.periodicPushModeId ->
        decode_temporal_properties buf
        >>= (fun (tp, buf) -> 
          return (SubscriptionMode.PeriodicPushMode tp, buf))

      | (id, buf) when  Flags.mid id =  SubscriptionModeId.periodicPullModeId ->
        decode_temporal_properties buf
        >>= (fun (tp, buf) -> 
          return (SubscriptionMode.PeriodicPullMode tp, buf))
      
      | _ -> fail Error.UnknownSubMode)
  
  

let encode_sub_mode m buf =
  let open SubscriptionMode in
  Logs.debug (fun m -> m "Writing SubMode") ;  
  IOBuf.put_char (id m) buf
  >>= encode_temporal_properties (temporal_properties m)

let make_sub_decl rid mode ps = Declaration.SubscriberDecl (SubscriberDecl.create rid mode ps)

let decode_sub_decl header =
  read3_spec
    (Logs.debug (fun m -> m "Reading SubDeclaration"))
    Tcodec.decode_vle
    decode_sub_mode
    (decode_properties header)
    make_sub_decl
  
let encode_sub_decl d buf =
  let open SubscriberDecl in
  Logs.debug (fun m -> m "Writing SubDeclaration") ;
  let id = (rid d) in
  Logs.debug (fun m -> m "Writing SubDeclaration for rid = %Ld" id) ;
  IOBuf.put_char (header d) buf
  >>= Tcodec.encode_vle id
  >>= encode_sub_mode (mode d)
  >>= encode_properties (properties d)


let make_selection_decl h sid query ps = 
  Declaration.SelectionDecl (SelectionDecl.create sid query ps (Flags.(hasFlag h gFlag)))

let decode_selection_decl header = 
  read3_spec
    (Logs.debug (fun m -> m "Reading SelectionDeclaration"))
    Tcodec.decode_vle  
    Tcodec.decode_string
    (decode_properties header)
    (make_selection_decl header)
    
let encode_selection_decl d buf =
  let open SelectionDecl in
  Logs.debug (fun m -> m "Writing SelectionDeclaration");
  IOBuf.put_char (header d) buf
  >>= Tcodec.encode_vle (sid d)
  >>= Tcodec.encode_string (query d) 
  >>= encode_properties (properties d)
  
let make_binding_decl h oldid newid = 
  Declaration.BindingDecl (BindingDecl.create oldid newid (Flags.(hasFlag h gFlag)))

let decode_binding_decl header =  
  read2_spec
    (Logs.debug (fun m -> m "Reading BindingDeclaration"))
    Tcodec.decode_vle
    Tcodec.decode_vle
    (make_binding_decl header)

let encode_bindind_decl d buf =
  let open BindingDecl in
  Logs.debug (fun m -> m "Writing BindingDeclaration") ;
  IOBuf.put_char (header d) buf
  >>= Tcodec.encode_vle (old_id d) 
  >>= Tcodec.encode_vle (new_id d)

let make_commit_decl commit_id = (Declaration.CommitDecl (CommitDecl.create commit_id))

let decode_commit_decl = 
  read1_spec 
    (Logs.debug (fun m -> m "Reading Commit Declaration"))
    IOBuf.get_char
    make_commit_decl
  
let encode_commit_decl cd buf =
  let open CommitDecl in  
  Logs.debug (fun m -> m "Writing Commit Declaration");
  IOBuf.put_char (header cd) buf
  >>= IOBuf.put_char (commit_id cd)
  
let decode_result_decl buf =  
  Logs.debug (fun m -> m "Reading Result Declaration");
  IOBuf.get_char buf 
  >>= (fun (commit_id, buf) -> 
    IOBuf.get_char buf
    >>= (function 
      | (status, buf) when status = char_of_int 0 ->
        return (Declaration.ResultDecl  (ResultDecl.create commit_id status None), buf)
      | (status, buf) ->
        decode_vle buf
        >>= (fun (v, buf) ->
          return (Declaration.ResultDecl (ResultDecl.create commit_id status (Some v)), buf))))
    
  
let encode_result_decl rd buf =
  let open ResultDecl in  
  Logs.debug (fun m -> m "Writing Result Declaration") ;
  IOBuf.put_char (header rd) buf
  >>= IOBuf.put_char (commit_id rd)
  >>= IOBuf.put_char (status rd) 
  >>= (fun buf -> 
    match (id rd) with 
    | None -> return buf 
    | Some v -> Tcodec.encode_vle v buf)

let decode_forget_res_decl buf =
  Logs.debug (fun m -> m "Reading ForgetResource Declaration");
  Tcodec.decode_vle buf
  >>= (fun (rid, buf) -> 
    return (Declaration.ForgetResourceDecl (ForgetResourceDecl.create rid), buf))
  
let encode_forget_res_decl frd buf =
  let open ForgetResourceDecl in
  Logs.debug (fun m -> m "Writing ForgetResource Declaration");
  IOBuf.put_char (header frd) buf
  >>= Tcodec.encode_vle (rid frd)  

let decode_forget_pub_decl buf =
  Logs.debug (fun m -> m "Reading ForgetPublisher Declaration");
   Tcodec.decode_vle buf
   >>= (fun (id, buf) -> 
    return (Declaration.ForgetPublisherDecl (ForgetPublisherDecl.create id), buf))
  
let encode_forget_pub_decl fpd buf =
  let open ForgetPublisherDecl in
  Logs.debug (fun m -> m "Writing ForgetPublisher Declaration");
  IOBuf.put_char (header fpd) buf 
  >>= Tcodec.encode_vle (id fpd)
  
let decode_forget_sub_decl buf =
  Logs.debug (fun m -> m "Reading ForgetSubscriber Declaration");
  Tcodec.decode_vle buf
  >>= (fun (id, buf) -> 
    return (Declaration.ForgetSubscriberDecl (ForgetSubscriberDecl.create id), buf))
  
let encode_forget_sub_decl fsd buf =
  let open ForgetSubscriberDecl in
  Logs.debug (fun m -> m "Writing ForgetSubscriber Declaration");
  IOBuf.put_char (header fsd) buf
  >>= Tcodec.encode_vle (id fsd)

let decode_forget_sel_decl buf =
  Logs.debug (fun m -> m "Reading ForgetSelection Declaration") ;
  Tcodec.decode_vle buf
  >>= (fun (sid, buf) -> 
    return (Declaration.ForgetSelectionDecl (ForgetSelectionDecl.create sid), buf))
    


let encode_forget_sel_decl fsd buf =
  let open ForgetSelectionDecl in
  Logs.debug (fun m -> m "Writing ForgetSelection Declaration" );   
  IOBuf.put_char (header fsd) buf 
  >>= Tcodec.encode_vle (sid fsd) 
