open Apero
open Message
open Locator
open Lwt

module VleMap = Map.Make(Vle)

type sublistener = IOBuf.t -> string -> unit Lwt.t
type queryreply = 
  | StorageData of {stoid:IOBuf.t; rsn:int; resname:string; data:IOBuf.t}
  | StorageFinal of {stoid:IOBuf.t; rsn:int}
  | ReplyFinal 
type reply_handler = queryreply -> unit Lwt.t
type query_handler = string -> string -> (string * IOBuf.t) list Lwt.t

type insub = {subid:int; resid:Vle.t; listener:sublistener}

type insto = {stoid:int; resname:PathExpr.t; listener:sublistener; qhandler:query_handler}

type resource = {rid: Vle.t; name: PathExpr.t; matches: Vle.t list; subs: insub list; stos : insto list;}

type query = {qid: Vle.t; listener:reply_handler;}

let with_match res mrid = 
  {res with matches = mrid :: List.filter (fun r -> r != mrid) res.matches}

(* let remove_match res mrid = 
  {res with matches = List.filter (fun r -> r != mrid) res.matches} *)

let with_sub sub res = 
  {res with subs = sub :: List.filter (fun s -> s != sub) res.subs}

let remove_sub subid res = 
  {res with subs = List.filter (fun s -> s.subid != subid) res.subs}

let with_sto sto res = 
  {res with stos = sto :: List.filter (fun s -> s != sto) res.stos}

let remove_sto stoid res = 
  {res with stos = List.filter (fun s -> s.stoid != stoid) res.stos}
  

type state = {
  next_sn : Vle.t;
  next_pubsub_id : int;
  next_res_id : Vle.t;
  next_qry_id : Vle.t;
  resmap : resource VleMap.t;
  qrymap : query VleMap.t;
}

let create_state = {next_sn=1L; next_pubsub_id=0; next_res_id=0L; next_qry_id=0L; resmap=VleMap.empty; qrymap=VleMap.empty}

let get_next_sn state = (state.next_sn, {state with next_sn=Vle.add state.next_sn 1L})
let get_next_entity_id state = (state.next_pubsub_id, {state with next_pubsub_id=(state.next_pubsub_id + 1)})
let get_next_res_id state = (state.next_res_id, {state with next_res_id=Vle.add state.next_res_id 1L})
let get_next_qry_id state = (state.next_qry_id, {state with next_qry_id=Vle.add state.next_qry_id 1L})

type t = {
  sock : Lwt_unix.file_descr;
  state : state Lwt_mvar.t;
}

type sub = {z:t; id:int; resid:Vle.t;}
type pub = {z:t; id:int; resid:Vle.t; reliable:bool}
type storage = {z:t; id:int; resid:Vle.t;}

type submode = SubscriptionMode.t

let lbuf = IOBuf.create 16
let wbuf = IOBuf.create 8192
let rbuf = IOBuf.create 8192


let pid  = IOBuf.flip @@ 
  Result.get @@ IOBuf.put_string (Uuidm.to_bytes @@ Uuidm.v5 (Uuidm.create `V4) (string_of_int @@ Unix.getpid ())) @@
  (IOBuf.create 32) 

let lease = 0L
let version = Char.chr 0x01


let match_resource rmap mres = 
  VleMap.fold (fun _ res x -> 
    let (rmap, mres) = x in
    match PathExpr.intersect mres.name res.name with 
    | true -> 
      let mres = with_match mres res.rid in 
      let rmap = VleMap.add res.rid (with_match res mres.rid) rmap in 
      (rmap, mres)
    | false -> x) rmap (rmap, mres)

let add_resource resname state = 
  let (rid, state) = get_next_res_id state in 
  let res =  {rid; name=resname; matches=[rid]; subs=[]; stos=[]} in
  let (resmap, res) = match_resource state.resmap res in
  let resmap = VleMap.add rid res resmap in 
  (res, {state with resmap})


(* let make_hello = Message.Hello (Hello.create (Vle.of_char ScoutFlags.scoutBroker) Locators.empty []) *)
let make_open = Message.Open (Open.create version pid lease Locators.empty [])
(* let make_accept opid = Message.Accept (Accept.create opid pid lease []) *)

let send_message sock msg =
  let open Result.Infix in
  let wbuf = IOBuf.clear wbuf
  and lbuf = IOBuf.clear lbuf in
  
  let wbuf = Result.get (Mcodec.encode_msg msg wbuf >>> IOBuf.flip) in

  let len = IOBuf.limit wbuf in
  let lbuf = Result.get (encode_vle (Vle.of_int len) lbuf >>> IOBuf.flip) in
  
  let%lwt _ = Net.write_all sock lbuf in
  Net.write_all sock wbuf


let process_incoming_message msg resolver t = 
  let open Lwt.Infix in
  match msg with
  | Message.Accept _ -> Lwt.wakeup_later resolver t; return_true
  | Message.StreamData dmsg ->
    let%lwt state = Lwt_mvar.take t.state in
    let%lwt _ = Lwt_mvar.put t.state state in 
    let%lwt _ = match VleMap.find_opt (StreamData.id dmsg) state.resmap with
    | Some res -> 
      (* TODO make sure that payload is a copy *)
      (* TODO make payload a readonly buffer *)
      let buf = StreamData.payload dmsg in
      Lwt_list.iter_s (fun resid -> 
        match VleMap.find_opt resid state.resmap with
        | Some res -> 
          let%lwt _ = Lwt_list.iter_s (fun (sub:insub) -> 
            sub.listener buf (PathExpr.to_string res.name)
          ) res.subs in
          Lwt_list.iter_s (fun (sto:insto) -> 
            sto.listener buf (PathExpr.to_string res.name)
          ) res.stos
        | None -> Lwt.return_unit 
      ) res.matches
    | None -> Lwt.return_unit in
    return_true
  | Message.WriteData dmsg ->
    let datapath = PathExpr.of_string @@ WriteData.resource dmsg in
    let%lwt state = Lwt_mvar.take t.state in
    let%lwt _ = Lwt_mvar.put t.state state in 
    (* TODO make sure that payload is a copy *)
    (* TODO make payload a readonly buffer *)
    let buf = WriteData.payload dmsg in
    let%lwt _ = Lwt_list.iter_s (fun (_, res) -> 
      match PathExpr.intersect res.name datapath with 
      | true -> 
          let%lwt _ = Lwt_list.iter_s (fun (sub:insub) ->
            sub.listener buf (WriteData.resource dmsg)
          ) res.subs in
          Lwt_list.iter_s (fun (sto:insto) -> 
            sto.listener buf (WriteData.resource dmsg)
          ) res.stos
      | false -> return_unit) (VleMap.bindings state.resmap) in
      return_true
  | Message.Query qmsg -> 
    let querypath = PathExpr.of_string @@ Query.resource qmsg in
    let%lwt state = Lwt_mvar.take t.state in
    let%lwt _ = Lwt_mvar.put t.state state in 
    let%lwt _ = Lwt_list.iter_s (fun (_, res) -> 
      match PathExpr.intersect res.name querypath with 
      | true -> 
          Lwt_list.fold_left_s (fun rsn (sto:insto) ->
            sto.qhandler (Query.resource qmsg) (Query.predicate qmsg)
            >>= Lwt_list.fold_left_s (fun rsn (resname, payload) -> 
              send_message t.sock (Message.Reply(Reply.create (Query.pid qmsg) (Query.qid qmsg) (Some (pid, rsn, resname, payload))))
              >>= fun _ -> Lwt.return (Vle.add rsn Vle.one)) rsn
          ) Vle.zero res.stos
          >>= fun rsn -> 
          send_message t.sock (Message.Reply(Reply.create (Query.pid qmsg) (Query.qid qmsg) (Some (pid, rsn, "", IOBuf.create 0))))
          >>= fun _ -> 
          send_message t.sock (Message.Reply(Reply.create (Query.pid qmsg) (Query.qid qmsg) None))
          >>= fun _ -> return_unit
      | false -> return_unit) (VleMap.bindings state.resmap) in
    return_true    
  | Message.Reply rmsg -> 
    (match String.equal (IOBuf.hexdump (Reply.qpid rmsg)) (IOBuf.hexdump pid) with 
    | false -> return_true
    | true -> 
      let%lwt state = Lwt_mvar.take t.state in
      let%lwt _ = Lwt_mvar.put t.state state in 
      match VleMap.find_opt (Reply.qid rmsg) state.qrymap with 
      | None -> return_true 
      | Some query -> 
        (match (Message.Reply.value rmsg) with 
        | None -> query.listener ReplyFinal >>= fun _ -> return_true
        | Some (stoid, rsn, resname, payload) -> 
          (match IOBuf.available payload with 
          | 0 -> query.listener (StorageFinal({stoid; rsn=(Vle.to_int rsn)})) >>= fun _ -> return_true
          | _ -> query.listener (StorageData({stoid; rsn=(Vle.to_int rsn); resname; data=payload})) >>= fun _ -> return_true)))
  | msg ->
    Logs.debug (fun m -> m "\n[received: %s]\n>> " (Message.to_string msg));  
    return_true

let get_message_length sock buf =
  let rec extract_length buf v bc =
    let buf = Result.get @@ IOBuf.reset_with  0 1 buf in
    match%lwt Net.read_all sock buf with
    | 0 -> fail @@ Exception(`ClosedSession (`Msg "Peer closed the session unexpectedly"))
    | _ ->
      let (b, buf) = Result.get (IOBuf.get_char buf) in
      match int_of_char b with
      | c when c <= 0x7f -> return (v lor (c lsl (bc * 7)))
      | c  -> extract_length buf (v lor ((c land 0x7f) lsl bc)) (bc + 1)
  in extract_length buf 0 0

let rec run_decode_loop resolver t = 
  let%lwt len = get_message_length t.sock rbuf in
  let%lwt _ = Logs_lwt.debug (fun m -> m ">>> Received message of %d bytes" len) in
  let rbuf = Result.get @@ IOBuf.set_position 0 rbuf in
  let rbuf = Result.get @@ IOBuf.set_limit len rbuf in
  let%lwt _ = Net.read_all t.sock rbuf in
  let%lwt _ =  Logs_lwt.debug (fun m -> m "tx-received: %s "  (IOBuf.to_string rbuf)) in
  let (msg, _) = Result.get @@ Mcodec.decode_msg rbuf in
  let%lwt _ = process_incoming_message msg resolver t in
  run_decode_loop resolver t
  
let safe_run_decode_loop resolver t =  
  try%lwt
    run_decode_loop resolver t
  with
  | x ->
    let%lwt _ = Logs_lwt.warn (fun m -> m "Exception in decode loop : %s\n%s" (Printexc.to_string x) (Printexc.get_backtrace ()) ) in
    try%lwt
      let%lwt _ = Lwt_unix.close t.sock in
      fail @@ Exception (`ClosedSession (`Msg (Printexc.to_string x)))
    with
    | _ -> 
      fail @@ Exception (`ClosedSession (`Msg (Printexc.to_string x)))
  
let (>>) a b = a >>= fun x -> x |> fun _ -> b  

let zopen peer = 
  let open Lwt_unix in
  let sock = socket PF_INET SOCK_STREAM 0 in
  setsockopt sock SO_REUSEADDR true;
  setsockopt sock TCP_NODELAY true;
  let saddr = Scanf.sscanf peer "%[^/]/%[^:]:%d" (fun _ ip port -> 
    ADDR_INET (Unix.inet_addr_of_string ip, port)) in
  let name_info = Unix.getnameinfo saddr [NI_NUMERICHOST; NI_NUMERICSERV] in
  let _ = Logs_lwt.debug (fun m -> m "peer : tcp/%s:%s" name_info.ni_hostname name_info.ni_service) in
  let (promise, resolver) = Lwt.task () in
  let con = connect sock saddr in 
  let _ = con >>= fun _ -> safe_run_decode_loop resolver {sock; state=Lwt_mvar.create create_state} in
  let _ = con >>= fun _ -> send_message sock make_open in
  con >>= fun _ -> promise

let info z =
  let peer = match Unix.getpeername @@ Lwt_unix.unix_file_descr z.sock with
    | ADDR_UNIX a -> "unix:"^a
    | ADDR_INET (a,p) -> Printf.sprintf "%s:%d" (Unix.string_of_inet_addr a) p
  in
  Apero.Properties.singleton "peer" peer

let publish resname z = 
  let resname = PathExpr.of_string resname in
  let%lwt state = Lwt_mvar.take z.state in
  let (res, state) = add_resource resname state in
  let (pubid, state) = get_next_entity_id state in

  let (sn, state) = get_next_sn state in
  let _ = send_message z.sock (Message.Declare(Declare.create (true, false) sn [
    ResourceDecl(ResourceDecl.create res.rid (PathExpr.to_string res.name) []);
    PublisherDecl(PublisherDecl.create res.rid [])
  ])) in 

  let%lwt _ = Lwt_mvar.put z.state state in
  Lwt.return {z; id=pubid; resid=res.rid; reliable=false}


let write buf resname z = 
  let%lwt state = Lwt_mvar.take z.state in
  let (sn, state) = get_next_sn state in
  let%lwt _ = Lwt_mvar.put z.state state in
  send_message z.sock (Message.WriteData(WriteData.create (false, false) sn resname buf))
  >> Lwt.return_unit


let stream buf (pub:pub) = 
  let%lwt state = Lwt_mvar.take pub.z.state in
  let (sn, state) = get_next_sn state in
  let%lwt _ = Lwt_mvar.put pub.z.state state in
  send_message pub.z.sock (Message.StreamData(StreamData.create (false, pub.reliable) sn pub.resid None buf))
  >> Lwt.return_unit


let unpublish (pub:pub) z = 
  let%lwt state = Lwt_mvar.take z.state in
  let (sn, state) = get_next_sn state in
  let%lwt _ = send_message z.sock (Message.Declare(Declare.create (true, false) sn [
    ForgetPublisherDecl(ForgetPublisherDecl.create pub.resid)
  ])) in 
  Lwt_mvar.put z.state state 


let push_mode = SubscriptionMode.push_mode
let pull_mode = SubscriptionMode.pull_mode


let subscribe resname listener ?(mode=push_mode) z = 
  let resname = PathExpr.of_string resname in
  let%lwt state = Lwt_mvar.take z.state in
  let (res, state) = add_resource resname state in
  let (subid, state) = get_next_entity_id state in
  let insub = {subid; resid=res.rid; listener} in
  let res = with_sub insub res in
  let resmap = VleMap.add res.rid res state.resmap in 
  let state = {state with resmap} in

  let (sn, state) = get_next_sn state in
  let _ = send_message z.sock (Message.Declare(Declare.create (true, false) sn [
    ResourceDecl(ResourceDecl.create res.rid (PathExpr.to_string res.name) []);
    SubscriberDecl(SubscriberDecl.create res.rid mode [])
  ])) in 

  let%lwt _ = Lwt_mvar.put z.state state in
  let sub : sub = {z=z; id=subid; resid=res.rid} in
  Lwt.return sub


let pull (sub:sub) = 
  let%lwt state = Lwt_mvar.take sub.z.state in
  let (sn, state) = get_next_sn state in 
  let%lwt _ = Lwt_mvar.put sub.z.state state in
  let%lwt _ = send_message sub.z.sock (Message.Pull(Pull.create (true, true) sn sub.resid None)) in 
  Lwt.return_unit


let unsubscribe (sub:sub) z = 
  let%lwt state = Lwt_mvar.take z.state in
  let state = match VleMap.find_opt sub.resid state.resmap with 
  | None -> state 
  | Some res -> 
    let res = remove_sub sub.id res in 
    let resmap = VleMap.add res.rid res state.resmap in 
    {state with resmap} in
  let (sn, state) = get_next_sn state in
  let%lwt _ = send_message z.sock (Message.Declare(Declare.create (true, false) sn [
    ForgetSubscriberDecl(ForgetSubscriberDecl.create sub.resid)
  ])) in 
  Lwt_mvar.put z.state state 


let storage resname listener qhandler z = 
  let resname = PathExpr.of_string resname in
  let%lwt state = Lwt_mvar.take z.state in
  let (res, state) = add_resource resname state in
  let (stoid, state) = get_next_entity_id state in
  let insto = {stoid; resname; listener; qhandler} in
  let res = with_sto insto res in
  let resmap = VleMap.add res.rid res state.resmap in 
  let state = {state with resmap} in

  let (sn, state) = get_next_sn state in
  let _ = send_message z.sock (Message.Declare(Declare.create (true, false) sn [
    ResourceDecl(ResourceDecl.create res.rid (PathExpr.to_string res.name) []);
    StorageDecl(StorageDecl.create res.rid [])
  ])) in 

  let%lwt _ = Lwt_mvar.put z.state state in
  Lwt.return {z=z; id=stoid; resid=res.rid}


let query resname predicate listener ?(quorum=(-1)) ?(max_samples=(-1)) z = 
  let%lwt state = Lwt_mvar.take z.state in
  let (qryid, state) = get_next_qry_id state in
  let qrymap = VleMap.add qryid {qid=qryid; listener} state.qrymap in 
  let%lwt _ = send_message z.sock (Message.Query(Query.create pid qryid resname predicate (Vle.of_int quorum) (Some (Vle.of_int max_samples)))) in 
  let state = {state with qrymap} in
  Lwt_mvar.put z.state state 


let unstore (sto:storage) z = 
  let%lwt state = Lwt_mvar.take z.state in
  let state = match VleMap.find_opt sto.resid state.resmap with 
  | None -> state 
  | Some res -> 
    let res = remove_sto sto.id res in 
    let resmap = VleMap.add res.rid res state.resmap in 
    {state with resmap} in
  let (sn, state) = get_next_sn state in
  let%lwt _ = send_message z.sock (Message.Declare(Declare.create (true, false) sn [
    ForgetStorageDecl(ForgetStorageDecl.create sto.resid)
  ])) in 
  Lwt_mvar.put z.state state 