open Apero
open Apero.Infix
open Ztypes
open Message
open Locator
open Lwt

module VleMap = Map.Make(Vle)

type sublistener = string -> (Abuf.t * data_info) list -> unit Lwt.t
type queryreply = 
  | StorageData of {stoid:Abuf.t; rsn:int; resname:string; data:Abuf.t; info:data_info}
  | StorageFinal of {stoid:Abuf.t; rsn:int}
  | ReplyFinal 
type reply_handler = queryreply -> unit Lwt.t
type query_handler = string -> string -> (string * Abuf.t * data_info) list Lwt.t

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

type session = | Sock of Lwt_unix.file_descr | Stream of (Frame.Frame.t Lwt_stream.t * Frame.Frame.t Lwt_stream.bounded_push)

let lbuf = Abuf.create_bigstring 16
let wbuf = Abuf.create_bigstring ~grow:8192 (64*1024)
let rbuf = Abuf.create_bigstring ~grow:8192 (64*1024)

let send_message sock msg =
  match sock with 
  | Sock sock -> 
    Abuf.clear wbuf;
    Abuf.clear lbuf;
    Mcodec.encode_msg msg wbuf;
    let len = Abuf.readable_bytes wbuf in
    fast_encode_vle (Vle.of_int len) lbuf;
    Net.write_all sock (Abuf.wrap [lbuf; wbuf]) 
    >>= fun _ -> Lwt.return_unit
  | Stream (_, push) -> 
    push#push @@ Frame.Frame.create [msg]

let read_messages = function
  | Sock sock -> 
    let%lwt len = Net.read_vle sock >>= fun v -> Vle.to_int v |> Lwt.return in
    let%lwt _ = Logs_lwt.debug (fun m -> m ">>> Received message of %d bytes" len) in
    Abuf.clear rbuf;
    let%lwt _ = Net.read_all sock rbuf len in
    let%lwt _ =  Logs_lwt.debug (fun m -> m "tx-received: %s "  (Abuf.to_string rbuf)) in
    Lwt.return @@ [Mcodec.decode_msg rbuf]
  | Stream (stream, _) -> 
    Lwt_stream.get stream >>= (Option.get %> Lwt.return) >>= (Frame.Frame.to_list %> Lwt.return)

let close = function
  | Sock sock -> Lwt_unix.close sock 
  | Stream _ -> Lwt.return_unit

type t = {
  sock : session;
  peer_pid : string option;
  state : state Guard.t;
}

type sub = {z:t; id:int; resid:Vle.t;}
type pub = {z:t; id:int; resid:Vle.t; reliable:bool}
type storage = {z:t; id:int; resid:Vle.t;}

type submode = SubscriptionMode.t


let pid  = 
  Abuf.create_bigstring 32 |> fun buf -> 
  Abuf.write_bytes (Bytes.unsafe_of_string ((Uuidm.to_bytes @@ Uuidm.v5 (Uuidm.create `V4) (string_of_int @@ Unix.getpid ())))) buf; 
  buf

let lease = 0L
let version = Char.chr 0x01

let clean_query z qryid = 
  let%lwt state = Guard.acquire z.state in
  let qrymap = VleMap.remove qryid state.qrymap in 
  let state = {state with qrymap} in
  Lwt.return @@ Guard.release z.state state

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

let process_incoming_message msg resolver t = 
  let open Lwt.Infix in
  match msg with
  | Message.Accept amsg -> Lwt.wakeup_later resolver {t with peer_pid=Some (Abuf.hexdump @@ Message.Accept.apid amsg)}; return_true
  | Message.BatchedStreamData dmsg ->
    let state = Guard.get t.state in
    (* let%lwt _ = Lwt_mvar.put t.state state in  *)
    let%lwt _ = match VleMap.find_opt (BatchedStreamData.id dmsg) state.resmap with
    | Some res -> 
      (* TODO make sure that payload is a copy *)
      (* TODO make payload a readonly buffer *)
      let payloads = BatchedStreamData.payload dmsg in
      Lwt_list.iter_s (fun resid -> 
        match VleMap.find_opt resid state.resmap with
        | Some res -> 
          let%lwt _ = Lwt_list.iter_s (fun (sub:insub) -> 
            Lwt.catch (fun () -> sub.listener (PathExpr.to_string res.name) (List.map (fun p -> (Payload.data p), (Payload.header p)) payloads)) 
                      (fun e -> Logs_lwt.info (fun m -> m "Subscriber listener raised exception %s" (Printexc.to_string e)))
            |> Lwt.ignore_result; Lwt.return_unit
          ) res.subs in
          Lwt_list.iter_s (fun (sto:insto) -> 
            Lwt.catch (fun () -> sto.listener (PathExpr.to_string res.name) (List.map (fun p -> (Payload.data p), (Payload.header p)) payloads)) 
                      (fun e -> Logs_lwt.info (fun m -> m "Storage listener raised exception %s" (Printexc.to_string e)))
            |> Lwt.ignore_result; Lwt.return_unit
          ) res.stos
        | None -> Lwt.return_unit 
      ) res.matches
    | None -> Lwt.return_unit in
    return_true
  | Message.StreamData dmsg ->
    let state = Guard.get t.state in
    (* let%lwt _ = Lwt_mvar.put t.state state in  *)
    let%lwt _ = match VleMap.find_opt (StreamData.id dmsg) state.resmap with
    | Some res -> 
      (* TODO make sure that payload is a copy *)
      (* TODO make payload a readonly buffer *)
      let payload = StreamData.payload dmsg in
      Lwt_list.iter_s (fun resid -> 
        match VleMap.find_opt resid state.resmap with
        | Some res -> 
          let%lwt _ = Lwt_list.iter_s (fun (sub:insub) -> 
            Lwt.catch (fun () -> sub.listener (PathExpr.to_string res.name) [(Payload.data payload), (Payload.header payload)]) 
                      (fun e -> Logs_lwt.info (fun m -> m "Subscriber listener raised exception %s" (Printexc.to_string e)))
            |> Lwt.ignore_result; Lwt.return_unit
          ) res.subs in
          Lwt_list.iter_s (fun (sto:insto) -> 
            Lwt.catch (fun () -> sto.listener (PathExpr.to_string res.name) [(Payload.data payload), (Payload.header payload)]) 
                      (fun e -> Logs_lwt.info (fun m -> m "Storage listener raised exception %s" (Printexc.to_string e)))
            |> Lwt.ignore_result; Lwt.return_unit
          ) res.stos
        | None -> Lwt.return_unit 
      ) res.matches
    | None -> Lwt.return_unit in
    return_true
  | Message.CompactData dmsg ->
    let state = Guard.get t.state in
    (* let%lwt _ = Lwt_mvar.put t.state state in  *)
    let%lwt _ = match VleMap.find_opt (CompactData.id dmsg) state.resmap with
    | Some res -> 
      (* TODO make sure that payload is a copy *)
      (* TODO make payload a readonly buffer *)
      let payload = CompactData.payload dmsg in
      Lwt_list.iter_s (fun resid -> 
        match VleMap.find_opt resid state.resmap with
        | Some res -> 
          let%lwt _ = Lwt_list.iter_s (fun (sub:insub) -> 
            Lwt.catch (fun () -> sub.listener (PathExpr.to_string res.name) [(Payload.data payload), empty_data_info]) 
                      (fun e -> Logs_lwt.info (fun m -> m "Subscriber listener raised exception %s" (Printexc.to_string e)))
            |> Lwt.ignore_result; Lwt.return_unit
          ) res.subs in
          Lwt_list.iter_s (fun (sto:insto) -> 
            Lwt.catch (fun () -> sto.listener (PathExpr.to_string res.name) [(Payload.data payload), empty_data_info])
                      (fun e -> Logs_lwt.info (fun m -> m "Storage listener raised exception %s" (Printexc.to_string e)))
            |> Lwt.ignore_result; Lwt.return_unit
          ) res.stos
        | None -> Lwt.return_unit 
      ) res.matches
    | None -> Lwt.return_unit in
    return_true
  | Message.WriteData dmsg ->
    let datapath = PathExpr.of_string @@ WriteData.resource dmsg in
    let state = Guard.get t.state in    
    (* TODO make sure that payload is a copy *)
    (* TODO make payload a readonly buffer *)
    let payload = WriteData.payload dmsg in
    let%lwt _ = Lwt_list.iter_s (fun (_, res) -> 
      match PathExpr.intersect res.name datapath with 
      | true -> 
          let%lwt _ = Lwt_list.iter_s (fun (sub:insub) ->
            Lwt.catch (fun () -> sub.listener (WriteData.resource dmsg) [(Payload.data payload), (Payload.header payload)]) 
                      (fun e -> Logs_lwt.info (fun m -> m "Subscriber listener raised exception %s" (Printexc.to_string e)))
            |> Lwt.ignore_result; Lwt.return_unit
          ) res.subs in
          Lwt_list.iter_s (fun (sto:insto) -> 
            Lwt.catch (fun () -> sto.listener (WriteData.resource dmsg) [(Payload.data payload), (Payload.header payload)]) 
                      (fun e -> Logs_lwt.info (fun m -> m "Storage listener raised exception %s" (Printexc.to_string e)))
            |> Lwt.ignore_result; Lwt.return_unit
          ) res.stos
      | false -> return_unit) (VleMap.bindings state.resmap) in
      return_true
  | Message.Query qmsg -> 
    let querypath = PathExpr.of_string @@ Query.resource qmsg in
    let state = Guard.get t.state in    
    Lwt_list.iter_s (fun (_, res) -> 
      match PathExpr.intersect res.name querypath with 
      | true -> 
          Lwt_list.fold_left_s (fun rsn (sto:insto) ->
            Lwt.catch(fun () -> sto.qhandler (Query.resource qmsg) (Query.predicate qmsg)) 
                     (fun e -> Logs_lwt.info (fun m -> m "Storage query handler raised exception %s" (Printexc.to_string e)) >>= fun () -> Lwt.return [])
                     (* TODO propagate query failures *)
            >>= Lwt_list.fold_left_s (fun rsn (resname, data, ctx) -> 
              send_message t.sock (Message.Reply(Reply.create (Query.pid qmsg) (Query.qid qmsg) (Some (pid, rsn, resname, Payload.create ~header:ctx data))))
              >>= fun _ -> Lwt.return (Vle.add rsn Vle.one)) rsn
          ) Vle.zero res.stos
          >>= fun rsn -> 
          send_message t.sock (Message.Reply(Reply.create (Query.pid qmsg) (Query.qid qmsg) (Some (pid, rsn, "", Payload.create ~header:empty_data_info @@ Abuf.create 0))))
          >>= fun _ -> return_unit
      | false -> return_unit) (VleMap.bindings state.resmap)
    >>= fun () -> send_message t.sock (Message.Reply(Reply.create (Query.pid qmsg) (Query.qid qmsg) None)) 
    |> Lwt.ignore_result; Lwt.return_true    
  | Message.Reply rmsg -> 
    (match String.equal (Abuf.hexdump (Reply.qpid rmsg)) (Abuf.hexdump pid) with 
    | false -> return_true
    | true -> 
      let state = Guard.get t.state in    
      match VleMap.find_opt (Reply.qid rmsg) state.qrymap with 
      | None -> return_true 
      | Some query -> 
        (match (Message.Reply.value rmsg) with 
        | None -> Lwt.catch(fun () -> query.listener ReplyFinal) 
                           (fun e -> Logs_lwt.info (fun m -> m "Reply handler raised exception %s" (Printexc.to_string e)))
                  |> Lwt.ignore_result;
                  clean_query t (Reply.qid rmsg)
        | Some (stoid, rsn, resname, payload) -> 
          let data = Payload.data payload in
          (match Abuf.readable_bytes data with 
          | 0 -> Lwt.catch(fun () -> query.listener (StorageFinal({stoid; rsn=(Vle.to_int rsn)})))
                          (fun e -> Logs_lwt.info (fun m -> m "Reply handler raised exception %s" (Printexc.to_string e)))
                 |> Lwt.ignore_result; Lwt.return_unit
          | _ -> Lwt.catch(fun () -> query.listener (StorageData({stoid; rsn=(Vle.to_int rsn); resname; data; info=Payload.header payload})))
                          (fun e -> Logs_lwt.info (fun m -> m "Reply handler raised exception %s" (Printexc.to_string e)))
                 |> Lwt.ignore_result; Lwt.return_unit
          )) >>= fun _ -> return_true )
  | msg ->
    Logs.debug (fun m -> m "\n[received: %s]\n>> " (Message.to_string msg));  
    return_true

let rec run_decode_loop resolver t = 
  let%lwt msgs = read_messages t.sock in
  let%lwt () = Lwt_list.iter_s (fun msg -> 
    process_incoming_message msg resolver t >>= fun _ -> Lwt.return_unit) msgs in
  run_decode_loop resolver t
  
let safe_run_decode_loop resolver t =  
  try%lwt
    run_decode_loop resolver t
  with
  | x ->
    let%lwt _ = Logs_lwt.warn (fun m -> m "Exception in decode loop : %s\n%s" (Printexc.to_string x) (Printexc.get_backtrace ()) ) in
    try%lwt
      let%lwt _ = close t.sock in
      fail @@ Exception (`ClosedSession (`Msg (Printexc.to_string x)))
    with
    | _ -> 
      fail @@ Exception (`ClosedSession (`Msg (Printexc.to_string x)))

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
  let sock = Sock(sock) in
  let _ = con >>= fun _ -> safe_run_decode_loop resolver {sock; state=Guard.create create_state; peer_pid=None;} in
  let _ = con >>= fun _ -> send_message sock make_open in
  con >>= fun _ -> promise

let zropen stream = 
let sock = Stream(stream) in
  let (promise, resolver) = Lwt.task () in
  let _ = safe_run_decode_loop resolver {sock; state=Guard.create create_state; peer_pid=None;} in 
  let _ = send_message sock make_open in
  promise

let info z =
  let peer = 
  match z.sock with 
  | Sock sock -> 
    (match Unix.getpeername @@ Lwt_unix.unix_file_descr sock with
    | ADDR_UNIX a -> "unix:"^a
    | ADDR_INET (a,p) -> Printf.sprintf "%s:%d" (Unix.string_of_inet_addr a) p)
  | Stream _ -> "local"
  in
  let props = Apero.Properties.of_list ["peer", peer; "pid",(Abuf.hexdump pid)]  in 
  match z.peer_pid with 
  | None -> props 
  | Some bpid -> Apero.Properties.add "peer_pid" bpid props

let publish z resname = 
  let resname = PathExpr.of_string resname in
  let%lwt state = Guard.acquire z.state in
  let (res, state) = add_resource resname state in
  let (pubid, state) = get_next_entity_id state in

  let (sn, state) = get_next_sn state in
  let _ = send_message z.sock (Message.Declare(Declare.create (true, false) sn [
    ResourceDecl(ResourceDecl.create res.rid (PathExpr.to_string res.name) []);
    PublisherDecl(PublisherDecl.create res.rid [])
  ])) in 
  Guard.release z.state state;
  Lwt.return {z; id=pubid; resid=res.rid; reliable=false}


let write z resname ?timestamp ?kind ?encoding buf = 
  let%lwt state = Guard.acquire z.state in
  let (sn, state) = get_next_sn state in
  let%lwt _ = send_message z.sock (Message.WriteData(WriteData.create (false, false) sn resname (Payload.create ~header:{ts=timestamp; encoding; kind; srcid=None; srcsn=None; bkrid=None; bkrsn=None} buf))) in
  Lwt.return @@ Guard.release z.state state

let stream (pub:pub) ?timestamp ?kind ?encoding buf = 
  let%lwt state = Guard.acquire pub.z.state in
  let (sn, state) = get_next_sn state in
  let%lwt _ = send_message pub.z.sock (Message.StreamData(StreamData.create (false, pub.reliable) sn pub.resid (Payload.create ~header:{ts=timestamp; encoding; kind; srcid=None; srcsn=None; bkrid=None; bkrsn=None} buf))) in
  Lwt.return @@ Guard.release pub.z.state state

let lstream (pub:pub) (bufs: Abuf.t list) = 
  let%lwt state = Guard.acquire pub.z.state in
  let (sn, state) = get_next_sn state in
  let payloads = List.map (fun b -> Payload.create b) bufs in
  let%lwt _ = send_message pub.z.sock (Message.BatchedStreamData(BatchedStreamData.create (false, pub.reliable) sn pub.resid payloads)) in
  Lwt.return @@ Guard.release pub.z.state state


let unpublish z (pub:pub) = 
  let%lwt state = Guard.acquire z.state in
  let (sn, state) = get_next_sn state in
  let%lwt _ = send_message z.sock (Message.Declare(Declare.create (true, false) sn [
    ForgetPublisherDecl(ForgetPublisherDecl.create pub.resid)
  ])) in 
  Lwt.return @@ Guard.release z.state state


let push_mode = SubscriptionMode.push_mode
let pull_mode = SubscriptionMode.pull_mode


let subscribe z ?(mode=push_mode)  resname listener = 
  let resname = PathExpr.of_string resname in
  let%lwt state = Guard.acquire z.state in
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
  Guard.release z.state state ;
  Lwt.return ({z=z; id=subid; resid=res.rid}:sub)


let pull (sub:sub) = 
  let%lwt state = Guard.acquire sub.z.state in
  let (sn, state) = get_next_sn state in 
  let%lwt _ = send_message sub.z.sock (Message.Pull(Pull.create (true, true) sn sub.resid None)) in 
  Lwt.return @@ Guard.release sub.z.state state


let unsubscribe z (sub:sub) = 
  let%lwt state = Guard.acquire z.state in
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
  Lwt.return @@ Guard.release z.state state 


let store z resname listener qhandler = 
  let resname = PathExpr.of_string resname in
  let%lwt state = Guard.acquire z.state in
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
  Guard.release z.state state ;
  Lwt.return {z=z; id=stoid; resid=res.rid} 



let query z ?(dest=Partial)  resname predicate listener = 
  let%lwt state = Guard.acquire z.state in
  let (qryid, state) = get_next_qry_id state in
  let qrymap = VleMap.add qryid {qid=qryid; listener} state.qrymap in 
  let props = [ZProperty.QueryDest.make dest] in
  let state = {state with qrymap} in
  let%lwt _ = send_message z.sock (Message.Query(Query.create pid qryid resname predicate props)) in
  Lwt.return @@ Guard.release z.state state


let squery z ?(dest=Partial) resname predicate = 
  let stream, push = Lwt_stream.create () in 
  let reply_handler qreply = push @@ Some qreply; Lwt.return_unit in 
  let _ = (query z resname predicate reply_handler ~dest) in 
  stream

type lquery_context = {resolver: (string * Abuf.t * data_info) list Lwt.u; mutable qs: (string * Abuf.t * data_info) list}

let lquery z ?(dest=Partial) resname predicate =   
  let promise,resolver = Lwt.wait () in 
  let ctx = {resolver; qs = []} in  
  let reply_handler qreply =     
    match qreply with 
    | StorageData {stoid=_; rsn=_; resname; data; info} -> 
      (* TODO: Eventually we should check the timestamp *)
      (match List.find_opt (fun (k,_,_) -> k = resname) ctx.qs with 
      | Some _ -> Lwt.return_unit
      | None  -> ctx.qs <- (resname, data, info)::ctx.qs; Lwt.return_unit)
    | StorageFinal {stoid=_;rsn=_} -> Lwt.return_unit
    | ReplyFinal ->       
      Lwt.wakeup_later ctx.resolver ctx.qs; Lwt.return_unit
  in
  let _ = (query z resname predicate reply_handler ~dest) in 
  promise


let unstore z (sto:storage) = 
  let%lwt state = Guard.acquire z.state in
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
  Lwt.return @@ Guard.release z.state state 