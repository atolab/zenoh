open Apero
open Common
open Iobuf
open Message
open Locator
open Lwt
open R_name

module VleMap = Map.Make(Vle)

type listener = IOBuf.t -> string -> unit Lwt.t


type insub = {subid:int; resid:Vle.t; listener:listener}

type resource = {rid: Vle.t; name: string; matches: Vle.t list; subs: insub list}

let with_match res mrid = 
  {res with matches = mrid :: List.filter (fun r -> r != mrid) res.matches}

(* let remove_match res mrid = 
  {res with matches = List.filter (fun r -> r != mrid) res.matches} *)

let with_sub sub res = 
  {res with subs = sub :: List.filter (fun s -> s != sub) res.subs}

let remove_sub subid res = 
  {res with subs = List.filter (fun s -> s.subid != subid) res.subs}
  

type state = {
  next_sn : Vle.t;
  next_pubsub_id : int;
  next_res_id : Vle.t;
  resmap : resource VleMap.t;
}

let create_state = {next_sn=1L; next_pubsub_id=0; next_res_id=0L; resmap=VleMap.empty}

let get_next_sn state = (state.next_sn, {state with next_sn=Vle.add state.next_sn 1L})
let get_next_pubsub_id state = (state.next_pubsub_id, {state with next_pubsub_id=(state.next_pubsub_id + 1)})
let get_next_res_id state = (state.next_res_id, {state with next_res_id=Vle.add state.next_res_id 1L})

type t = {
  sock : Lwt_unix.file_descr;
  state : state Lwt_mvar.t;
}

type sub = {z:t; id:int; resid:Vle.t;}
type pub = {z:t; id:int; resid:Vle.t; reliable:bool}

type submode = SubscriptionMode.t

let lbuf = IOBuf.create 16
let wbuf = IOBuf.create 8192
let rbuf = IOBuf.create 8192


let pid  = IOBuf.flip @@ Result.get @@ IOBuf.put_string "zenoha" (IOBuf.create 16) 

let lease = 0L
let version = Char.chr 0x01


let match_resource rmap mres = 
  VleMap.fold (fun _ res x -> 
    let (rmap, mres) = x in
    match URI.uri_match mres.name res.name with 
    | true -> 
      let mres = with_match mres res.rid in 
      let rmap = VleMap.add res.rid (with_match res mres.rid) rmap in 
      (rmap, mres)
    | false -> x) rmap (rmap, mres)

let add_resource resname state = 
  let (rid, state) = get_next_res_id state in 
  let res =  {rid; name=resname; matches=[rid]; subs=[]} in
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
  match msg with
  | Message.Accept _ -> Lwt.wakeup_later resolver t; return_true
  | Message.StreamData dmsg ->
    let%lwt state = Lwt_mvar.take t.state in
    let%lwt _ = Lwt_mvar.put t.state state in 
    match VleMap.find_opt (StreamData.id dmsg) state.resmap with
    | Some res -> 
      (* TODO make sure that payload is a copy *)
      (* TODO make payload a readonly buffer *)
      let buf = StreamData.payload dmsg in
      List.iter (fun resid -> 
        match VleMap.find_opt resid state.resmap with
        | Some res -> 
          List.iter (fun sub -> 
            Lwt.ignore_result @@ sub.listener buf res.name
          ) res.subs
        | None -> () 
      ) res.matches
    | None -> () ; ;
    return_true
  | Message.WriteData dmsg ->
    let%lwt state = Lwt_mvar.take t.state in
    let%lwt _ = Lwt_mvar.put t.state state in 
    (* TODO make sure that payload is a copy *)
    (* TODO make payload a readonly buffer *)
    let buf = WriteData.payload dmsg in
    VleMap.iter (fun _ res -> 
      match URI.uri_match res.name (WriteData.resource dmsg) with 
      | true -> 
          List.iter (fun sub -> 
            Lwt.ignore_result @@ sub.listener buf (WriteData.resource dmsg)
          ) res.subs
      | false -> ()) state.resmap;
      return_true
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
  (* try%lwt *)
    let%lwt _ = Logs_lwt.debug (fun m -> m "[Starting run_decode_loop]\n") in 
    let%lwt len = get_message_length t.sock rbuf in
    let%lwt _ = Logs_lwt.debug (fun m -> m ">>> Received message of %d bytes" len) in
    let rbuf = Result.get @@ IOBuf.set_position 0 rbuf in
    let rbuf = Result.get @@ IOBuf.set_limit len rbuf in
    let%lwt _ = Net.read_all t.sock rbuf in
    let%lwt _ =  Logs_lwt.debug (fun m -> m "tx-received: %s "  (IOBuf.to_string rbuf)) in
    let (msg, _) = Result.get @@ Mcodec.decode_msg rbuf in
    let%lwt _ = process_incoming_message msg resolver t in
    run_decode_loop resolver t

  (* with
  | _ ->
    let%lwt _ = Logs_lwt.warn (fun m -> m "Connection close by peer") in
    try%lwt
      let%lwt _ = Lwt_unix.close t.sock in
      fail @@ Exception (`ClosedSession (`Msg "Connection  closed by peer"))
    with
    | _ ->  
      let%lwt _ = Logs_lwt.warn (fun m -> m "[Session Closed]\n") in
      fail @@ Exception (`ClosedSession `NoMsg) *)
  
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
  let _ = con >>= fun _ -> run_decode_loop resolver {sock; state=Lwt_mvar.create create_state} in
  let _ = con >>= fun _ -> send_message sock make_open in
  con >>= fun _ -> promise


let publish resname z = 
  let%lwt state = Lwt_mvar.take z.state in
  let (res, state) = add_resource resname state in
  let (pubid, state) = get_next_pubsub_id state in

  let (sn, state) = get_next_sn state in
  let _ = send_message z.sock (Message.Declare(Declare.create (true, false) sn [
    ResourceDecl(ResourceDecl.create res.rid res.name []);
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


let stream buf pub = 
  let%lwt state = Lwt_mvar.take pub.z.state in
  let (sn, state) = get_next_sn state in
  let%lwt _ = Lwt_mvar.put pub.z.state state in
  send_message pub.z.sock (Message.StreamData(StreamData.create (false, pub.reliable) sn pub.resid None buf))
  >> Lwt.return_unit


let unpublish pub z = 
  let%lwt state = Lwt_mvar.take z.state in
  let (sn, state) = get_next_sn state in
  let%lwt _ = send_message z.sock (Message.Declare(Declare.create (true, false) sn [
    ForgetPublisherDecl(ForgetPublisherDecl.create pub.resid)
  ])) in 
  Lwt_mvar.put z.state state 


let push_mode = SubscriptionMode.push_mode
let pull_mode = SubscriptionMode.pull_mode


let subscribe resname listener ?(mode=push_mode) z = 
  let%lwt state = Lwt_mvar.take z.state in
  let (res, state) = add_resource resname state in
  let (subid, state) = get_next_pubsub_id state in
  let insub = {subid; resid=res.rid; listener} in
  let res = with_sub insub res in
  let resmap = VleMap.add res.rid res state.resmap in 
  let state = {state with resmap} in

  let (sn, state) = get_next_sn state in
  let _ = send_message z.sock (Message.Declare(Declare.create (true, false) sn [
    ResourceDecl(ResourceDecl.create res.rid res.name []);
    SubscriberDecl(SubscriberDecl.create res.rid mode [])
  ])) in 

  let%lwt _ = Lwt_mvar.put z.state state in
  Lwt.return {z=z; id=subid; resid=res.rid}


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

  