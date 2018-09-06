open Apero
open Apero_net
open Transport
open Channel
open Printf
open Frame
open Message

module SID = Transport.Session.Id
module Event = Transport.Event

module URI = struct

  let uri_match uri1 uri2 =
    let pattern_match uri pattern = 
      let expr = Str.regexp (pattern 
      |> Str.global_replace (Str.regexp "\\.") "\\."
      |> Str.global_replace (Str.regexp "\\*\\*") ".*"
      |> Str.global_replace (Str.regexp "\\([^\\.]\\)\\*") "\\1[^/]*"
      |> Str.global_replace (Str.regexp "^\\*") "[^/]*"
      |> Str.global_replace (Str.regexp "\\\\\\.\\*") "\\.[^/]*") in
      (Str.string_match expr uri 0) && (Str.match_end() = String.length uri) in
    (pattern_match uri1 uri2) || (pattern_match uri2 uri1)

end

module ResName = struct 
  type t  = | URI of string | ID of Vle.t

  let compare name1 name2 = 
    match name1 with 
    | ID id1 -> (match name2 with 
      | ID id2 -> Vle.compare id1 id2
      | URI _ -> 1)
    | URI uri1 -> (match name2 with 
      | ID _ -> -1
      | URI uri2 -> String.compare uri1 uri2)

  let name_match name1 name2 =
    match name1 with 
    | ID id1 -> (match name2 with 
      | ID id2 -> id1 = id2
      | URI _ -> false)
    | URI uri1 -> (match name2 with 
      | ID _ -> false
      | URI uri2 -> URI.uri_match uri1 uri2)

  let to_string = function 
    | URI uri -> uri 
    | ID id -> Vle.to_string id

end 

module SIDMap = Map.Make(SID)
module VleMap = Map.Make(Vle)
module ResMap = Map.Make(ResName)

module Resource = struct 

  type mapping = {
    id : Vle.t;
    session : SID.t;
    pub : bool;
    sub : bool;
    matched_pub : bool;
    matched_sub : bool;
  }

  type t = {
    name : ResName.t;
    mappings : mapping list;
    matches : ResName.t list;
  }

  let with_mapping res mapping = 
    {res with mappings = mapping :: List.filter (fun m -> not (SID.equal m.session mapping.session)) res.mappings}

  let update_mapping res sid updater = 
    let mapping = List.find_opt (fun m -> m.session = sid) res.mappings in 
    let mapping = updater mapping in
    with_mapping res mapping

  let remove_mapping res sid = 
    {res with mappings = List.filter (fun m -> not (SID.equal m.session sid)) res.mappings}

  let with_match res mname = 
    {res with matches = mname :: List.filter (fun r -> r != mname) res.matches}

  let remove_match res mname = 
    {res with matches = List.filter (fun r -> r != mname) res.matches}

  let res_match res1 res2 = ResName.name_match res1.name res2.name
end

module Session : sig

  type t = {
    sid : SID.t;
    tx_push : Transport.Event.push;
    ic : InChannel.t;
    oc : OutChannel.t;
    rmap : ResName.t VleMap.t;
    mask : Vle.t;
  }
  val create : SID.t -> Transport.Event.push -> Vle.t -> t
  val in_channel : t -> InChannel.t
  val out_channel : t -> OutChannel.t
  val sid : t -> SID.t  
end = struct
  type sessionRes = {
    uri : string;
    id : Vle.t;
    session : SID.t;
  }

  type t = {    
    sid : SID.t;   
    tx_push : Transport.Event.push;
    ic : InChannel.t;
    oc : OutChannel.t;
    rmap : ResName.t VleMap.t;
    mask : Vle.t;
  }

  let create sid txp mask =
    let ic = InChannel.create Int64.(shift_left 1L 16) in
    let oc = OutChannel.create Int64.(shift_left 1L 16) in        
    {
      sid;
      tx_push = txp;
      ic;
      oc;
      rmap = VleMap.empty; 
      mask = mask;
    }
  let in_channel s = s.ic
  let out_channel s = s.oc
  let sid s = s.sid
end

let rec hash = function 
  | "" -> 0
  | s -> (31 * (hash (String.sub s 1 ((String.length s) - 1))) + int_of_char s.[0]) mod 4294967295

let hostid = Printf.sprintf "%08X" @@ hash @@ Unix.gethostname ()

module Config = struct
  type nid_t = string
  type prio_t = int
  type dist_t = int

  let local_id = hostid ^ Printf.sprintf "%08d" (Unix.getpid ())
  let local_prio = Unix.getpid ()
  let max_dist = 2
  let max_trees = 1
end

module Router = Router.Make(Config)
open Router 

module ProtocolEngine = struct

  type t = {
    pid : IOBuf.t;
    lease : Vle.t;
    locators : Locators.t;
    smap : Session.t SIDMap.t;
    rmap : Resource.t ResMap.t;
    evt_sink : Event.event Lwt_stream.t;
    evt_sink_push : Event.push;
    peers : Locator.t list;
    router : Router.t;
    next_mapping : Vle.t;
  }

  let next_mapping pe = 
    let next = pe.next_mapping in
    ({pe with next_mapping = Vle.add next 1L}, next)

  let send_nodes peer _nodes = 
    List.iter (fun node -> 
      let b = Marshal.to_bytes node [] in
      let sdata = with_marker 
        (StreamData(StreamData.create (true, true) 0L 0L None (IOBuf.from_bytes (Lwt_bytes.of_bytes b))))
        (RSpace (RSpace.create 1L)) in 
      Lwt.ignore_result @@ peer.push (Event.SessionMessage (Frame.create [sdata], peer.sid, None)) ) _nodes

  let send_nodes peers nodes = List.iter (fun peer -> send_nodes peer nodes) peers

  let create (pid : IOBuf.t) (lease : Vle.t) (ls : Locators.t) (peers : Locator.t list) = 
    (* TODO Parametrize depth *)
    let (evt_sink, bpush) = Lwt_stream.create_bounded 128 in 
    let evt_sink_push = fun e -> bpush#push e in
    { 
    pid; 
    lease; 
    locators = ls; 
    smap = SIDMap.empty; 
    rmap = ResMap.empty; 
    evt_sink;
    evt_sink_push;
    peers;
    router = Router.create send_nodes;
    next_mapping = 0L; }

  let event_push pe = pe.evt_sink_push

  let get_tx_push pe sid =  (Option.get @@ SIDMap.find_opt sid pe.smap).tx_push
    
  let add_session pe (sid : SID.t) tx_push mask =    
    let%lwt _ = Logs_lwt.debug (fun m -> m "Registering Session %s: \n" (SID.show sid)) in
    let s = Session.create sid tx_push mask in     
    let smap = SIDMap.add sid s pe.smap in 
    Lwt.return {pe with smap}

  let remove_session pe sid =    
    let%lwt _ = Logs_lwt.debug (fun m -> m  "Un-registering Session %s \n" (SID.show sid)) in
    let smap = SIDMap.remove sid pe.smap in
    let rmap = ResMap.map (fun r -> Resource.remove_mapping r sid) pe.rmap in 
    Lwt.return {pe with rmap; smap}

  let match_resource rmap mres = 
    let open Resource in
    match mres.name with 
    | URI _ -> (
      ResMap.fold (fun _ res x -> 
        let (rmap, mres) = x in
        match res_match mres res with 
        | true -> 
          let mres = with_match mres res.name in 
          let rmap = ResMap.add res.name (with_match res mres.name) rmap in 
          (rmap, mres)
        | false -> x) rmap (rmap, mres))
    | ID _ -> (rmap, with_match mres mres.name)

  let update_resource pe name updater = 
    let open Resource in
    let optres = ResMap.find_opt name pe.rmap in 
    let res = updater optres in
    let (rmap, res) = match optres with 
    | None -> match_resource pe.rmap res
    | Some _ -> (pe.rmap, res) in
    let rmap = ResMap.add res.name res rmap in 
    ({pe with rmap}, res)

  let update_resource_mapping (pe:t) name session rid updater = 
    let open Resource in 
    let open Session in 
    Logs.debug (fun m -> m "Register resource '%s' mapping [sid : %s, rid : %d]" (ResName.to_string name) (SID.show session.sid) (Vle.to_int rid));
    let(pe, res) = update_resource pe name 
      (fun r -> match r with 
      | Some res -> update_mapping res session.sid updater
      | None -> {name; mappings=[updater None]; matches=[name]}) in
    let session = {session with rmap=VleMap.add rid res.name session.rmap;} in
    let smap = SIDMap.add session.sid session pe.smap in
    ({pe with smap}, res)

  let declare_resource pe sid rd =
    let open Resource in
    let session = SIDMap.find_opt sid pe.smap in 
    match session with 
    | None -> let%lwt _ = Logs_lwt.warn (fun m -> m "Received ResourceDecl on unknown session %s: Ignore it!" (SID.show sid)) in Lwt.return pe
    | Some session -> 
      let rid = ResourceDecl.rid rd in 
      let uri = ResourceDecl.resource rd in 
      let (pe, _) = update_resource_mapping pe (URI(uri)) session rid 
        (fun m -> match m with 
          | Some mapping -> mapping
          | None -> {id = rid; session = session.sid; pub = false; sub = false; matched_pub = false; matched_sub=false}) in 
      Lwt.return pe

  let pid_to_string pid = fst @@ Result.get (IOBuf.get_string (IOBuf.available pid) pid)

  let make_scout = Message.Scout (Scout.create (Vle.of_char ScoutFlags.scoutBroker) [])

  let make_hello pe = Message.Hello (Hello.create (Vle.of_char ScoutFlags.scoutBroker) pe.locators [])

  let make_open pe = Message.Open (Open.create (char_of_int 0) pe.pid 0L pe.locators [])

  let make_accept pe opid = Message.Accept (Accept.create opid pe.pid pe.lease [])

  let process_scout pe sid msg push =
    let%lwt pe = add_session pe sid push (Scout.mask msg) in 
    Lwt.return (pe, [make_hello pe])

  let process_hello pe sid msg push =
    let%lwt pe = add_session pe sid push (Hello.mask msg) in 
    match Vle.logand (Hello.mask msg) (Vle.of_char ScoutFlags.scoutBroker) <> 0L with 
    | false -> (Lwt.return (pe, []))
    | true -> (
      let%lwt _ = Logs_lwt.debug (fun m -> m "Try to open ZENOH session with broker on transport session: %s\n" (SID.show sid)) in
      Lwt.return (pe, [make_open pe]))

  let process_open pe s msg push =
    match SIDMap.find_opt s pe.smap with
    | None -> 
      let%lwt _ = Logs_lwt.debug (fun m -> m "Accepting Open from unscouted remote peer: %s\n" (pid_to_string @@ Open.pid msg)) in
      let%lwt pe = add_session pe s push Vle.zero in Lwt.return (pe, [make_accept pe (Open.pid msg)])
    | Some session -> match Vle.logand session.mask (Vle.of_char ScoutFlags.scoutBroker) <> 0L with 
      | false -> (
        let%lwt _ = Logs_lwt.debug (fun m -> m "Accepting Open from remote peer: %s\n" (pid_to_string @@ Open.pid msg)) in
        Lwt.return (pe, [make_accept pe (Open.pid msg)]))
      | true -> (
        let%lwt _ = Logs_lwt.debug (fun m -> m "Accepting Open from remote broker: %s\n" (pid_to_string @@ Open.pid msg)) in
        let pe = {pe with router = Router.new_node pe.router {pid = pid_to_string @@ Open.pid msg; sid = session.sid; push = push}} in
        Lwt.return (pe, [make_accept pe (Open.pid msg)]))

  let process_accept pe s msg push =
    match SIDMap.find_opt s pe.smap with
    | None -> 
      let%lwt _ = Logs_lwt.debug (fun m -> m "Accepted from unscouted remote peer: %s\n" (pid_to_string @@ Accept.apid msg)) in
      let%lwt pe = add_session pe s push Vle.zero in Lwt.return (pe, [])
    | Some session -> match Vle.logand session.mask (Vle.of_char ScoutFlags.scoutBroker) <> 0L with 
      | false -> (
        let%lwt _ = Logs_lwt.debug (fun m -> m "Accepted from remote peer: %s\n" (pid_to_string @@ Accept.apid msg)) in
        Lwt.return (pe, []))
      | true -> (
        let%lwt _ = Logs_lwt.debug (fun m -> m "Accepted from remote broker: %s\n" (pid_to_string @@ Accept.apid msg)) in
        let pe = {pe with router = Router.new_node pe.router {pid = pid_to_string @@ Accept.apid msg; sid = session.sid; push = push}} in
        Lwt.return (pe, []))

  let make_result pe s cd =
    let open Declaration in
    let%lwt _ =  Logs_lwt.debug (fun m -> m  "Crafting Declaration Result") in
    Lwt.return (pe, [Declaration.ResultDecl (ResultDecl.create (CommitDecl.commit_id cd) (char_of_int 0) None)])


  (* ======================== PUB DECL =========================== *)

  let register_publication pe sid pd =
    let open Resource in 
    let session = SIDMap.find_opt sid pe.smap in 
    match session with 
    | None -> let%lwt _ = Logs_lwt.warn (fun m -> m "Received PublicationDecl on unknown session %s: Ignore it!" 
                          (SID.show sid)) in Lwt.return (pe, None)
    | Some session -> 
      let rid = PublisherDecl.rid pd in 
      let resname = match VleMap.find_opt rid session.rmap with 
      | Some name -> name
      | None -> ID(rid) in
      let (pe, res) = update_resource_mapping pe resname session rid 
        (fun m -> match m with 
          | Some m -> {m with pub=true;} 
          | None -> {id = rid; session = sid; pub = true; sub = false; matched_pub = false; matched_sub=false}) in
      Lwt.return (pe, Some res)
  
  let forward_pdecl_to_session pe res s = 
    let open Resource in 
    let oc = Session.out_channel s in
    let (pe, ds) = match res.name with 
      | ID id -> (
        let pubdecl = Declaration.PublisherDecl (PublisherDecl.create id []) in
        (pe, [pubdecl]))
      | URI uri -> 
        let (pe, rid) = next_mapping pe in 
        let resdecl = Declaration.ResourceDecl (ResourceDecl.create rid uri []) in
        let pubdecl = Declaration.PublisherDecl (PublisherDecl.create rid []) in
        let (pe, _) = update_resource_mapping pe res.name s rid 
          (fun m -> match m with 
            | Some mapping -> mapping
            | None -> {id = rid; session = s.sid; pub = false; sub = false; matched_pub = false; matched_sub=false}) in 
        (pe, [resdecl; pubdecl]) in
    let decl = Message.Declare (Declare.create (true, true) (OutChannel.next_rsn oc) ds) in
    (* TODO: This is going to throw an exception if the channel is out of places... need to handle that! *)
    (pe,  s.tx_push  (Event.SessionMessage (Frame.create [decl], s.sid, None)))

  let forward_pdecl_to_parents pe res = 
    let open Router in
    let (pe, ps) = Router.TreeSet.parents pe.router.tree_set
    |> List.map (fun (node:Router.TreeSet.Tree.Node.t) -> 
      (List.find (fun x -> x.pid = node.node_id) pe.router.peers).sid )
    |> List.fold_left (fun x sid -> 
       let (pe, ps) = x in
       let s = Option.get @@ SIDMap.find_opt sid pe.smap in
       let (pe, p) = forward_pdecl_to_session pe res s in 
       (pe, p :: ps)
       ) (pe, []) in 
    let%lwt _ = Lwt.join ps in
    Lwt.return pe

  let match_pdecl pe pr id sid =
    let open Resource in 
    let pm = List.find (fun m -> m.session = sid) pr.mappings in
    match pm.matched_pub with 
    | true -> Lwt.return (pe, [])
    | false -> 
      match ResMap.exists 
        (fun _ sr ->  res_match pr sr && List.exists 
          (fun m -> m.sub && m.session != sid) sr.mappings) pe.rmap with
      | false -> Lwt.return (pe, [])
      | true -> 
        let pm = {pm with matched_pub = true} in
        let pr = with_mapping pr pm in
        let rmap = ResMap.add pr.name pr pe.rmap in
        let pe = {pe with rmap} in
        Lwt.return (pe, [Declaration.SubscriberDecl (SubscriberDecl.create id SubscriptionMode.push_mode [])])

  let process_pdecl pe (sid : SID.t) pd =
    let open Resource in 
    let%lwt (pe, pr) = register_publication pe sid pd in
    match pr with 
    | None -> Lwt.return (pe, [])
    | Some pr -> 
      let%lwt pe = forward_pdecl_to_parents pe pr in
      let id = PublisherDecl.rid pd in
      match_pdecl pe pr id sid

  (* ======================== SUB DECL =========================== *)

  let forward_sdecl_to_session pe res s = 
    let open Resource in 
    let oc = Session.out_channel s in
    let (pe, ds) = match res.name with 
      | ID id -> (
        let subdecl = Declaration.SubscriberDecl (SubscriberDecl.create id SubscriptionMode.push_mode []) in
        (pe, [subdecl]))
      | URI uri -> 
        let (pe, rid) = next_mapping pe in 
        let resdecl = Declaration.ResourceDecl (ResourceDecl.create rid uri []) in
        let subdecl = Declaration.SubscriberDecl (SubscriberDecl.create rid SubscriptionMode.push_mode []) in
        let (pe, _) = update_resource_mapping pe res.name s rid 
          (fun m -> match m with 
            | Some mapping -> mapping
            | None -> {id = rid; session = s.sid; pub = false; sub = false; matched_pub = false; matched_sub=false}) in 
        (pe, [resdecl; subdecl]) in
    let decl = Message.Declare (Declare.create (true, true) (OutChannel.next_rsn oc) ds) in
    (* TODO: This is going to throw an exception if the channel is out of places... need to handle that! *)
    (pe,  s.tx_push  (Event.SessionMessage (Frame.create [decl], s.sid, None)))

  let forward_sdecl_to_parents pe res = 
    let open Router in
    let (pe, ps) = Router.TreeSet.parents pe.router.tree_set
    |> List.map (fun (node:Router.TreeSet.Tree.Node.t) -> 
      (List.find (fun x -> x.pid = node.node_id) pe.router.peers).sid )
    |> List.fold_left (fun x sid -> 
       let (pe, ps) = x in
       let s = Option.get @@ SIDMap.find_opt sid pe.smap in
       let (pe, p) = forward_sdecl_to_session pe res s in 
       (pe, p :: ps)
       ) (pe, []) in 
    let%lwt _ = Lwt.join ps in
    Lwt.return pe

  let register_subscription pe sid sd =
    let open Resource in 
    let session = SIDMap.find_opt sid pe.smap in 
    match session with 
    | None -> let%lwt _ = Logs_lwt.warn (fun m -> m "Received SubscriptionDecl on unknown session %s: Ignore it!" 
                          (SID.show sid)) in Lwt.return (pe, None)
    | Some session -> 
      let rid = SubscriberDecl.rid sd in 
      let resname = match VleMap.find_opt rid session.rmap with 
      | Some name -> name
      | None -> ID(rid) in
      let (pe, res) = update_resource_mapping pe resname session rid 
        (fun m -> 
          match m with 
          | Some m -> {m with sub=true;} 
          | None -> {id = rid; session = sid; pub = false; sub = true; matched_pub = false; matched_sub=false}) in
      Lwt.return (pe, Some res)

  let notify_pub_matching_res pe (sid : SID.t) res =
    let%lwt _ = Logs_lwt.debug (fun m -> m "Notifing Pub Matching Subs") in 
    let open Resource in
    let (pe, ps) = List.fold_left (fun x name -> 
      match ResMap.find_opt name pe.rmap with 
      | None -> x
      | Some mres -> 
      let (pe, ps) = x in 
      List.fold_left (fun x m -> 
        match (m.pub && not m.matched_pub && m.session != sid) with 
        | false -> x
        | true -> let (pe, ps) = x in
          let m = {m with matched_pub = true} in
          let pres = Resource.with_mapping mres m in
          let rmap = ResMap.add pres.name pres pe.rmap in
          let pe = {pe with rmap} in

          let session = SIDMap.find m.session pe.smap in
          let oc = Session.out_channel session in
          let ds = [Declaration.SubscriberDecl (SubscriberDecl.create m.id SubscriptionMode.push_mode [])] in
          let decl = Message.Declare (Declare.create (true, true) (OutChannel.next_rsn oc) ds) in
          Lwt.ignore_result @@ Logs_lwt.debug(fun m ->  m "Sending SubscriberDecl to session %s" (SID.show session.sid));
          (* TODO: This is going to throw an exception if the channel is out of places... need to handle that! *)
          (pe, (get_tx_push pe session.sid)  (Event.SessionMessage (Frame.create [decl], session.sid, None)) :: ps)
        ) (pe, ps) mres.mappings
    ) (pe, []) res.matches in
    let%lwt _ = Lwt.join ps in
    Lwt.return pe

  let process_sdecl pe (sid : SID.t) sd =
    let open Resource in
    let%lwt (pe, res) = register_subscription pe sid sd in
    match res with 
    | None -> Lwt.return (pe, [])
    | Some res -> 
      let%lwt pe = forward_sdecl_to_parents pe res in
      let%lwt _ = notify_pub_matching_res pe sid res in
      Lwt.return (pe, [])

  (* ======================== ======== =========================== *)

  let process_declaration pe (sid : SID.t) d =
    let open Declaration in
    match d with
    | ResourceDecl rd ->
      let%lwt _ = Logs_lwt.debug (fun m -> m "RDecl for resource: %Ld %s"  (ResourceDecl.rid rd) (ResourceDecl.resource rd) ) in
      let%lwt pe = declare_resource pe sid rd in
      Lwt.return (pe, [])
    | PublisherDecl pd ->
      let%lwt _ = Logs_lwt.debug (fun m -> m "PDecl for resource: %Ld" (PublisherDecl.rid pd)) in
      process_pdecl pe sid pd
    | SubscriberDecl sd ->
      let%lwt _ = Logs_lwt.debug (fun m -> m "SDecl for resource: %Ld"  (SubscriberDecl.rid sd)) in      
      process_sdecl pe sid sd
    | CommitDecl cd -> 
      let%lwt _ = Logs_lwt.debug (fun m -> m "Commit SDecl ") in
      make_result pe sid cd
    | _ ->
      let%lwt _ = Logs_lwt.debug (fun m -> m "Unknown / Unhandled Declaration...."  ) in       
      Lwt.return (pe, [])

  let process_declarations pe (sid : SID.t) ds =  
    let open Declaration in
    (* Must process ResourceDecls first *)
    List.sort (fun x y -> match (x, y) with 
      | (ResourceDecl _, ResourceDecl _) -> 0
      | (ResourceDecl _, _) -> -1
      | (_, ResourceDecl _) -> 1
      | (_, _) -> 0) ds
    |> List.fold_left (fun x d -> 
      let%lwt (pe, ds) = x in
      let%lwt (pe, decl) = process_declaration pe sid d in 
      Lwt.return (pe, decl @ ds)) (Lwt.return (pe, [])) 

  let process_declare pe (sid : SID.t) msg =
    let%lwt _ = Logs_lwt.debug (fun m -> m "Processing Declare Message\n") in    
    match SIDMap.find_opt sid pe.smap with 
    | Some s ->
      let ic = Session.in_channel s in
      let oc = Session.out_channel s in
      let sn = (Declare.sn msg) in
      let csn = InChannel.rsn ic in
      if sn >= csn then
        begin
          InChannel.update_rsn ic sn  ;
          let%lwt (pe, ds) = process_declarations pe sid (Declare.declarations msg) in
          match ds with 
          | [] ->
            let%lwt _ = Logs_lwt.debug (fun m -> m  "Acking Declare with sn: %Ld" sn) in 
            Lwt.return (pe, [Message.AckNack (AckNack.create (Vle.add sn 1L) None)])
          | _ as ds ->
            let%lwt _ = Logs_lwt.debug (fun m -> m "Sending Matching decalrations and ACKNACK \n") in
            Lwt.return (pe, [Message.Declare (Declare.create (true, true) (OutChannel.next_rsn oc) ds);
                        Message.AckNack (AckNack.create (Vle.add sn 1L) None)])
        end
      else
        begin
          let%lwt _ = Logs_lwt.debug (fun m -> m "Received out of oder message") in
          Lwt.return (pe, [])
        end
      | None -> Lwt.return (pe, [])

  let process_synch pe (sid : SID.t) msg =
    let asn = Synch.sn msg in
    Lwt.return (pe, [Message.AckNack (AckNack.create asn None)])

  let process_ack_nack pe sid msg = Lwt.return (pe, [])

  let forward_data_to_mapping pe srcres dstres dstmap reliable payload =
    let open Session in
    let open Resource in
    match SIDMap.find_opt dstmap.session pe.smap with
    | None -> Lwt.return_unit
    | Some s ->
      let%lwt _ = Logs_lwt.debug (fun m -> m  "Forwarding data to session %s" (SID.show s.sid)) in
      let oc = Session.out_channel s in
      let fsn = if reliable then OutChannel.next_rsn oc else  OutChannel.next_usn oc in
      let msg = match srcres.name with 
      | ID id -> StreamData(StreamData.create (true, reliable) fsn id None payload)
      | URI uri -> match srcres.name = dstres.name with 
        | true -> StreamData(StreamData.create (true, reliable) fsn dstmap.id None payload)
        | false -> WriteData(WriteData.create (true, reliable) fsn uri payload) in
      get_tx_push pe s.sid @@ Event.SessionMessage (Frame.create [msg], s.sid, None)
  
  let rspace msg = 
    List.fold_left (fun res marker -> 
      match marker with 
      | RSpace rs -> RSpace.id rs 
      | _ -> res) 0L (markers msg)

  let forward_data pe sid srcres reliable payload = 
    let open Resource in
    let (_, ps) = List.fold_left (fun (sss, pss) name -> 
      match ResMap.find_opt name pe.rmap with 
      | None -> (sss, pss)
      | Some r -> 
        List.fold_left (fun (ss, ps) m ->
          match m.sub && m.session != sid && not @@ List.exists (fun s -> m.session == s) ss with
          | true -> 
            let p = forward_data_to_mapping pe srcres r m reliable payload in
            (m.session :: ss , p :: ps)
          | false -> (ss, ps)
        ) (sss, pss) r.mappings 
    ) ([], []) srcres.matches in 
    Lwt.join ps 

  let process_user_data (pe:t) session msg =
    let open Resource in 
    let open Session in 
    let rid = StreamData.id msg in
    let name = match VleMap.find_opt rid session.rmap with 
    | None -> ResName.ID(rid)
    | Some name -> name in 
    match ResMap.find_opt name pe.rmap with 
    | None -> let%lwt _ = Logs_lwt.warn (fun m -> m "Received StreamData for unknown resource %s on session %s: Ignore it!" 
                        (ResName.to_string name) (SID.show session.sid)) in Lwt.return (pe, [])
    | Some res -> 
      let%lwt _ = Logs_lwt.debug (fun m -> m "Handling Stream Data Message for resource: [%s:%Ld] (%s)" 
                  (SID.show session.sid) rid (match res.name with URI u -> u | ID _ -> "UNNAMED")) in
      Lwt.ignore_result @@ forward_data pe session.sid res (reliable msg) (StreamData.payload msg);
      Lwt.return (pe, [])

  let process_broker_data (pe:t) session msg = 
    let open Session in
    let%lwt _ = Logs_lwt.debug (fun m -> m "Received tree state on %s\n" (SID.show session.sid)) in
    let b = Lwt_bytes.to_bytes @@ IOBuf.to_bytes @@ StreamData.payload msg in 
    let node = Marshal.from_bytes b 0 in
    let pe = {pe with router = Router.update pe.router node} in
    Router.print pe.router; 
    Lwt.return (pe, []) 

  let process_stream_data pe sid msg =
    let open Resource in 
    let session = SIDMap.find_opt sid pe.smap in 
    match session with 
    | None -> let%lwt _ = Logs_lwt.warn (fun m -> m "Received StreamData on unknown session %s: Ignore it!" 
                          (SID.show sid)) in Lwt.return (pe, [])
    | Some session -> 
      Logs.debug (fun m -> m "RSPACE %d "  (Vle.to_int (rspace (StreamData(msg)))));
      match rspace (StreamData(msg)) with 
      | 1L -> process_broker_data pe session msg
      | _ -> process_user_data pe session msg
  
  let process pe (sid : SID.t) msg push =
    let%lwt _ = Logs_lwt.debug (fun m -> m  "Received message: %s" (Message.to_string msg)) in
    let%lwt (pe, rs) = match msg with
    | Message.Scout msg -> process_scout pe sid msg push
    | Message.Hello msg -> process_hello pe sid msg push
    | Message.Open msg -> process_open pe sid msg push
    | Message.Accept msg -> process_accept pe sid msg push
    | Message.Close msg -> 
      let%lwt _ = remove_session pe sid in 
      Lwt.return (pe, [Message.Close (Close.create pe.pid '0')])
    | Message.Declare msg -> process_declare pe sid msg
    | Message.Synch msg -> process_synch pe sid msg
    | Message.AckNack msg -> process_ack_nack pe sid msg
    | Message.StreamData msg -> process_stream_data pe sid msg
    | Message.KeepAlive msg -> Lwt.return (pe, [])
    | _ -> Lwt.return (pe, [])
    in 
    match rs with 
    | [] -> Lwt.return pe
    | _ as xs -> 
      let%lwt _ = get_tx_push pe sid @@ (Event.SessionMessage (Frame.create xs, sid, None)) in
      Lwt.return pe

  let rec connect_peer peer tx = 
    let module TxTcp = (val tx : Transport.S) in 
    let open Lwt in
    Lwt.catch(fun () ->
        TxTcp.connect peer >>= (fun (sid, push) -> 
        let%lwt _ = Logs_lwt.debug (fun m -> m "Connected to %s (sid = %s)" (Locator.to_string peer) (SID.show sid)) in 
        let%lwt _ = push (Event.SessionMessage (Frame.create [make_scout], sid, None)) in 
        Lwt.return_unit))
      (fun ex -> 
        let%lwt _ = Logs_lwt.debug (fun m -> m "Failed to connect to %s" (Locator.to_string peer)) in 
        let%lwt _ = Lwt_unix.sleep 2.0 in 
        connect_peer peer tx)

  let connect_peers peers tx = 
    let open Lwt in
    let module TxTcp = (val tx : Transport.S) in 
    Lwt_list.iter_p (fun p -> connect_peer p tx) peers

  let start pe tx = 
    let rec loop pe =      
      let open Lwt.Infix in 
      let%lwt _ = Logs_lwt.debug (fun m -> m "Processing Protocol Engine Events") in
      (match%lwt Lwt_stream.get pe.evt_sink with 
      | Some(Event.SessionMessage (f, sid, Some push)) -> 
        let%lwt _ = Logs_lwt.debug (fun m -> m "Processing SessionMessage") in
        let msgs = Frame.to_list f in
        List.fold_left (fun pe msg -> 
          pe >>= (fun pe -> process pe sid msg push)) (Lwt.return pe) msgs
      |Some _ -> 
        let%lwt _ = Logs_lwt.debug (fun m -> m "Processing Some other Event...") in
        Lwt.return pe
      | None -> 
        let%lwt _ = Logs_lwt.debug (fun m -> m "Processing None!!!") in
        Lwt.return pe)  
      >>= loop
    in 
    let%lwt _ = Logs_lwt.debug (fun m -> m "Starting Protocol Engine") in
    Lwt.ignore_result @@ connect_peers pe.peers tx;
    loop pe
end
