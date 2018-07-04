open Apero
open Ztypes
open Locator
open Message
open Transport
open Channel
open Iobuf
open Printf
open Property
open Frame

module SID = Transport.Session.Id
module Event = Transport.Event

module URI = struct

  let uri_match uri1 uri2 =
    let pattern_match uri pattern = 
      let expr = Str.regexp (pattern 
      |> Str.global_replace (Str.regexp "\\.") "\\."
      |> Str.global_replace (Str.regexp "\\*\\*") ".*"
      |> Str.global_replace (Str.regexp "\\([^\\.]\\)\\*") "\\1[^/]*"
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
  }

  type t = {
    name : ResName.t;
    mappings : mapping list;
  }

  let with_mapping res mapping = 
    {res with mappings=mapping :: List.filter (fun m -> not (SID.equal m.session mapping.session)) res.mappings}

  let update_mapping res sid updater = 
    let mapping = List.find_opt (fun m -> m.session = sid) res.mappings in 
    let mapping = updater mapping in
    with_mapping res mapping

  let remove_mapping res sid = 
    {res with mappings=List.filter (fun m -> not (SID.equal m.session sid)) res.mappings}

  let res_match res1 res2 = ResName.name_match res1.name res2.name
end

module Session : sig

  type t = {
    sid : SID.t;
    tx_push : Transport.Event.push;
    ic : InChannel.t;
    oc : OutChannel.t;
    rmap : ResName.t VleMap.t;
  }
  val create : SID.t -> Transport.Event.push -> t
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
  }

  let create sid txp =
    let ic = InChannel.create Int64.(shift_left 1L 16) in
    let oc = OutChannel.create Int64.(shift_left 1L 16) in        
    {
      sid;
      tx_push = txp;
      ic;
      oc;
      rmap = VleMap.empty; 
    }
  let in_channel s = s.ic
  let out_channel s = s.oc
  let sid s = s.sid
end

module ProtocolEngine = struct

  type t = {
    pid : IOBuf.t;
    lease : Vle.t;
    locators : Locators.t;
    smap : Session.t SIDMap.t;
    rmap : Resource.t ResMap.t;
    evt_sink : Event.event Lwt_stream.t;
    evt_sink_push : Event.push;
  }

  let create (pid : IOBuf.t) (lease : Vle.t) (ls : Locators.t) = 
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
    evt_sink_push }

  let event_push pe = pe.evt_sink_push

  let get_tx_push pe sid =  (OptionM.get @@ SIDMap.find_opt sid pe.smap).tx_push
    
  let add_session pe (sid : SID.t) tx_push =    
    let%lwt _ = Logs_lwt.debug (fun m -> m "Registering Session %s: \n" (SID.show sid)) in
    let s = Session.create sid tx_push in     
    let smap = SIDMap.add sid s pe.smap in 
    Lwt.return {pe with smap}

  let remove_session pe sid =    
    let%lwt _ = Logs_lwt.debug (fun m -> m  "Un-registering Session %s \n" (SID.show sid)) in
    let smap = SIDMap.remove sid pe.smap in
    let rmap = ResMap.map (fun r -> Resource.remove_mapping r sid) pe.rmap in 
    Lwt.return {pe with rmap; smap}

  let update_resource_mapping (pe:t) name session rid updater = 
    let open Resource in 
    let open Session in 
    let res = ResMap.find_opt name pe.rmap in
    let res = match res with 
    | Some res -> update_mapping res session.sid updater
    | None -> {name; mappings=[updater None]} in 
    let rmap = ResMap.add res.name res pe.rmap in
    let session = {session with rmap=VleMap.add rid res.name session.rmap;} in
    let smap = SIDMap.add session.sid session pe.smap in
    Lwt.return ({pe with smap; rmap}, res)

  let add_resource pe sid rd =
    let open Resource in
    let session = SIDMap.find_opt sid pe.smap in 
    match session with 
    | None -> let%lwt _ = Logs_lwt.warn (fun m -> m "Received ResourceDecl on unknown session %s: Ignore it!" (SID.show sid)) in Lwt.return pe
    | Some session -> 
      let rid = ResourceDecl.rid rd in 
      let uri = ResourceDecl.resource rd in 
      let%lwt _ = Logs_lwt.debug (fun m -> m "Register resource %s" uri) in
      let%lwt (pe, _) = update_resource_mapping pe (URI(uri)) session rid 
        (fun m -> match m with 
          | Some mapping -> mapping
          | None -> {id = rid; session = session.sid; pub = false; sub = false; matched_pub = false}) in 
      Lwt.return pe

  let add_publication pe sid pd =
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
      let%lwt (pe, res) = update_resource_mapping pe resname session rid 
        (fun m -> match m with 
          | Some m -> {m with pub=true;} 
          | None -> {id = rid; session = sid; pub = true; sub = false; matched_pub = false}) in
      Lwt.return (pe, Some res)

  let add_subscription pe sid sd =
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
      let%lwt (pe, res) = update_resource_mapping pe resname session rid 
        (fun m -> match m with 
          | Some m -> {m with sub=true;} 
          | None -> {id = rid; session = sid; pub = false; sub = true; matched_pub = false}) in
      Lwt.return (pe, Some res)

  let make_hello pe = Message.Hello (Hello.create (Vle.of_char ScoutFlags.scoutBroker) pe.locators [])

  let make_accept pe opid = Message.Accept (Accept.create opid pe.pid pe.lease Properties.empty)

  let process_scout pe sid msg =
    if Vle.logand (Scout.mask msg) (Vle.of_char ScoutFlags.scoutBroker) <> 0L then Lwt.return (pe, [make_hello pe])
    else Lwt.return (pe, [])

  let process_open pe s msg push =
    let%lwt _ = Logs_lwt.debug (fun m -> m "Accepting Open from remote peer: %s\n" (IOBuf.to_string @@ Open.pid msg)) in
    let%lwt pe = add_session pe s push in Lwt.return (pe, [make_accept pe (Open.pid msg)])

  let make_result pe s cd =
    let open Declaration in
    let%lwt _ =  Logs_lwt.debug (fun m -> m  "Crafting Declaration Result") in
    Lwt.return (pe, [Declaration.ResultDecl (ResultDecl.create (CommitDecl.commit_id cd) (char_of_int 0) None)])

  let notify_pub_matching_res pe (sid : SID.t) res =
    let%lwt _ = Logs_lwt.debug (fun m -> m "Notifing Pub Matching Subs") in 
    let open Resource in
    let (pe, ps) = ResMap.fold (fun _ pr x -> 
      match res_match res pr with
      | false -> x 
      | true -> 
        let (pe, ps) = x in 
        List.fold_left (fun x m -> 
          match (m.pub && not m.matched_pub && m.session != sid) with 
          | false -> x
          | true -> let (pe, ps) = x in
            let m = {m with matched_pub = true} in
            let ms = m :: List.filter (fun am -> not (SID.equal m.session am.session)) pr.mappings in
            let pr = {pr with mappings = ms} in
            let rmap = ResMap.add pr.name pr pe.rmap in
            let pe = {pe with rmap} in

            let session = SIDMap.find m.session pe.smap in
            let oc = Session.out_channel session in
            let ds = [Declaration.SubscriberDecl (SubscriberDecl.create m.id SubscriptionMode.push_mode Properties.empty)] in
            let decl = Message.Declare (Declare.create (true, true) (OutChannel.next_rsn oc) ds) in
            Lwt.ignore_result @@ Logs_lwt.debug(fun m ->  m "Sending SubscriberDecl to session %s" (SID.show session.sid));
            (* TODO: This is going to throw an exception if the channel is out of places... need to handle that! *)
            (pe, (get_tx_push pe sid)  (Event.SessionMessage (Frame.create [decl], sid, None)) :: ps)
        ) (pe, ps) pr.mappings
    ) pe.rmap (pe, []) in
    Lwt.ignore_result @@ Lwt.join ps;
    Lwt.return pe

  let match_sub pe (sid : SID.t) sd =
    let%lwt _ = Logs_lwt.debug (fun m -> m "Matching SubDeclaration") in   
    let open Resource in
    let%lwt (pe, res) = add_subscription pe sid sd in
    match res with 
    | None -> Lwt.return (pe, [])
    | Some res -> 
      let%lwt _ = notify_pub_matching_res pe sid res in
      Lwt.return (pe, [])

  let match_pub pe (sid : SID.t) pd =
    let%lwt _ = Logs_lwt.debug (fun m -> m "Matching PubDeclaration") in   
    let open Resource in 
    let id = PublisherDecl.rid pd in
    let%lwt (pe, pr) = add_publication pe sid pd in
    match pr with 
    | None -> Lwt.return (pe, [])
    | Some pr -> 
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
          Lwt.return (pe, [Declaration.SubscriberDecl (SubscriberDecl.create id SubscriptionMode.push_mode Properties.empty)])

  let process_declaration pe (sid : SID.t) d =
    let open Declaration in
    match d with
    | ResourceDecl rd ->
      let%lwt _ = Logs_lwt.debug (fun m -> m "RDecl for resource: %Ld %s"  (ResourceDecl.rid rd) (ResourceDecl.resource rd) ) in
      let%lwt pe = add_resource pe sid rd in
      Lwt.return (pe, [])
    | PublisherDecl pd ->
      let%lwt _ = Logs_lwt.debug (fun m -> m "PDecl for resource: %Ld" (PublisherDecl.rid pd)) in
      match_pub pe sid pd
    | SubscriberDecl sd ->
      let%lwt _ = Logs_lwt.debug (fun m -> m "SDecl for resource: %Ld"  (SubscriberDecl.rid sd)) in      
      match_sub pe sid sd
    | CommitDecl cd -> 
      let%lwt _ = Logs_lwt.debug (fun m -> m "Commit SDecl ") in
      make_result pe sid cd
    | _ ->
      let%lwt _ = Logs_lwt.debug (fun m -> m "Unknown / Unhandled Declaration...."  ) in       
      Lwt.return (pe, [])

  let process_declarations pe (sid : SID.t) ds =  
    List.fold_left (fun x d -> 
      let%lwt (pe, ds) = x in
      let%lwt (pe, decl) = process_declaration pe sid d in 
      Lwt.return (pe, decl @ ds)) (Lwt.return (pe, [])) ds

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

  let forward_data pe srcres dstres dstmap reliable payload =
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

  let process_stream_data pe (sid : SID.t) msg =
    let open Resource in 
    let rid = StreamData.id msg in
    let session = SIDMap.find_opt sid pe.smap in 
    match session with 
    | None -> let%lwt _ = Logs_lwt.warn (fun m -> m "Received StreamData on unknown session %s: Ignore it!" 
                          (SID.show sid)) in Lwt.return (pe, [])
    | Some session -> 
      match VleMap.find_opt rid session.rmap with 
      | None -> let%lwt _ = Logs_lwt.warn (fun m -> m "Received StreamData for unknown resource %Ld on session %s: Ignore it!" 
                            rid (SID.show sid)) in Lwt.return (pe, [])
      | Some name -> 
        match ResMap.find_opt name pe.rmap with 
        | None -> let%lwt _ = Logs_lwt.warn (fun m -> m "Received StreamData for unknown resource %s on session %s: Ignore it!" 
                            (ResName.to_string name) (SID.show sid)) in Lwt.return (pe, [])
        | Some res -> 
          let%lwt _ = Logs_lwt.debug (fun m -> m "Handling Stream Data Message for resource: [%s:%Ld] (%s)" 
                      (SID.show sid) rid (match res.name with URI u -> u | ID _ -> "UNNAMED")) in
          ResMap.iter (fun _ r -> 
            if res_match res r then 
            begin
              List.iter (fun m ->
                if m.sub && m.session != sid then
                begin 
                  Lwt.ignore_result @@ forward_data pe res r m (reliable msg) (StreamData.payload msg)
                end) r.mappings 
            end) pe.rmap;
          Lwt.return (pe, [])
  
  let process pe (sid : SID.t) msg push =
    let%lwt _ = Logs_lwt.debug (fun m -> m  "Received message: %s" (Message.to_string msg)) in
    let%lwt (pe, rs) = match msg with
    | Message.Scout msg -> process_scout pe sid msg
    | Message.Hello _ -> Lwt.return (pe, [])
    | Message.Open msg -> process_open pe sid msg push
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
    | [] -> Lwt.return (pe, Lwt.return_unit)
    | _ as xs -> 
      Lwt.return (pe, get_tx_push pe sid @@ (Event.SessionMessage (Frame.create xs, sid, None)))


  let start pe =    
    let rec loop pe =      
      let open Lwt.Infix in 
      let%lwt _ = Logs_lwt.debug (fun m -> m "Processing Protocol Engine Events") in
      (match%lwt Lwt_stream.get pe.evt_sink with 
      | Some(Event.SessionMessage (f, sid, Some push)) -> 
        let%lwt _ = Logs_lwt.debug (fun m -> m "Processing SessionMessage") in
        let msgs = Frame.to_list f in
        let%lwt (pe, ps) = List.fold_left (fun x msg -> 
          let%lwt (pe, ps) = x in 
          let%lwt (pe, p) = process pe sid msg push in 
          Lwt.return (pe, p :: ps)) (Lwt.return (pe, [])) msgs in
        Lwt.ignore_result @@ Lwt.join ps;
        Lwt.return pe
      |Some _ -> 
        let%lwt _ = Logs_lwt.debug (fun m -> m "Processing Some other Event...") in
        Lwt.return pe
      | None -> 
        let%lwt _ = Logs_lwt.debug (fun m -> m "Processing None!!!") in
        Lwt.return pe)  
      >>= loop
    in 
    let%lwt _ = Logs_lwt.debug (fun m -> m "Starting Protocol Engine") in
    loop pe
end