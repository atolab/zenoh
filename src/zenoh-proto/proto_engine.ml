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
module SessionMap = Map.Make (SID)
module PubSubMap = Map.Make(Vle)

module Session : sig
  type t = {
    sid : SID.t;
    tx_push : Transport.Event.push;
    ic : InChannel.t;
    oc : OutChannel.t
  }
  val create : SID.t -> Transport.Event.push -> t
  val in_channel : t -> InChannel.t
  val out_channel : t -> OutChannel.t
  val sid : t -> SID.t  
end = struct
  type t = {    
    sid : SID.t;   
    tx_push : Transport.Event.push;
    ic : InChannel.t;
    oc : OutChannel.t
  }

  let create sid txp =
    let ic = InChannel.create Int64.(shift_left 1L 16) in
    let oc = OutChannel.create Int64.(shift_left 1L 16) in        
    {
      sid;
      tx_push = txp;
      ic;
      oc;
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
    mutable smap : Session.t SessionMap.t;
    mutable pubmap : (SID.t list) PubSubMap.t;
    mutable submap : (SID.t list) PubSubMap.t;
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
    smap = SessionMap.empty; 
    pubmap = PubSubMap.empty; 
    submap = PubSubMap.empty;
    evt_sink;
    evt_sink_push }

  let event_push pe = pe.evt_sink_push

  let get_tx_push pe sid =  (OptionM.get @@ SessionMap.find_opt sid pe.smap).tx_push
    
  let add_session pe (sid : SID.t) tx_push =    
    let%lwt _ = Logs_lwt.debug (fun m -> m "Registering Session %s: \n" (SID.show sid)) in
    let s = Session.create sid tx_push in     
    let m = SessionMap.add sid s pe.smap in pe.smap <- m ; Lwt.return_unit
  
  let remove_session pe sid =    
    let%lwt _ = Logs_lwt.debug (fun m -> m  "Un-registering Session %s \n" (SID.show sid)) in
    let m = SessionMap.remove sid pe.smap in pe.smap <- m ;
    
    PubSubMap.iter (fun k xs ->
        let ys = List.filter (fun s -> SID.equal s sid) xs in
        let m = PubSubMap.add k ys pe.pubmap in pe.pubmap <- m
      ) pe.pubmap ;

    PubSubMap.iter (fun k xs ->
        let ys = List.filter (fun s -> SID.equal s sid) xs in
        let m = PubSubMap.add k ys pe.submap in pe.submap <- m
      ) pe.submap ;
    
    Lwt.return_unit

  let add_publication pe sid pd =
    let rid = PublisherDecl.rid pd in    
    let pubs = PubSubMap.find_opt rid pe.pubmap in
    let%lwt _ = Logs_lwt.debug (fun m -> m "Registering Pub for resource %Ld in session %s: \n" rid (SID.show sid)) in
    let m =  match pubs with
      | None -> PubSubMap.add rid [sid] pe.pubmap
      | Some xs -> PubSubMap.add rid (sid::xs) pe.pubmap
    in pe.pubmap <- m ;
    Lwt.return_unit

  let add_subscription pe sid sd =
    let rid = SubscriberDecl.rid sd in    
    let subs = PubSubMap.find_opt rid pe.submap in
    let%lwt _ = Logs_lwt.debug (fun m -> m "Registering Sub for resource %Ld in session %s: \n" rid (SID.show sid)) in

    let m = match subs with
      | None -> PubSubMap.add rid [sid] pe.submap
      | Some xs -> PubSubMap.add rid (sid::xs) pe.submap
    in pe.submap <- m ;
    Lwt.return_unit



  let make_hello pe = Message.Hello (Hello.create (Vle.of_char ScoutFlags.scoutBroker) pe.locators [])

  let make_accept pe opid = Message.Accept (Accept.create opid pe.pid pe.lease Properties.empty)

  let process_scout pe sid msg =
    if Vle.logand (Scout.mask msg) (Vle.of_char ScoutFlags.scoutBroker) <> 0L then Lwt.return [make_hello pe]
    else Lwt.return []

  let process_open pe s msg push =
    let%lwt _ = Logs_lwt.debug (fun m -> m "Accepting Open from remote peer: %s\n" (IOBuf.to_string @@ Open.pid msg)) in
    let _ = add_session pe s push in Lwt.return [make_accept pe (Open.pid msg)] 

  let make_result pe s cd =
    let open Declaration in
    let%lwt _ =  Logs_lwt.debug (fun m -> m  "Crafting Declaration Result") in
    Lwt.return [Declaration.ResultDecl (ResultDecl.create (CommitDecl.commit_id cd) (char_of_int 0) None)]

  let notify_pub_matching_sub pe (sid : SID.t) sd =
    let%lwt _ = Logs_lwt.debug (fun m -> m "Notifing Pub Matching Subs") in 
    let id = SubscriberDecl.rid sd in
    let open Apero in
    match PubSubMap.find_opt id pe.pubmap with
    | None -> Lwt.return_unit
    | Some xs -> 
      let ps = List.map (fun sid ->
        match SessionMap.find_opt sid pe.smap with
        | None -> Lwt.return_unit
        | Some s ->
          let sid = Session.sid s in
          let oc = Session.out_channel s in
          let ds = [Declaration.SubscriberDecl (SubscriberDecl.create id SubscriptionMode.push_mode Properties.empty)] in
          let decl = Message.Declare (Declare.create (true, true) (OutChannel.next_rsn oc) ds) in
          let%lwt _ = Logs_lwt.debug(fun m ->  m "Notifing Pub Matching Subs -- sending SubscriberDecl for sid = %s" (SID.show sid)) in          
          (* TODO: This is going to throw an exception is the channel is our of places... need to handle that! *)
          (get_tx_push pe sid)  (Event.SessionMessage (Frame.create [decl], sid, None))) xs 
      in  Lwt.join ps


  let match_sub pe (sid : SID.t) sd =
    let%lwt _ = Logs_lwt.debug (fun m -> m "Matching SubDeclaration") in
    let open Declaration in
    let id = SubscriberDecl.rid sd in
    let%lwt _ = add_subscription pe sid sd in
    let%lwt _ = notify_pub_matching_sub pe sid sd in
    match PubSubMap.find_opt (SubscriberDecl.rid sd) pe.pubmap  with
    | None -> Lwt.return []
    | Some pubs -> Lwt.return [Declaration.PublisherDecl (PublisherDecl.create id Properties.empty)]

  let match_pub pe (sid : SID.t) pd =
    let%lwt _ = Logs_lwt.debug (fun m -> m "Matching PubDeclaration") in    
    let id = PublisherDecl.rid pd in
    let%lwt _  = add_publication pe sid pd in
    match PubSubMap.find_opt (PublisherDecl.rid pd) pe.submap  with
    | None -> Lwt.return []
    | Some subs -> Lwt.return [Declaration.SubscriberDecl (SubscriberDecl.create id SubscriptionMode.push_mode Properties.empty)]

  let process_declaration pe (sid : SID.t) d =
    let open Declaration in
    match d with
    | PublisherDecl pd ->
      let%lwt _ = Logs_lwt.debug (fun m -> m "PDecl for resource: %s"  (Vle.to_string @@ PublisherDecl.rid pd)) in
      match_pub pe sid pd
    | SubscriberDecl sd ->
      let%lwt _ = Logs_lwt.debug (fun m -> m "SDecl for resource: %s"  (Vle.to_string @@ SubscriberDecl.rid sd)) in      
      match_sub pe sid sd
    | CommitDecl cd -> 
      let%lwt _ = Logs_lwt.debug (fun m -> m "Commit SDecl ") in
      make_result pe sid cd
    | _ ->
      let%lwt _ = Logs_lwt.debug (fun m -> m "Unknown / Unhandled Declaration...."  ) in       
      Lwt.return []

  let process_declarations pe (sid : SID.t) ds =      
    let%lwt rs = ds
    |> List.map (fun d -> process_declaration pe sid d)
    |> LwtM.flatten 
    in Lwt.return @@ List.concat rs

  let process_declare pe (sid : SID.t) msg =
    let%lwt _ = Logs_lwt.debug (fun m -> m "Processing Declare Message\n") in    
    match SessionMap.find_opt sid pe.smap with 
    | Some s ->
      let ic = Session.in_channel s in
      let oc = Session.out_channel s in
      let sn = (Declare.sn msg) in
      let csn = InChannel.rsn ic in
      if sn >= csn then
        begin
          InChannel.update_rsn ic sn  ;
          match%lwt process_declarations pe sid (Declare.declarations msg) with
          | [] ->
            let%lwt _ = Logs_lwt.debug (fun m -> m  "Acking Declare with sn: %d"  (Vle.to_int sn)) in 
            Lwt.return [Message.AckNack (AckNack.create (Vle.add sn 1L) None)]
          | _ as ds ->
            let%lwt _ = Logs_lwt.debug (fun m -> m "Sending Matching decalrations and ACKNACK \n") in
            Lwt.return [Message.Declare (Declare.create (true, true) (OutChannel.next_rsn oc) ds);
                        Message.AckNack (AckNack.create (Vle.add sn 1L) None)]
        end
      else
        begin
          let%lwt _ = Logs_lwt.debug (fun m -> m "Received out of oder message") in
          Lwt.return []
        end
      | None -> Lwt.return []

  let process_synch pe (sid : SID.t) msg =
    let asn = Synch.sn msg in
    Lwt.return [Message.AckNack (AckNack.create asn None)]

  let process_ack_nack pe sid msg = Lwt.return []


  let forward_data pe (sid : SID.t) msg =
      match SessionMap.find_opt sid pe.smap with
      | None -> Lwt.return_unit
      | Some s ->
        let%lwt _ = Logs_lwt.debug (fun m -> m  "Forwarding data for res : %s to session %Ld" (SID.show sid) (StreamData.id msg)) in
        let oc = Session.out_channel s  in
        let fsn = if reliable msg then OutChannel.next_rsn oc else  OutChannel.next_usn oc in
        let fwd_msg = StreamData.with_sn msg fsn in        

        get_tx_push pe sid @@ Event.SessionMessage (Frame.create [Message.StreamData fwd_msg], sid, None)

  let process_stream_data pe (sid : SID.t) msg =
    let id = StreamData.id msg in
    let subs = PubSubMap.find_opt id pe.submap in
    let sn = sn msg in
    let sn1 = Vle.add sn 1L in
    (* maybe_ack *)
    let _  = if synch msg then [Message.AckNack (AckNack.create sn1 None)] else [] in
    let%lwt _ = Logs_lwt.debug (fun m -> m "Handling Stream Data Message for resource: %Ld " id) in
    match subs with
    | None -> Lwt.return []
    | Some xs ->  
      let _ = (List.map (fun sid ->  (forward_data pe sid msg)) xs) in 
      Lwt.return []

  
  let process pe (sid : SID.t) msg push =
    let%lwt _ = Logs_lwt.debug (fun m -> m  "Received message: %s" (Message.to_string msg)) in
    let rs = match msg with
    | Message.Scout msg -> process_scout pe sid msg
    | Message.Hello _ -> Lwt.return []
    | Message.Open msg -> process_open pe sid msg push
    | Message.Close msg -> 
      let%lwt _ = remove_session pe sid in 
      Lwt.return [Message.Close (Close.create pe.pid '0')]

    | Message.Declare msg -> process_declare pe sid msg
    | Message.Synch msg -> process_synch pe sid msg
    | Message.AckNack msg -> process_ack_nack pe sid msg
    | Message.StreamData msg -> process_stream_data pe sid msg
    | Message.KeepAlive msg -> Lwt.return []
    | _ -> Lwt.return []
    in 
    match%lwt rs with 
    | [] -> Lwt.return_unit
    | _ as xs -> get_tx_push pe sid @@ (Event.SessionMessage (Frame.create xs, sid, None))


  let start pe =    
    let rec loop () =      
      let open Lwt.Infix in 
      let%lwt _ = Logs_lwt.debug (fun m -> m "Processing Protocol Engine Events") in
      (match%lwt Lwt_stream.get pe.evt_sink with 
      | Some(Event.SessionMessage (f, sid, Some push)) -> 
        let%lwt _ = Logs_lwt.debug (fun m -> m "Processing SessionMessage") in
        let msgs = Frame.to_list f in
        Lwt.join @@ List.map (fun msg -> process pe sid msg push) msgs 
      |Some _ -> 
        let%lwt _ = Logs_lwt.debug (fun m -> m "Processing Some other Event...") in
        Lwt.return_unit   
      | None -> 
        let%lwt _ = Logs_lwt.debug (fun m -> m "Processing None!!!") in
        Lwt.return_unit)  >>= loop
    in 
    let%lwt _ = Logs_lwt.debug (fun m -> m "Starting Protocol Engine") in
    loop ()
end
