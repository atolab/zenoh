open Lwt
open Lwt.Infix
open Ztypes
open Zlocator
open Zmessage
open Zsession
open Netbuf
open Printf

module SessionMap = Map.Make (SessionId)
module PubSubMap = Map.Make(Vle)

module ProtocolEngine = struct
  type t = {
    pid : IOBuf.t;
    lease : Vle.t;
    locators : Locators.t;
    mutable smap : Session.t SessionMap.t;
    mutable pubmap : (SessionId.t list) PubSubMap.t;
    mutable submap : (SessionId.t list) PubSubMap.t
  }

  let add_session pe s =
    let sid = Session.sid s in
    ignore_result @@ Lwt_log.debug (sprintf "Registering Session %Ld: \n" sid) ;
    let m = SessionMap.add sid s pe.smap in pe.smap <- m

  let remove_session pe s =
    let sid = Session.sid s in
    ignore_result @@ Lwt_log.debug (sprintf "Un-registering Session %Ld: \n" sid) ;
    let m = SessionMap.remove sid pe.smap in pe.smap <- m ;
    PubSubMap.iter (fun k xs ->
        let ys = List.filter (fun s -> s != sid) xs in
        let m = PubSubMap.add k ys pe.pubmap in pe.pubmap <- m
      ) pe.pubmap ;

    PubSubMap.iter (fun k xs ->
        let ys = List.filter (fun s -> s != sid) xs in
        let m = PubSubMap.add k ys pe.pubmap in pe.pubmap <- m
      ) pe.submap

  let add_publication pe s pd =
    let rid = PublisherDecl.rid pd in
    let sid = Session.sid s in
    let pubs = PubSubMap.find_opt rid pe.pubmap in
    ignore_result @@ Lwt_log.debug (sprintf "Registering Pub for resource %Ld in session %Ld: \n" rid sid) ;
    let m =  match pubs with
      | None -> PubSubMap.add rid [sid] pe.pubmap
      | Some xs -> PubSubMap.add rid (sid::xs) pe.pubmap
    in pe.pubmap <- m

  let add_subscription pe s sd =
    let rid = SubscriberDecl.rid sd in
    let sid = Session.sid s in
    let subs = PubSubMap.find_opt rid pe.submap in
    ignore_result @@ Lwt_log.debug (sprintf "Registering Sub for resource %Ld in session %Ld: \n" rid sid) ;

    let m = match subs with
      | None -> PubSubMap.add rid [sid] pe.submap
      | Some xs -> PubSubMap.add rid (sid::xs) pe.submap
    in pe.submap <- m

  let create pid lease ls = { pid; lease; locators = ls; smap = SessionMap.empty; pubmap = PubSubMap.empty; submap = PubSubMap.empty }

  let make_hello pe = Message.Hello (Hello.create (Vle.of_char ScoutFlags.scoutBroker) pe.locators [])

  let make_accept pe opid = Message.Accept (Accept.create opid pe.pid pe.lease Properties.empty)

  let process_scout pe s msg =
    if Vle.logand (Scout.mask msg) (Vle.of_char ScoutFlags.scoutBroker) <> 0L then [make_hello pe]
    else []

  let process_open pe s msg =
    ignore_result @@ Lwt_log.debug (sprintf "Accepting Open from remote peer: %s\n" (IOBuf.to_string @@ Open.pid msg)) ;
    let _ = add_session pe s in [make_accept pe (Open.pid msg)]

  let make_result pe s cd =
    let open Declaration in
    [Declaration.ResultDecl (ResultDecl.create (CommitDecl.commit_id cd) (char_of_int 0) None)]

  let notify_pub_matching_sub pe s sd =
    ignore_result @@ Lwt_log.debug @@ "Notifing Pub Matching Subs" ;
    let id = SubscriberDecl.rid sd in
    let open Apero in
    match PubSubMap.find_opt id pe.pubmap with
    | None -> ()
    | Some xs -> List.iter (fun sid ->
        ignore_result
        @@  match SessionMap.find_opt sid pe.smap with
        | None -> return_unit
        | Some s ->
          let sid = Session.sid s in
          let oc = Session.out_channel s in
          let ds = [Declaration.SubscriberDecl (SubscriberDecl.create id SubscriptionMode.push_mode Properties.empty)] in
          let decl = Message.Declare (Declare.create (true, true) (OutChannel.next_rsn oc) ds) in
          ignore_result @@ Lwt_log.debug @@ (sprintf "Notifing Pub Matching Subs -- sending SubscriberDecl for sid = %Ld" sid) ;
          Session.send s decl
      ) xs


  let match_sub pe s sd =
    ignore_result @@ Lwt_log.debug @@ "Matching SubDeclaration" ;
    let open Declaration in
    let id = SubscriberDecl.rid sd in
    add_subscription pe s sd ;
    notify_pub_matching_sub pe s sd;
    match PubSubMap.find_opt (SubscriberDecl.rid sd) pe.pubmap  with
    | None -> []
    | Some pubs -> [Declaration.PublisherDecl (PublisherDecl.create id Properties.empty)]

  let match_pub pe s pd =
    ignore_result @@ Lwt_log.debug @@ "Matching PubDeclaration" ;
    let id = PublisherDecl.rid pd in
    add_publication pe s pd ;
    match PubSubMap.find_opt (PublisherDecl.rid pd) pe.submap  with
    | None -> []
    | Some subs -> [Declaration.SubscriberDecl (SubscriberDecl.create id SubscriptionMode.push_mode Properties.empty)]

  let process_declaration pe s d =
    let open Declaration in
    match d with
    | PublisherDecl pd ->
      ignore_result @@ Lwt_log.debug @@ "PDecl for resource: " ^ (Vle.to_string @@ PublisherDecl.rid pd) ;
      match_pub pe s pd
    | SubscriberDecl sd ->
      ignore_result @@ Lwt_log.debug @@ "SDecl for resource: " ^ (Vle.to_string @@ SubscriberDecl.rid sd) ;
      match_sub pe s sd
    | CommitDecl cd -> make_result pe s cd
    | _ -> []

  let process_declarations pe s ds =
    ds
    |> List.map (fun d -> process_declaration pe s d)
    |> List.concat

  let process_declare pe s msg =
    ignore_result @@ Lwt_log.debug "Processing Declare Message\n" ;
    let ic = Session.in_channel s in
    let oc = Session.out_channel s in
    let sn = (Declare.sn msg) in
    let csn = InChannel.rsn ic in
    if sn >= csn then
      begin
        InChannel.update_rsn ic sn  ;
        match process_declarations pe s (Declare.declarations msg) with
        | [] ->
          ignore_result @@ (Lwt_log.debug @@ "Acking Declare with sn: " ^ (Vle.to_string sn) ^ "\n" );
          [Message.AckNack (AckNack.create (Vle.add sn 1L) None)]
        | _ as ds -> [Message.Declare (Declare.create (true, true) (OutChannel.next_rsn oc) ds);
                      Message.AckNack (AckNack.create (Vle.add sn 1L) None)]

      end
    else
      begin
        ignore_result @@ Lwt_log.debug "Received out of oder message"  ;
        []
      end

  let process_synch pe s msg =
    let asn = Synch.sn msg in
    [Message.AckNack (AckNack.create asn None)]

  let process_ack_nack pe sid msg = []


  let forward_data pe sid msg =
      match SessionMap.find_opt sid pe.smap with
      | None ->
        return_unit
      | Some s ->
        ignore_result @@ Lwt_log.debug @@ sprintf "Forwarding data for res : %Ld to session %Ld" (Session.sid s) (StreamData.id msg)
        ; let oc = Session.out_channel s  in
        let fsn = if StreamData.reliable msg then OutChannel.next_rsn oc else  OutChannel.next_usn oc in
        let fwd_msg = StreamData.with_sn msg fsn in
        (Session.send s) @@ Message.StreamData fwd_msg

  let process_stream_data pe s msg =
    let id = StreamData.id msg in
    let subs = PubSubMap.find_opt id pe.submap in
    ignore_result @@ Lwt_log.debug @@ (sprintf "Handling Stream Data Message for resource: %Ld " id)
    ; match subs with
      | None ->
        ignore_result @@ Lwt_log.debug @@ (sprintf "Empty Subscription List for session: %Ld " id) ;
        []
      | Some xs -> List.iter (fun sid -> ignore_result @@ (forward_data pe sid msg)) xs ; []

  let process pe s msg =
    ignore_result @@ Lwt_log.debug (sprintf "Received message: %s" (Message.to_string msg));
    match msg with
    | Message.Scout msg -> process_scout pe s msg
    | Message.Hello _ -> []
    | Message.Open msg -> process_open pe s msg
    | Message.Close msg -> ignore_result @@ Session.(s.close ()) ; []
    | Message.Declare msg -> process_declare pe s msg
    | Message.Synch msg -> process_synch pe s msg
    | Message.AckNack msg -> process_ack_nack pe s msg
    | Message.StreamData msg -> process_stream_data pe s msg
    | Message.KeepAlive msg -> []
    | _ -> []


end
