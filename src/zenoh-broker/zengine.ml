open Ztypes
open Zenoh
open Lwt
open Lwt.Infix
open Session

module SessionMap = Map.Make (SessionId)
module PubSubMap = Map.Make(Vle)

module ProtocolEngine = struct
  type t = {
    pid : Lwt_bytes.t;
    lease : Vle.t;
    locators : Locators.t;
    mutable smap : Session.t SessionMap.t;
    mutable pubmap : (SessionId.t list) PubSubMap.t;
    mutable submap : (SessionId.t list) PubSubMap.t
  }

  let create pid lease ls = { pid; lease; locators = ls; smap = SessionMap.empty; pubmap = PubSubMap.empty; submap = PubSubMap.empty }

  let make_hello pe = Message.Hello (Hello.create (Vle.of_char ScoutFlags.scoutBroker) pe.locators [])

  let make_accept pe opid = Message.Accept (Accept.create pe.pid opid pe.lease Properties.empty)

  let process_scout pe s msg =
    if Vle.logand (Scout.mask msg) (Vle.of_char ScoutFlags.scoutBroker) <> 0L then [make_hello pe]
    else []

  let process_open pe s msg =
    ignore_result @@ Lwt_log.debug (Printf.sprintf "Accepting Open from remote peer: %s\n" (Lwt_bytes.to_string @@ Open.pid msg)) ;
    pe.smap <- SessionMap.add Session.(s.sid) s pe.smap ;
    [make_accept pe (Open.pid msg)]

  let make_result pe s cd =
    let open Declaration in
    [Declaration.ResultDecl (ResultDecl.create (CommitDecl.commit_id cd) (char_of_int 0) None)]

  let match_sub pe s sd =
    let open Declaration in
    match PubSubMap.find_opt (SubscriberDecl.rid sd) pe.pubmap  with
    | None -> []
    | Some pubs ->
      []

  let match_pub pe s sd = []

  let process_declaration pe s d =
    let open Declaration in
    match d with
    | PublisherDecl pd -> match_pub pe s pd
    | SubscriberDecl sd -> match_sub pe s sd
    | CommitDecl cd -> make_result pe s cd
    | _ -> []

  let process_declarations pe s ds =
    ds
    |> List.map (fun d -> process_declaration pe s d)
    |> List.concat

  let process_declare pe s msg =
    let ic = Session.in_channel s in
    let oc = Session.out_channel s in
    let sn = (Declare.sn msg) in
    let csn = InChannel.rsn ic in
    if sn > csn then
      begin
        InChannel.update_rsn ic sn  ;
        match process_declarations pe s (Declare.declarations msg) with
        | [] -> []
        | _ as ds -> [Message.Declare (Declare.create (OutChannel.next_rsn oc) ds false false);
                      Message.AckNack (AckNack.create sn None)]

      end
    else
      begin
        ignore_result @@ Lwt_log.debug "Received out of oder message"  ;
        []
      end



  let process_synch pe s msg = []
  let process_ack_nack pe s msg = []
  let process_stream_data pe s msg = []

  let process pe s msg =
    ignore_result @@ Lwt_log.debug (Printf.sprintf "Received message: %s" (Message.to_string msg));
    match msg with
    | Message.Scout msg -> process_scout pe s msg
    | Message.Hello _ -> []
    | Message.Open msg -> process_open pe s msg
    | Message.Close msg -> ignore_result @@ Session.(s.close ()) ; []
    | Message.Declare msg -> process_declare pe s msg
    | Message.Synch msg -> process_synch pe s msg
    | Message.AckNack msg -> process_ack_nack pe s msg
    | Message.StreamData msg -> process_stream_data pe s msg
    | _ -> []


end
