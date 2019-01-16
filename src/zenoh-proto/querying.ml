open Apero
open NetService
open R_name
open Engine_state

module Make (MVar : MVar) = struct

    let forward_query_to_session pe q sid =
      match SIDMap.find_opt sid pe.smap with
      | None -> let%lwt _ = Logs_lwt.debug (fun m -> m  "Unable to forward query to unknown session %s" (Id.to_string sid)) in Lwt.return_unit
      | Some s ->
        let%lwt _ = Logs_lwt.debug (fun m -> m  "Forwarding query to session %s" (Id.to_string s.sid)) in
        let sock = TxSession.socket s.tx_sex in 
        let open Lwt.Infix in 
        (Mcodec.ztcp_write_frame_pooled sock @@ Frame.Frame.create [Query(q)]) pe.buffer_pool >>= fun _ -> Lwt.return_unit
    
    let forward_reply_to_session pe r sid =
      match SIDMap.find_opt sid pe.smap with
      | None -> let%lwt _ = Logs_lwt.debug (fun m -> m  "Unable to forward reply to unknown session %s" (Id.to_string sid)) in Lwt.return_unit
      | Some s ->
        let%lwt _ = Logs_lwt.debug (fun m -> m  "Forwarding reply to session %s" (Id.to_string s.sid)) in
        let sock = TxSession.socket s.tx_sex in 
        let open Lwt.Infix in 
        (Mcodec.ztcp_write_frame_pooled sock @@ Frame.Frame.create [Reply(r)]) pe.buffer_pool >>= fun _ -> Lwt.return_unit

    let forward_query pe sid q = 
      let open Resource in 
      let open Lwt.Infix in 
      let (ss, ps) = ResMap.fold (fun _ res (sss, pss) -> 
          match ResName.name_match (ResName.Path(PathExpr.of_string @@ Message.Query.resource q)) res.name with 
          | false -> (sss, pss)
          | true -> 
            List.fold_left (fun (ss, ps) m ->
                match m.sto != None && m.session != sid && not @@ List.exists (fun s -> m.session == s) ss with
                | true -> 
                  let p = forward_query_to_session pe q m.session in
                  (m.session :: ss , p :: ps)
                | false -> (ss, ps)
              ) (sss, pss) res.mappings 
        ) pe.rmap ([], []) in
      Lwt.join ps >>= fun _ -> Lwt.return ss

    let store_query pe srcFace fwdFaces q =
      let open Query in
      let qmap = QIDMap.add (Message.Query.pid q, Message.Query.qid q) {srcFace; fwdFaces} pe.qmap in 
      {pe with qmap}

    let process_query engine tsex q = 
      MVar.guarded engine @@ fun pe -> 
        let open Session in 
        let open Lwt.Infix in
        let sid = TxSession.id tsex in
        let session = SIDMap.find_opt sid pe.smap in 
        let%lwt pe = match session with 
        | None -> let%lwt _ = Logs_lwt.warn (fun m -> m "Received Query on unknown session %s: Ignore it!" (Id.to_string sid)) in Lwt.return pe
        | Some session -> 
          let%lwt _ = Logs_lwt.debug (fun m -> 
                                          let nid = match List.find_opt (fun (peer:ZRouter.peer) -> 
                                              TxSession.id peer.tsex = session.sid) pe.router.peers with 
                                          | Some peer -> peer.pid
                                          | None -> "UNKNOWN" in
                                          m "Handling Query Message. nid[%s] sid[%s] res[%s]" 
                                          nid (Id.to_string session.sid) (Message.Query.resource q)) in
          let%lwt ss = forward_query pe session.sid q in
          match ss with 
          | [] -> forward_reply_to_session pe (Message.Reply.create (Message.Query.pid q) (Message.Query.qid q) None) session.sid >>= fun _ -> Lwt.return pe
          | ss -> Lwt.return @@ store_query pe session.sid ss q in
        Lwt.return (Lwt.return [], pe)
    
    let process_reply engine tsex r = 
      let open Lwt.Infix in
      MVar.guarded engine @@ fun pe -> 
        let qid = (Message.Reply.qpid r, Message.Reply.qid r) in 
        let%lwt pe = match QIDMap.find_opt qid pe.qmap with 
        | None -> Logs_lwt.debug (fun m -> m  "Received reply for unknown query. Ingore it!") >>= fun _ -> Lwt.return pe
        | Some qs -> 
          (match Message.Reply.value r with 
          | Some _ -> forward_reply_to_session pe r qs.srcFace >>= fun _ -> Lwt.return pe
          | None -> 
            let fwdFaces = List.filter (fun face -> 
              match SIDMap.find_opt face pe.smap with 
              | Some s -> s.tx_sex != tsex
              | None -> true) qs.fwdFaces in 
            let%lwt qmap = match fwdFaces with 
            | [] -> forward_reply_to_session pe r qs.srcFace >>= fun _ -> Lwt.return @@ QIDMap.remove qid pe.qmap
            | _ -> Lwt.return @@ QIDMap.add qid {qs with fwdFaces} pe.qmap in 
            Lwt.return {pe with qmap} ) in
        Lwt.return (Lwt.return [], pe)

end