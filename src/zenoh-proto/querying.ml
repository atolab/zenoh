open Apero
open NetService
open R_name
open Engine_state

module Make (MVar : MVar) = struct

    let forward_query_to_session pe q sid =
      match SIDMap.find_opt sid pe.smap with
      | None -> let%lwt _ = Logs_lwt.debug (fun m -> m  "Unable to forward query to unknown session %s" (Id.show sid)) in Lwt.return_unit
      | Some s ->
        let%lwt _ = Logs_lwt.debug (fun m -> m  "Forwarding query to session %s" (Id.show s.sid)) in
        let sock = TxSession.socket s.tx_sex in 
        let open Lwt.Infix in 
        (Mcodec.ztcp_write_frame_pooled sock @@ Frame.Frame.create [Query(q)]) pe.buffer_pool >>= fun _ -> Lwt.return_unit


    let forward_query pe sid q = 
      let open Resource in 
      let open Lwt.Infix in 
      let (ss, ps) = ResMap.fold (fun _ res (sss, pss) -> 
          match ResName.name_match (ResName.URI(Message.Query.resource q)) res.name with 
          | false -> (sss, pss)
          | true -> 
            List.fold_left (fun (ss, ps) m ->
                match m.sto = true && m.session != sid && not @@ List.exists (fun s -> m.session == s) ss with
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
        let sid = TxSession.id tsex in
        let session = SIDMap.find_opt sid pe.smap in 
        let%lwt pe = match session with 
        | None -> let%lwt _ = Logs_lwt.warn (fun m -> m "Received Query on unknown session %s: Ignore it!" (Id.show sid)) in Lwt.return pe
        | Some session -> 
          let%lwt _ = Logs_lwt.debug (fun m -> 
                                          let nid = match List.find_opt (fun (peer:ZRouter.peer) -> 
                                              TxSession.id peer.tsex = session.sid) pe.router.peers with 
                                          | Some peer -> peer.pid
                                          | None -> "UNKNOWN" in
                                          m "Handling Query Message. nid[%s] sid[%s] res[%s]" 
                                          nid (Id.show session.sid) (Message.Query.resource q)) in
          let%lwt ss = forward_query pe session.sid q in
          Lwt.return @@ store_query pe session.sid ss q in
        Lwt.return (Lwt.return [], pe)
    
    let forward_reply_to_session pe r sid =
      match SIDMap.find_opt sid pe.smap with
      | None -> let%lwt _ = Logs_lwt.debug (fun m -> m  "Unable to forward reply to unknown session %s" (Id.show sid)) in Lwt.return_unit
      | Some s ->
        let%lwt _ = Logs_lwt.debug (fun m -> m  "Forwarding reply to session %s" (Id.show s.sid)) in
        let sock = TxSession.socket s.tx_sex in 
        let open Lwt.Infix in 
        (Mcodec.ztcp_write_frame_pooled sock @@ Frame.Frame.create [Reply(r)]) pe.buffer_pool >>= fun _ -> Lwt.return_unit
    
    let process_reply engine _(*tsex*) r = 
      MVar.guarded engine @@ fun pe -> 
        let qid = (Message.Reply.qpid r, Message.Reply.qid r) in 
        let%lwt _ = match QIDMap.find_opt qid pe.qmap with 
        | None -> Logs_lwt.debug (fun m -> m  "Received reply for unknown query. Ingore it!") 
        | Some qs -> forward_reply_to_session pe r qs.srcFace in
        Lwt.return (Lwt.return [], pe)

end