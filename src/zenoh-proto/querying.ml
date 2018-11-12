open Apero
open NetService
open R_name
open Engine_state

module Make (MVar : MVar) = struct

    let forward_query_to_mapping pe q dstmapsession =
      match SIDMap.find_opt dstmapsession pe.smap with
      | None -> Lwt.return_unit
      | Some s ->
        let%lwt _ = Logs_lwt.debug (fun m -> m  "Forwarding query to session %s" (Id.show s.sid)) in
        let sock = TxSession.socket s.tx_sex in 
        let open Lwt.Infix in 
        (Mcodec.ztcp_write_frame_pooled sock @@ Frame.Frame.create [Query(q)]) pe.buffer_pool >>= fun _ -> Lwt.return_unit


    let forward_query pe sid q = 
      let open Resource in 
      let (_, ps) = ResMap.fold (fun _ res (sss, pss) -> 
          match ResName.name_match (ResName.URI(Message.Query.resource q)) res.name with 
          | false -> (sss, pss)
          | true -> 
            List.fold_left (fun (ss, ps) m ->
                match m.sto = true && m.session != sid && not @@ List.exists (fun s -> m.session == s) ss with
                | true -> 
                  let p = forward_query_to_mapping pe q m.session in
                  (m.session :: ss , p :: ps)
                | false -> (ss, ps)
              ) (sss, pss) res.mappings 
        ) pe.rmap ([], []) in
      Lwt.join ps 

    let process_query engine tsex q =    
      let%lwt pe = MVar.read engine in
      let open Session in 
      let sid = TxSession.id tsex in
      let session = SIDMap.find_opt sid pe.smap in 
      match session with 
      | None -> let%lwt _ = Logs_lwt.warn (fun m -> m "Received Query on unknown session %s: Ignore it!" 
                                              (Id.show sid)) in Lwt.return []
      | Some session -> 
        let%lwt _ = Logs_lwt.debug (fun m -> 
                                        let nid = match List.find_opt (fun (peer:ZRouter.peer) -> 
                                            TxSession.id peer.tsex = session.sid) pe.router.peers with 
                                        | Some peer -> peer.pid
                                        | None -> "UNKNOWN" in
                                        m "Handling Query Message. nid[%s] sid[%s] res[%s]" 
                                        nid (Id.show session.sid) (Message.Query.resource q)) in
        let%lwt _ = forward_query pe session.sid q in
        Lwt.return []

end