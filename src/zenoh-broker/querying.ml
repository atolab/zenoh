open Apero
open NetService
open R_name
open Engine_state

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

let forward_query_to pe q = List.map (fun s -> forward_query_to_session pe q s)

let forward_query pe sid q = 
  let open Resource in 
  let open Lwt.Infix in 
  let dest = match ZProperty.QueryDest.find_opt (Message.Query.properties q) with 
    | None -> Queries.Partial
    | Some prop -> ZProperty.QueryDest.dest prop in 

  let get_complete_faces () = 
    ResMap.fold (fun _ res accu -> match res.name with 
        | ID _ -> accu
        | Path path -> 
          (match PathExpr.includes ~subexpr:(PathExpr.of_string @@ Message.Query.resource q) path with 
           | false -> accu 
           | true -> 
             List.fold_left (fun accu m -> 
                 match m.sto != None && m.session != sid && not @@ List.exists (fun (s, _) -> m.session == s) accu with
                 | true -> (m.session, Option.get m.sto) :: accu
                 | false -> accu
               ) accu res.mappings)) pe.rmap [] in

  let get_matching_faces () = 
    ResMap.fold (fun _ res accu -> 
        match ResName.name_match (ResName.Path(PathExpr.of_string @@ Message.Query.resource q)) res.name with 
        | false -> accu 
        | true -> 
          List.fold_left (fun accu m -> 
              match m.sto != None && m.session != sid && not @@ List.exists (fun s -> m.session == s) accu with
              | true -> m.session :: accu
              | false -> accu
            ) accu res.mappings) pe.rmap [] in

  let (ps, ss) = match dest with 
    | All -> let matching_faces = get_matching_faces () in (forward_query_to pe q matching_faces, matching_faces) 
    | Complete _ -> let complete_faces = get_complete_faces () |> List.map (fun (face, _) -> face) in (forward_query_to pe q complete_faces, complete_faces) 
    (* TODO : manage quorum *)
    | Partial -> 
      (let complete_faces = get_complete_faces () in
       match complete_faces with 
       | [] -> let matching_faces = get_matching_faces () in (forward_query_to pe q matching_faces, matching_faces)
       | faces -> 
         let (nearest_face, _) = List.fold_left (fun (accu, accudist) (s, sdist) -> 
             match sdist < accudist with 
             | true -> (s, sdist)
             | false -> (accu, accudist)) (List.hd faces) faces in 
         (forward_query_to pe q [nearest_face], [nearest_face]))

  in Lwt.join ps >>= fun _ -> Lwt.return ss

let store_query pe srcFace fwdFaces q =
  let open Query in
  let qmap = QIDMap.add (Message.Query.pid q, Message.Query.qid q) {srcFace; fwdFaces} pe.qmap in 
  {pe with qmap}


let process_query engine tsex q = 
  Guard.guarded engine @@ fun pe -> 
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
          m "Handling Query Message. nid[%s] sid[%s] pid[%s] qid[%d] res[%s]" 
            nid (Id.to_string session.sid) (MIOBuf.hexdump (Message.Query.pid q)) (Int64.to_int (Message.Query.qid q)) (Message.Query.resource q)) in
      let%lwt ss = forward_query pe session.sid q in
      match ss with 
      | [] -> forward_reply_to_session pe (Message.Reply.create (Message.Query.pid q) (Message.Query.qid q) None) session.sid >>= fun _ -> Lwt.return pe
      | ss -> Lwt.return @@ store_query pe session.sid ss q in
  Guard.return [] pe

let process_reply engine tsex r = 
  let open Lwt.Infix in
  Guard.guarded engine @@ fun pe -> 
  let qid = (Message.Reply.qpid r, Message.Reply.qid r) in 
  let%lwt pe = match QIDMap.find_opt qid pe.qmap with 
    | None -> Logs_lwt.debug (fun m -> m  "Received reply for unknown query. Ingore it!") >>= fun _ -> Lwt.return pe
    | Some qs -> 
      let%lwt _ = Logs_lwt.debug (fun m -> 
          let nid = match List.find_opt (fun (peer:ZRouter.peer) -> 
              TxSession.id peer.tsex = (TxSession.id tsex)) pe.router.peers with 
          | Some peer -> peer.pid
          | None -> "UNKNOWN" in
          let resource = match (Message.Reply.resource r) with 
            | Some res -> res
            | None -> "" in
          m "Handling Reply Message. nid[%s] sid[%s] qpid[%s] qid[%d] res[%s]" 
            nid (Id.to_string (TxSession.id tsex)) 
            (MIOBuf.hexdump (Message.Reply.qpid r)) (Int64.to_int (Message.Reply.qid r))
            resource) in
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
  Guard.return [] pe

