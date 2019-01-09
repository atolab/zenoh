open Apero
open Channel
open NetService
open R_name
open Engine_state


module Make (MVar : MVar) = struct
 
    module MDiscovery = Discovery.Make(MVar) open MDiscovery

    let is_pulled pe resname = 
      let open Resource in 
      ResMap.bindings pe.rmap |> 
      List.exists (fun (rname, res) -> 
          ResName.name_match resname rname &&
          List.exists (fun m -> m.sub = Some true) res.mappings)

    let store_data pe resname payload = 
      update_resource_opt pe resname  (fun r -> match r with 
          | Some res -> Some{res with last_value=Some payload}
          | None -> match is_pulled pe resname with 
            | true -> Some{name=resname; mappings=[]; matches=[]; local_id=0L; last_value=Some payload}
            | false -> None)

    let forward_data_to_mapping pe srcresname dstres dstmapsession dstmapid reliable payload =
      let open Resource in 
      let open ResName in 
      match SIDMap.find_opt dstmapsession pe.smap with
      | None -> Lwt.return_unit
      | Some s ->
        let%lwt _ = Logs_lwt.debug (fun m -> m  "Forwarding data to session %s" (Id.to_string s.sid)) in
        let oc = Session.out_channel s in
        let fsn = if reliable then OutChannel.next_rsn oc else  OutChannel.next_usn oc in
        let msg = match srcresname with 
          | ID id -> Message.StreamData(Message.StreamData.create (true, reliable) fsn id None payload)
          | Path uri -> match srcresname = dstres.name with 
            | true -> Message.StreamData(Message.StreamData.create (true, reliable) fsn dstmapid None payload)
            | false -> WriteData(Message.WriteData.create (true, reliable) fsn (PathExpr.to_string uri) payload) 
        in

        let sock = TxSession.socket s.tx_sex in 
        let open Lwt.Infix in 
        (Mcodec.ztcp_write_frame_pooled sock @@ Frame.Frame.create [msg]) pe.buffer_pool >>= fun _ -> Lwt.return_unit

    let forward_data pe sid srcres reliable payload = 
      let open Resource in
      let (_, ps) = List.fold_left (fun (sss, pss) name -> 
          match ResMap.find_opt name pe.rmap with 
          | None -> (sss, pss)
          | Some r -> 
            List.fold_left (fun (ss, ps) m ->
                match (m.sub = Some false || m.sto = true) && m.session != sid && not @@ List.exists (fun s -> m.session == s) ss with
                | true -> 
                  let p = forward_data_to_mapping pe srcres.name r m.session m.id reliable payload in
                  (m.session :: ss , p :: ps)
                | false -> (ss, ps)
              ) (sss, pss) r.mappings 
        ) ([], []) srcres.matches in 
      Lwt.join ps 

    let forward_oneshot_data pe sid srcresname reliable payload = 
      let open Resource in 
      let (_, ps) = ResMap.fold (fun _ res (sss, pss) -> 
          match ResName.name_match srcresname res.name with 
          | false -> (sss, pss)
          | true -> 
            List.fold_left (fun (ss, ps) m ->
                match (m.sub = Some false || m.sto = true) && m.session != sid && not @@ List.exists (fun s -> m.session == s) ss with
                | true -> 
                  let p = forward_data_to_mapping pe srcresname res m.session m.id reliable payload in
                  (m.session :: ss , p :: ps)
                | false -> (ss, ps)
              ) (sss, pss) res.mappings 
        ) pe.rmap ([], []) in
      Lwt.join ps 

    let process_user_streamdata (pe:engine_state) session msg =      
      let open Session in
      let open Resource in
      let rid = Message.StreamData.id msg in
      let name = match VleMap.find_opt rid session.rmap with 
        | None -> (match ResMap.bindings pe.rmap |> List.find_opt (fun (_, res) -> res.local_id = rid) with 
          | None -> ResName.ID(rid)
          | Some (_, res) -> res.name)
        | Some name -> name in 
      match store_data pe name (Message.StreamData.payload msg) with 
      | (pe, None) -> let%lwt _ = Logs_lwt.warn (fun m -> m "Received StreamData for unknown resource %s on session %s: Ignore it!" 
                                                    (ResName.to_string name) (Id.to_string session.sid)) in Lwt.return (pe, [])
      | (pe, Some res) -> 
        let%lwt _ = Logs_lwt.debug (fun m -> 
                                    let nid = match List.find_opt (fun (peer:ZRouter.peer) -> 
                                        TxSession.id peer.tsex = session.sid) pe.router.peers with 
                                    | Some peer -> peer.pid
                                    | None -> "UNKNOWN" in
                                    m "Handling StreamData Message. nid[%s] sid[%s] rid[%Ld] res[%s]"
                                     nid (Id.to_string session.sid) rid (match res.name with Path u -> PathExpr.to_string u | ID _ -> "UNNAMED")) in
        let%lwt _ = forward_data pe session.sid res (Message.Reliable.reliable msg) (Message.StreamData.payload msg) in
        Lwt.return (pe, [])

    let process_user_writedata pe session msg =      
      let open Session in 
      let%lwt _ = Logs_lwt.debug (fun m -> 
                                    let nid = match List.find_opt (fun (peer:ZRouter.peer) -> 
                                        TxSession.id peer.tsex = session.sid) pe.router.peers with 
                                    | Some peer -> peer.pid
                                    | None -> "UNKNOWN" in
                                    m "Handling WriteData Message. nid[%s] sid[%s] res[%s]" 
                                    nid (Id.to_string session.sid) (Message.WriteData.resource msg)) in
      let name = ResName.Path(PathExpr.of_string @@ Message.WriteData.resource msg) in
      match store_data pe name (Message.WriteData.payload msg) with 
      | (pe, None) -> 
        let%lwt _ = forward_oneshot_data pe session.sid name (Message.Reliable.reliable msg) (Message.WriteData.payload msg) in
        Lwt.return (pe, [])
      | (pe, Some res) -> 
        let%lwt _ = forward_data pe session.sid res (Message.Reliable.reliable msg) (Message.WriteData.payload msg) in
        Lwt.return (pe, [])


    let process_pull engine tsex msg =
      let sid = TxSession.id tsex in 
      let open Lwt.Infix in 
      MVar.read engine >>= fun pe -> 
      let session = SIDMap.find_opt sid pe.smap in 
      match session with 
      | None -> let%lwt _ = Logs_lwt.warn (fun m -> m "Received Pull on unknown session %s: Ignore it!" 
                                              (Id.to_string sid)) in Lwt.return []
      | Some session -> 
        let rid = Message.Pull.id msg in
        let name = match VleMap.find_opt rid session.rmap with 
          | None -> ResName.ID(rid)
          | Some name -> name in 
        match ResMap.find_opt name pe.rmap with 
        | None -> let%lwt _ = Logs_lwt.warn (fun m -> m "Received Pull for unknown resource %s on session %s: Ignore it!" 
                                                (ResName.to_string name) (Id.to_string session.sid)) in Lwt.return []
        | Some res -> 
          let%lwt _ = Logs_lwt.debug (fun m -> m "Handling Pull Message for resource: [%s:%Ld] (%s)" 
                                         (Id.to_string session.sid) rid (match res.name with Path u -> PathExpr.to_string u | ID _ -> "UNNAMED")) in
          let%lwt _ = Lwt_list.iter_p (fun mresname -> 
              let mres = ResMap.find mresname pe.rmap in
              match mres.last_value with 
              | None -> Lwt.return_unit
              | Some v -> forward_data_to_mapping pe mres.name res sid rid true v
            ) res.matches in Lwt.return []


    let rspace msg =       
      let open Message in 
      List.fold_left (fun res marker -> 
          match marker with 
          | RSpace rs -> RSpace.id rs 
          | _ -> res) 0L (markers msg)

    let process_broker_data pe session msg = 
      let open Session in      
      let%lwt _ = Logs_lwt.debug (fun m -> m "Received tree state on %s\n" (Id.to_string session.sid)) in
      let b = Lwt_bytes.to_bytes @@ IOBuf.to_bytes @@ Message.StreamData.payload msg in
      let node = Marshal.from_bytes b 0 in
      let pe = {pe with router = ZRouter.update pe.router node} in
      let%lwt _ = Logs_lwt.debug (fun m -> m "Spanning trees status :\n%s" (ZRouter.report pe.router)) in
      forward_all_decls pe;
      Lwt.return (pe, []) 

    let process_stream_data engine tsex msg =
      MVar.guarded engine
      @@ fun pe ->
      let%lwt (pe, ms) = 
        let sid = TxSession.id tsex in 
        let session = SIDMap.find_opt sid pe.smap in 
        match session with 
        | None -> let%lwt _ = Logs_lwt.warn (fun m -> m "Received StreamData on unknown session %s: Ignore it!" 
                                                (Id.to_string sid)) in Lwt.return (pe, [])
        | Some session -> 
          match rspace (Message.StreamData(msg)) with 
          | 1L -> process_broker_data pe session msg
          | _ -> process_user_streamdata pe session msg
      in MVar.return ms pe

    let process_write_data engine tsex msg =
      MVar.guarded engine
      @@ fun pe ->
      let%lwt (pe, ms) = 
        let sid = TxSession.id tsex in
        let session = SIDMap.find_opt sid pe.smap in 
        match session with 
        | None -> let%lwt _ = Logs_lwt.warn (fun m -> m "Received WriteData on unknown session %s: Ignore it!" 
                                                (Id.to_string sid)) in Lwt.return (pe, [])
        | Some session -> 
          match rspace (Message.WriteData(msg)) with 
          | 1L -> Lwt.return (pe, []) 
          | _ -> process_user_writedata pe session msg
      in MVar.return ms pe
end