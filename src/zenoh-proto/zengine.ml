open Apero
open Apero_net
open Channel
open NetService
open R_name

module ZEngine (MVar : MVar) = struct

  module SIDMap = Map.Make(NetService.Id)

  module Resource = struct 

    type mapping = {
      id : Vle.t;
      session : Id.t;
      pub : bool;
      sub : bool option;
      matched_pub : bool;
      matched_sub : bool;
    }

    type t = {
      name : ResName.t;
      mappings : mapping list;
      matches : ResName.t list;
      local_id : Vle.t;
      last_value : IOBuf.t option;
    }

    

    let report_mapping m = 
      Printf.sprintf "SID:%2s RID:%2d PUB:%-4s SUB:%-4s" 
        (Id.show m.session) (Vle.to_int m.id)
        (match m.pub with true -> "YES" | false -> "NO")
        (match m.sub with None -> "NO" | Some true -> "PULL" | Some false -> "PUSH")

    let report res = 
      Printf.sprintf "Resource name %s\n  mappings:\n" (ResName.to_string res.name) |> fun s ->
      List.fold_left (fun s m -> s ^ "    " ^ report_mapping m ^ "\n") s res.mappings ^ 
      "  matches:\n" |> fun s -> List.fold_left (fun s mr -> s ^ "    " ^ (ResName.to_string mr) ^ "\n") s res.matches

    let with_mapping res mapping = 
      {res with mappings = mapping :: List.filter (fun m -> not (Id.equal m.session mapping.session)) res.mappings}

    let update_mapping res sid updater = 
      let mapping = List.find_opt (fun m -> m.session = sid) res.mappings in 
      let mapping = updater mapping in
      with_mapping res mapping

    let remove_mapping res sid = 
      {res with mappings = List.filter (fun m -> not (Id.equal m.session sid)) res.mappings}

    let with_match res mname = 
      {res with matches = mname :: List.filter (fun r -> r != mname) res.matches}

    let remove_match res mname = 
      {res with matches = List.filter (fun r -> r != mname) res.matches}

    let res_match res1 res2 = ResName.name_match res1.name res2.name
  end


  module Session : sig
    type t = {      
      tx_sex : TxSession.t;
      ic : InChannel.t;
      oc : OutChannel.t;
      rmap : ResName.t VleMap.t;
      mask : Vle.t;
      sid : Id.t
    }
    val create : TxSession.t -> Vle.t -> t
    val in_channel : t -> InChannel.t
    val out_channel : t -> OutChannel.t
    val tx_sex : t -> TxSession.t  
    val id : t -> Id.t
  end = struct

    type t = {    
      tx_sex : TxSession.t;      
      ic : InChannel.t;
      oc : OutChannel.t;
      rmap : ResName.t VleMap.t;
      mask : Vle.t;
      sid : Id.t
    }

    let create tx_sex mask =
      let ic = InChannel.create Int64.(shift_left 1L 16) in
      let oc = OutChannel.create Int64.(shift_left 1L 16) in        
      {      
        tx_sex;
        ic;
        oc;
        rmap = VleMap.empty; 
        mask = mask;
        sid = TxSession.id tx_sex
      }
    let in_channel s = s.ic
    let out_channel s = s.oc
    let tx_sex s = s.tx_sex
    let id s = TxSession.id s.tx_sex    
  end

  module ProtocolEngine = struct

    type tx_session_connector = Locator.t -> TxSession.t Lwt.t 

    type engine_state = {
      pid : IOBuf.t;
      lease : Vle.t;
      locators : Locators.t;
      smap : Session.t SIDMap.t;
      rmap : Resource.t ResMap.t;      
      peers : Locator.t list;
      router : ZRouter.t;
      next_mapping : Vle.t;
      tx_connector : tx_session_connector;
      buffer_pool : IOBuf.t Lwt_pool.t

    }

    type t = engine_state MVar.t

    let report_resources e = 
      List.fold_left (fun s (_, r) -> s ^ Resource.report r ^ "\n") "" (ResMap.bindings e.rmap)

    let next_mapping pe = 
      let next = pe.next_mapping in
      ({pe with next_mapping = Vle.add next 1L}, next)

    let send_nodes peer _nodes = 
      let open Message in
      let open Frame in 
      List.iter (fun node -> 
          let b = Marshal.to_bytes node [] in
          let sdata = Message.with_marker               
              (StreamData(StreamData.create (true, true) 0L 0L None (IOBuf.from_bytes (Lwt_bytes.of_bytes b))))
              (RSpace (RSpace.create 1L)) in           
          Lwt.ignore_result @@ Mcodec.ztcp_write_frame_alloc (TxSession.socket ZRouter.(peer.tsex)) (Frame.create [sdata]) ) _nodes

    let send_nodes peers nodes = List.iter (fun peer -> send_nodes peer nodes) peers

    let pid_to_string = IOBuf.hexdump

    let make_scout = Message.Scout (Message.Scout.create (Vle.of_char Message.ScoutFlags.scoutBroker) [])

    let make_hello pe = Message.Hello (Message.Hello.create (Vle.of_char Message.ScoutFlags.scoutBroker) pe.locators [])

    let make_open pe = 
      let buf = IOBuf.create 16 in 
      let buf = Result.get @@ IOBuf.put_string "broker" buf in
      Message.Open (Message.Open.create (char_of_int 0) pe.pid 0L pe.locators [Zproperty.ZProperty.make Vle.zero buf])

    let make_accept pe opid = Message.Accept (Message.Accept.create opid pe.pid pe.lease [])
    
    let create ?(bufn = 32) ?(buflen=65536) (pid : IOBuf.t) (lease : Vle.t) (ls : Locators.t) (peers : Locator.t list) strength (tx_connector: tx_session_connector) = 
      MVar.create @@ { 
        pid; 
        lease; 
        locators = ls; 
        smap = SIDMap.empty; 
        rmap = ResMap.empty; 
        peers;
        router = ZRouter.create send_nodes (IOBuf.hexdump pid) strength 2 0;
        next_mapping = 0L; 
        tx_connector;
        buffer_pool = Lwt_pool.create bufn (fun () -> Lwt.return @@ IOBuf.create buflen) }

    let rec connect_peer peer connector max_retries = 
      let open Frame in 
      Lwt.catch 
        (fun () ->
          let%lwt _ = Logs_lwt.debug (fun m -> m "Connecting to peer %s" (Locator.to_string peer)) in 
          let%lwt tx_sex = connector peer in
          let sock = TxSession.socket tx_sex in 
          let frame = Frame.create [make_scout] in 
          let%lwt _ = Logs_lwt.debug (fun m -> m "Sending scout to peer %s" (Locator.to_string peer)) in 
          Mcodec.ztcp_write_frame_alloc sock frame )
        (fun _ -> 
          let%lwt _ = Logs_lwt.debug (fun m -> m "Failed to connect to %s" (Locator.to_string peer)) in 
          let%lwt _ = Lwt_unix.sleep 1.0 in 
          if max_retries > 0 then connect_peer peer connector (max_retries -1)
          else Lwt.fail_with ("Permanently Failed to connect to " ^ (Locator.to_string peer)))

    let connect_peers pe =        
      let open Lwt.Infix in 
       Lwt_list.iter_p (fun p -> 
        let%lwt _ = Logs_lwt.debug (fun m -> m "Trying to establish connection to %s" (Locator.to_string p)) in 
        Lwt.catch
          (fun _ -> (connect_peer p  pe.tx_connector 1000) >|= ignore )
          (fun ex -> let%lwt _ = Logs_lwt.warn (fun m -> m "%s" (Printexc.to_string ex)) in Lwt.return_unit)
        ) pe.peers

    let start engine = 
      let%lwt pe = MVar.read engine in  
      let%lwt _ = Logs_lwt.debug (fun m -> m "Going to establish connection  to %d peers" (List.length pe.peers)) in 
      connect_peers pe

    
    
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

    let update_resource_opt pe name updater = 
      let optres = ResMap.find_opt name pe.rmap in 
      let optres' = updater optres in
      match optres' with 
      | Some res -> 
        let (rmap, res') = match optres with 
          | None -> match_resource pe.rmap res
          | Some _ -> (pe.rmap, res) in
        let rmap = ResMap.add res'.name res' rmap in 
        ({pe with rmap}, optres')
      | None -> (pe, None)

    let update_resource pe name updater = 
      let (pe, optres) = update_resource_opt pe name (fun ores -> Some (updater ores)) in 
      (pe, Option.get optres)

    let update_resource_mapping pe name (session:Session.t) rid updater =       
      let sid = Session.id session in 
      Logs.debug (fun m -> m "Register resource '%s' mapping [sid : %s, rid : %d]" (ResName.to_string name) (Id.to_string sid) (Vle.to_int rid));
      let (pe, local_id) = match name with 
        | URI _ -> next_mapping pe
        | ID id -> (pe, id) in
      let(pe, res) = update_resource pe name 
          (fun r -> match r with 
             | Some res -> Resource.update_mapping res (TxSession.id @@ Session.tx_sex session) updater
             | None -> {name; mappings=[updater None]; matches=[name]; local_id; last_value=None}) in
      let session = {session with rmap=VleMap.add rid res.name session.rmap;} in      
      let smap = SIDMap.add session.sid session pe.smap in
      ({pe with smap}, res)

    let declare_resource pe tsex rd =

      let sid = TxSession.id tsex in 
      let session = SIDMap.find_opt sid pe.smap in 
      match session with 
      | None -> let%lwt _ = Logs_lwt.warn (fun m -> m "Received ResourceDecl on unknown session %s: Ignore it!" (Id.show sid)) in Lwt.return pe
      | Some session -> 
        let rid = Message.ResourceDecl.rid rd in 
        let uri = Message.ResourceDecl.resource rd in 
        let (pe, _) = update_resource_mapping pe (URI(uri)) session rid 
            (fun m -> match m with 
               | Some mapping -> mapping
               | None -> {id = rid; session = session.sid; pub = false; sub = None; matched_pub = false; matched_sub=false}) in 
        Lwt.return pe

    (* ======================== PUB DECL =========================== *)

    let match_pdecl pe pr id tsex =
      let open Resource in 
      let sid = TxSession.id tsex in 
      let pm = List.find (fun m -> m.session = sid) pr.mappings in
      match pm.matched_pub with 
      | true -> Lwt.return (pe, [])
      | false -> 
        match ResMap.exists 
                (fun _ sr ->  res_match pr sr && List.exists 
                                (fun m -> m.sub != None && m.session != sid) sr.mappings) pe.rmap with
        | false -> Lwt.return (pe, [])
        | true -> 
          let pm = {pm with matched_pub = true} in
          let pr = with_mapping pr pm in
          let rmap = ResMap.add pr.name pr pe.rmap in
          let pe = {pe with rmap} in
          Lwt.return (pe, [Message.Declaration.SubscriberDecl (Message.SubscriberDecl.create id Message.SubscriptionMode.push_mode [])])

    let register_publication pe tsex pd =
      let sid = (TxSession.id tsex) in 
      let session = SIDMap.find_opt sid  pe.smap in 
      match session with 
      | None -> let%lwt _ = Logs_lwt.warn (fun m -> m "Received PublicationDecl on unknown session %s: Ignore it!" 
                                              (Id.to_string sid)) in Lwt.return (pe, None)
      | Some session -> 
        let rid = Message.PublisherDecl.rid pd in 
        let resname = match VleMap.find_opt rid session.rmap with 
          | Some name -> name
          | None -> ID(rid) in
        let (pe, res) = update_resource_mapping pe resname session rid 
            (fun m -> match m with 
               | Some m -> {m with pub=true;} 
               | None -> {id = rid; session = sid; pub = true; sub = None; matched_pub = false; matched_sub=false}) in
        Lwt.return (pe, Some res)

    let process_pdecl pe tsex pd =      
      let%lwt (pe, pr) = register_publication pe tsex pd in
      match pr with 
      | None -> Lwt.return (pe, [])
      | Some pr -> 
        let id = Message.PublisherDecl.rid pd in
        match_pdecl pe pr id tsex

    (* ======================== SUB DECL =========================== *)

    let forward_sdecl_to_session pe res zsex =       
      let module M = Message in
      let open Resource in 
      let oc = Session.out_channel zsex in
      let (pe, ds) = match res.name with 
        | ID id -> (
            let subdecl = M.Declaration.SubscriberDecl M.(SubscriberDecl.create id SubscriptionMode.push_mode []) in
            (pe, [subdecl]))
        | URI uri -> 
          let resdecl = M.Declaration.ResourceDecl (M.ResourceDecl.create res.local_id uri []) in
          let subdecl = M.Declaration.SubscriberDecl (M.SubscriberDecl.create res.local_id M.SubscriptionMode.push_mode []) in
          let (pe, _) = update_resource_mapping pe res.name zsex res.local_id 
              (fun m -> match m with 
                 | Some mapping -> mapping
                 | None -> {id = res.local_id; session = (TxSession.id zsex.tx_sex); pub = false; sub = None; matched_pub = false; matched_sub=false}) in 
          (pe, [resdecl; subdecl]) in
      let decl = M.Declare (M.Declare.create (true, true) (OutChannel.next_rsn oc) ds) in
      (* TODO: This is going to throw an exception if the channel is out of places... need to handle that! *)
      let open Lwt.Infix in 
      (pe, Mcodec.ztcp_write_frame_pooled (TxSession.socket @@ Session.tx_sex zsex) (Frame.Frame.create [decl]) pe.buffer_pool>|= fun _ -> ())

    let forget_sdecl_to_session pe res zsex =       
      let module M = Message in
      let open Resource in 
      let oc = Session.out_channel zsex in
      let fsubdecl = match res.name with 
        | ID id -> M.Declaration.ForgetSubscriberDecl M.(ForgetSubscriberDecl.create id)
        | URI _ -> M.Declaration.ForgetSubscriberDecl M.(ForgetSubscriberDecl.create res.local_id) in
      let decl = M.Declare (M.Declare.create (true, true) (OutChannel.next_rsn oc) [fsubdecl]) in
      (* TODO: This is going to throw an exception if the channel is out of places... need to handle that! *)
      let open Lwt.Infix in 
      (pe, Mcodec.ztcp_write_frame_pooled (TxSession.socket @@ Session.tx_sex zsex) (Frame.Frame.create [decl]) pe.buffer_pool>|= fun _ -> ())


    let forward_sdecl pe res router =  
      let open ZRouter in
      let open Resource in 
      let subs = List.filter (fun map -> map.sub != None) res.mappings in 
      let (pe, ps) = (match subs with 
        | [] -> 
          Lwt.ignore_result @@ Logs_lwt.debug (fun m -> m "Resource %s : no subs" (ResName.to_string res.name));
          SIDMap.fold (fun _ session (pe, ps) -> 
          let (pe, p) = forget_sdecl_to_session pe res session in 
          (pe, p::ps)) pe.smap (pe, [])
        | sub :: [] -> 
          (match SIDMap.find_opt sub.session pe.smap with 
          | None -> (pe, [])
          | Some subsex ->
          let tsex = Session.tx_sex subsex in
          Lwt.ignore_result @@ Logs_lwt.debug (fun m -> 
            let nid = match List.find_opt (fun (peer:ZRouter.peer) -> 
              TxSession.id peer.tsex = subsex.sid) pe.router.peers with 
            | Some peer -> peer.pid
            | None -> "UNKNOWN" in
            m "Resource %s : 1 sub (%s) (%s)" (ResName.to_string res.name) (Id.show sub.session) nid);
          let module TreeSet = (val router.tree_mod : Spn_tree.Set.S) in
          let tree0 = Option.get (TreeSet.get_tree router.tree_set 0) in
          let (pe, ps) = (match TreeSet.get_parent tree0 with 
            | None -> TreeSet.get_childs tree0 
            | Some parent -> parent :: TreeSet.get_childs tree0 )
            |> List.map (fun (node:Spn_tree.Node.t) -> 
                (List.find_opt (fun peer -> peer.pid = node.node_id) router.peers))
            |> List.filter (fun opt -> opt != None)
            |> List.map (fun peer -> (Option.get peer).tsex)
            |> List.fold_left (fun x stsex -> 
                if(tsex != stsex)
                then 
                  begin
                    let (pe, ps) = x in
                    let s = Option.get @@ SIDMap.find_opt (TxSession.id stsex) pe.smap in
                    let (pe, p) = forward_sdecl_to_session pe res s in 
                    (pe, p :: ps)
                  end
                else x
              ) (pe, []) in 
          let (pe, ps) = match Vle.logand (subsex.mask) (Vle.of_char Message.ScoutFlags.scoutBroker) <> 0L with
          | false -> (pe, ps)
          | true -> let (pe, p) = forget_sdecl_to_session pe res subsex in (pe, p::ps) in 
          TreeSet.get_broken_links tree0 
            |> List.map (fun (node:Spn_tree.Node.t) -> 
                (List.find_opt (fun peer -> peer.pid = node.node_id) router.peers))
            |> List.filter (fun opt -> opt != None)
            |> List.map (fun peer -> (Option.get peer).tsex)
            |> List.fold_left (fun x stsex -> 
                if(tsex != stsex)
                then 
                  begin
                    let (pe, ps) = x in
                    let s = Option.get @@ SIDMap.find_opt (TxSession.id stsex) pe.smap in
                    let (pe, p) = forget_sdecl_to_session pe res s in 
                    (pe, p :: ps)
                  end
                else x
              ) (pe, ps))
        | _ -> 
          Lwt.ignore_result @@ Logs_lwt.debug (fun m -> m "Resource %s : 2+ subs" (ResName.to_string res.name));
          let module TreeSet = (val router.tree_mod : Spn_tree.Set.S) in
          let tree0 = Option.get (TreeSet.get_tree router.tree_set 0) in
          let (pe, ps) = (match TreeSet.get_parent tree0 with 
            | None -> TreeSet.get_childs tree0 
            | Some parent -> parent :: TreeSet.get_childs tree0 )
            |> List.map (fun (node:Spn_tree.Node.t) -> 
                (List.find_opt (fun peer -> peer.pid = node.node_id) router.peers))
            |> List.filter (fun opt -> opt != None)
            |> List.map (fun peer -> (Option.get peer).tsex)
            |> List.fold_left (fun x stsex -> 
                    let (pe, ps) = x in
                    let s = Option.get @@ SIDMap.find_opt (TxSession.id stsex) pe.smap in
                    let (pe, p) = forward_sdecl_to_session pe res s in 
                    (pe, p :: ps)
              ) (pe, []) in 
          TreeSet.get_broken_links tree0 
            |> List.map (fun (node:Spn_tree.Node.t) -> 
                (List.find_opt (fun peer -> peer.pid = node.node_id) router.peers))
            |> List.filter (fun opt -> opt != None)
            |> List.map (fun peer -> (Option.get peer).tsex)
            |> List.fold_left (fun x stsex -> 
                    let (pe, ps) = x in
                    let s = Option.get @@ SIDMap.find_opt (TxSession.id stsex) pe.smap in
                    let (pe, p) = forget_sdecl_to_session pe res s in 
                    (pe, p :: ps)
              ) (pe, ps))
      in 
      Lwt.Infix.(
      Lwt.catch(fun () -> Lwt.join ps >>= fun () -> Lwt.return pe)
               (fun ex -> Logs_lwt.debug (fun m -> m "Ex %s" (Printexc.to_string ex)) >>= fun () -> Lwt.return pe))
    
    let register_subscription pe tsex sd =
      let sid = TxSession.id tsex in 
      let session = SIDMap.find_opt sid pe.smap in 
      match session with 
      | None -> let%lwt _ = Logs_lwt.warn (fun m -> m "Received SubscriberDecl on unknown session %s: Ignore it!" 
                                              (Id.to_string sid)) in Lwt.return (pe, None)
      | Some session -> 
        let rid = Message.SubscriberDecl.rid sd in 
        let pull = match Message.SubscriberDecl.mode sd with 
          | Message.SubscriptionMode.PullMode -> true
          | Message.SubscriptionMode.PushMode -> false 
          | Message.SubscriptionMode.PeriodicPullMode _ -> true
          | Message.SubscriptionMode.PeriodicPushMode _ -> false in
        let resname = match VleMap.find_opt rid session.rmap with 
          | Some name -> name
          | None -> ID(rid) in
        let (pe, res) = update_resource_mapping pe resname session rid 
            (fun m -> 
               match m with 
               | Some m -> {m with sub=Some pull;} 
               | None -> {id = rid; session = sid; pub = false; sub = Some pull; matched_pub = false; matched_sub=false}) in
        Lwt.return (pe, Some res)

    let unregister_subscription pe tsex fsd =
      let sid = TxSession.id tsex in 
      let session = SIDMap.find_opt sid pe.smap in 
      match session with 
      | None -> let%lwt _ = Logs_lwt.warn (fun m -> m "Received ForgetSubscriberDecl on unknown session %s: Ignore it!" 
                                              (Id.to_string sid)) in Lwt.return (pe, None)
      | Some session -> 
        let rid = Message.ForgetSubscriberDecl.id fsd in 
        let resname = match VleMap.find_opt rid session.rmap with 
          | Some name -> name
          | None -> ID(rid) in
        let (pe, res) = update_resource_mapping pe resname session rid 
            (fun m -> 
               match m with 
               | Some m -> {m with sub=None;} 
               | None -> {id = rid; session = sid; pub = false; sub = None; matched_pub = false; matched_sub=false}) in
              (* TODO do not create a mapping in this case *)
        Lwt.return (pe, Some res)
    
    let sub_state pe tsex rid = 
      let sid = TxSession.id tsex in 
      let session = SIDMap.find_opt sid pe.smap in 
      match session with 
      | None -> None
      | Some session -> 
        let resname = match VleMap.find_opt rid session.rmap with 
          | Some name -> name
          | None -> ID(rid) in
        let optres = ResMap.find_opt resname pe.rmap in 
        match optres with 
        | None -> None
        | Some res ->
          let open Resource in
          let mapping = List.find_opt (fun m -> m.session = sid) res.mappings in 
          match mapping with 
          | None -> None
          | Some mapping -> mapping.sub

    let notify_pub_matching_res pe tsex res =      
      let sid = TxSession.id tsex in 
      let%lwt _ = Logs_lwt.debug (fun m -> m "Notifing Pub Matching Subs") in 
      let (pe, ps) = List.fold_left (fun x name -> 
          match ResMap.find_opt name pe.rmap with 
          | None -> x
          | Some mres -> 
            let (pe, ps) = x in 
            List.fold_left (fun x m -> 
                let open Resource in                 
                match (m.pub && not m.matched_pub && m.session != sid) with 
                | false -> x
                | true -> let (pe, ps) = x in
                  let m = {m with matched_pub = true} in
                  let pres = Resource.with_mapping mres m in
                  let rmap = ResMap.add pres.name pres pe.rmap in
                  let pe = {pe with rmap} in

                  let session = SIDMap.find m.session pe.smap in
                  let oc = Session.out_channel session in
                  let ds = [Message.Declaration.SubscriberDecl (Message.SubscriberDecl.create m.id Message.SubscriptionMode.push_mode [])] in
                  let decl = Message.Declare (Message.Declare.create (true, true) (OutChannel.next_rsn oc) ds) in
                  Lwt.ignore_result @@ Logs_lwt.debug(fun m ->  m "Sending SubscriberDecl to session %s" (Id.to_string session.sid));
                  (* TODO: This is going to throw an exception if the channel is out of places... need to handle that! *)                                    
                  let r = Mcodec.ztcp_write_frame_pooled (TxSession.socket @@ Session.tx_sex session) (Frame.Frame.create [decl]) pe.buffer_pool in 
                  let open Lwt.Infix in 
                  (pe, (r >>= fun _ -> Lwt.return_unit) :: ps)
              ) (pe, ps) mres.mappings
        ) (pe, []) Resource.(res.matches) in
      let%lwt _ = Lwt.join ps in
      Lwt.return pe

    let process_sdecl pe tsex sd = 
      if sub_state pe tsex (Message.SubscriberDecl.rid sd) = None 
      then 
        begin
          let%lwt (pe, res) = register_subscription pe tsex sd in
          match res with 
          | None -> Lwt.return (pe, [])
          | Some res -> 
            let%lwt pe = forward_sdecl pe res pe.router in
            let%lwt _ = notify_pub_matching_res pe tsex res in
            Lwt.return (pe, [])
        end
      else Lwt.return (pe, [])

    let process_fsdecl pe tsex fsd = 
      if sub_state pe tsex (Message.ForgetSubscriberDecl.id fsd) != None 
      then 
        begin
          let%lwt (pe, res) = unregister_subscription pe tsex fsd in
          match res with 
          | None -> Lwt.return (pe, [])
          | Some res -> 
            let%lwt pe = forward_sdecl pe res pe.router in
            Lwt.return (pe, [])
        end
      else Lwt.return (pe, [])

    (* ======================== ======== =========================== *)

    let on_tree_change pe = 
      let _ = ResMap.for_all (fun _ res -> Lwt.ignore_result @@ forward_sdecl pe res pe.router; true) pe.rmap in ()

    let remove_session pe tsex peer =    
      let sid = TxSession.id tsex in 
      let%lwt _ = Logs_lwt.debug (fun m -> m  "Un-registering Session %s \n" (Id.to_string sid)) in
      let smap = SIDMap.remove sid pe.smap in
      let rmap = ResMap.map (fun r -> Resource.remove_mapping r sid) pe.rmap in 
      
      let optpeer = List.find_opt (fun (x:ZRouter.peer) -> TxSession.id x.tsex = TxSession.id tsex) pe.router.peers in
      let router = match optpeer with
      | Some peer ->
        Lwt.ignore_result @@ Logs_lwt.debug (fun m -> m  "Delete node \n");
        ZRouter.delete_node pe.router peer.pid
      | None ->
        Lwt.ignore_result @@ Logs_lwt.debug (fun m -> m  "Cannot find tree  node for session %s \n" (Id.to_string sid));
        pe.router in
      let%lwt _ = Logs_lwt.debug (fun m -> m "Spanning trees status :\n%s" (ZRouter.report pe.router)) in
      let pe = {pe with router} in
      on_tree_change pe;
      Lwt.ignore_result @@ Lwt.catch
        (fun _ -> match Locator.of_string peer with 
          | Some loc -> if List.exists (fun l -> l = loc) pe.peers 
                        then connect_peer loc pe.tx_connector 1000
                        else Lwt.return 0
          | None -> Lwt.return 0)
        (fun ex -> let%lwt _ = Logs_lwt.warn (fun m -> m "%s" (Printexc.to_string ex)) in Lwt.return 0);

      Lwt.return {pe with rmap; smap; router}


    let guarded_remove_session engine tsex peer =
      let%lwt _ = Logs_lwt.debug (fun m -> m "Cleaning up session %s (%s) because of a connection drop" (Id.show  @@ TxSession.id tsex) peer) in 
      MVar.guarded engine 
      @@ fun pe -> 
      let%lwt pe = remove_session pe tsex peer in
      MVar.return pe pe 

    let add_session engine tsex mask = 
      MVar.guarded engine 
      @@ fun pe ->      
      let sid = TxSession.id tsex in    
      let%lwt _ = Logs_lwt.debug (fun m -> m "Registering Session %s mask:%i\n" (Id.to_string sid) (Vle.to_int mask)) in
      let s = Session.create (tsex:TxSession.t) mask in    
      let smap = SIDMap.add (TxSession.id tsex) s pe.smap in   
      let%lwt peer = 
      Lwt.catch 
        (fun () -> 
          match (Lwt_unix.getpeername (TxSession.socket tsex)) with 
          | Lwt_unix.ADDR_UNIX u -> Lwt.return u 
          | Lwt_unix.ADDR_INET (a, p) -> Lwt.return @@ "tcp/" ^ (Unix.string_of_inet_addr a) ^ ":" ^ (string_of_int p))
        (fun _ -> Lwt.return "UNKNOWN") in
      let _ = Lwt.bind (TxSession.when_closed tsex)  (fun _ -> guarded_remove_session engine tsex peer) in
      let pe' = {pe with smap} in
      MVar.return pe' pe'


    let process_scout engine _ _ = 
      let open Lwt.Infix in
       MVar.read engine >>= fun pe -> Lwt.return [make_hello pe]

    let process_hello engine tsex msg  =
      let sid = TxSession.id tsex in 
      let%lwt pe' = add_session engine tsex (Message.Hello.mask msg) in           
      match Vle.logand (Message.Hello.mask msg) (Vle.of_char Message.ScoutFlags.scoutBroker) <> 0L with 
      | false -> Lwt.return  []
      | true -> (
          let%lwt _ = Logs_lwt.debug (fun m -> m "Try to open ZENOH session with broker on transport session: %s\n" (Id.show sid)) in
          Lwt.return [make_open pe'])

    let process_broker_open engine tsex msg = 
      MVar.guarded engine 
      @@ fun pe ->
      let%lwt _ = Logs_lwt.debug (fun m -> m "Accepting Open from remote broker: %s\n" (pid_to_string @@ Message.Open.pid msg)) in
      let pe' = {pe with router = ZRouter.new_node pe.router {pid = IOBuf.hexdump @@ Message.Open.pid msg; tsex}} in
      on_tree_change pe';
      MVar.return [make_accept pe' (Message.Open.pid msg)] pe'

    let process_open engine tsex msg  =
      let open Lwt.Infix in 
      MVar.read engine >>= fun pe -> 
      match SIDMap.find_opt (TxSession.id tsex) pe.smap with
      | None -> 
        (match List.exists (fun (key, _) -> key = Vle.zero) (Message.Open.properties msg) with 
        | false -> 
          let%lwt _ = Logs_lwt.debug (fun m -> m "Accepting Open from unscouted remote peer: %s\n" (pid_to_string @@ Message.Open.pid msg)) in
          let%lwt pe' = add_session engine tsex Vle.zero in 
          Lwt.return [make_accept pe' (Message.Open.pid msg)] 
        | true -> 
          let%lwt pe' = add_session engine tsex (Vle.of_char Message.ScoutFlags.scoutBroker) in 
          MVar.guarded engine (fun _ -> MVar.return () pe') >>= 
          fun _ -> process_broker_open engine tsex msg)
      | Some session -> match Vle.logand session.mask (Vle.of_char Message.ScoutFlags.scoutBroker) <> 0L with 
        | false -> 
          let%lwt _ = Logs_lwt.debug (fun m -> m "Accepting Open from remote peer: %s\n" (pid_to_string @@ Message.Open.pid msg)) in
          Lwt.return ([make_accept pe (Message.Open.pid msg)])     
        | true -> process_broker_open engine tsex msg

    let process_accept_broker engine tsex msg = 
      MVar.guarded engine
      @@ fun pe ->
      let%lwt _ = Logs_lwt.debug (fun m -> m "Accepted from remote broker: %s\n" (pid_to_string @@ Message.Accept.apid msg)) in
      let pe' = {pe with router = ZRouter.new_node pe.router {pid = IOBuf.hexdump @@ Message.Accept.apid msg; tsex}} in
      on_tree_change pe';
      MVar.return [] pe'

    let process_accept engine tsex msg =
      let open Lwt.Infix in
      MVar.read engine >>= fun pe -> 
      let sid = TxSession.id tsex in 
      match SIDMap.find_opt sid pe.smap with
      | None -> 
        let%lwt _ = Logs_lwt.debug (fun m -> m "Accepted from unscouted remote peer: %s\n" (pid_to_string @@ Message.Accept.apid msg)) in
        let%lwt _ = add_session engine tsex Vle.zero in  Lwt.return [] 
      | Some session -> match Vle.logand session.mask (Vle.of_char Message.ScoutFlags.scoutBroker) <> 0L with 
        | false -> (
            let%lwt _ = Logs_lwt.debug (fun m -> m "Accepted from remote peer: %s\n" (pid_to_string @@ Message.Accept.apid msg)) in
            Lwt.return [])      
        | true -> process_accept_broker engine tsex msg 


    let make_result pe _ cd =
      let%lwt _ =  Logs_lwt.debug (fun m -> m  "Crafting Declaration Result") in
      Lwt.return (pe, [Message.Declaration.ResultDecl (Message.ResultDecl.create (Message.CommitDecl.commit_id cd) (char_of_int 0) None)])


    let process_declaration pe tsex d =
      let open Message.Declaration in
      match d with
      | ResourceDecl rd ->
        let%lwt _ = Logs_lwt.debug (fun m -> m "RDecl for resource: %Ld %s"  (Message.ResourceDecl.rid rd) (Message.ResourceDecl.resource rd) ) in
        let%lwt pe = declare_resource pe tsex rd in
        Lwt.return (pe, [])
      | PublisherDecl pd ->
        let%lwt _ = Logs_lwt.debug (fun m -> m "PDecl for resource: %Ld" (Message.PublisherDecl.rid pd)) in
        process_pdecl pe tsex pd
      | SubscriberDecl sd ->
        let%lwt _ = Logs_lwt.debug (fun m -> m "SDecl for resource: %Ld"  (Message.SubscriberDecl.rid sd)) in
        process_sdecl pe tsex sd
      | ForgetSubscriberDecl fsd ->
        let%lwt _ = Logs_lwt.debug (fun m -> m "FSDecl for resource: %Ld"  (Message.ForgetSubscriberDecl.id fsd)) in
        process_fsdecl pe tsex fsd
      | CommitDecl cd -> 
        let%lwt _ = Logs_lwt.debug (fun m -> m "Commit SDecl ") in
        make_result pe tsex cd
      | _ ->
        let%lwt _ = Logs_lwt.debug (fun m -> m "Unknown / Unhandled Declaration...."  ) in       
        Lwt.return (pe, [])

    let process_declarations engine tsex ds =  
      let open Message.Declaration in
      (* Must process ResourceDecls first *)
      MVar.guarded engine 
      @@ fun pe -> 
      let%lwt (pe, ms) = List.sort (fun x y -> match (x, y) with 
          | (ResourceDecl _, ResourceDecl _) -> 0
          | (ResourceDecl _, _) -> -1
          | (_, ResourceDecl _) -> 1
          | (_, _) -> 0) ds
                         |> List.fold_left (fun x d -> 
                             let%lwt (pe, ds) = x in
                             let%lwt (pe, decl) = process_declaration pe tsex d in 
                             Lwt.return (pe, decl @ ds)) (Lwt.return (pe, [])) 
      in MVar.return ms pe

    let process_declare engine tsex msg =         
      let%lwt pe = MVar.read engine in
      let%lwt _ = Logs_lwt.debug (fun m -> m "Processing Declare Message\n") in    
      let sid = TxSession.id tsex in 
      match SIDMap.find_opt sid pe.smap with 
      | Some s ->
        let ic = Session.in_channel s in
        let oc = Session.out_channel s in
        let sn = (Message.Declare.sn msg) in
        let csn = InChannel.rsn ic in
        if sn >= csn then
          begin
            InChannel.update_rsn ic sn  ;
            let%lwt ds = process_declarations engine tsex (Message.Declare.declarations msg) in
            match ds with 
            | [] ->
              let%lwt _ = Logs_lwt.debug (fun m -> m  "Acking Declare with sn: %Ld" sn) in 
              Lwt.return [Message.AckNack (Message.AckNack.create (Vle.add sn 1L) None)]
            | _ as ds ->
              let%lwt _ = Logs_lwt.debug (fun m -> m "Sending Matching decalrations and ACKNACK \n") in
              Lwt.return [Message.Declare (Message.Declare.create (true, true) (OutChannel.next_rsn oc) ds);
                          Message.AckNack (Message.AckNack.create (Vle.add sn 1L) None)]
          end
        else
          begin
            let%lwt _ = Logs_lwt.debug (fun m -> m "Received out of oder message") in
            Lwt.return  []
          end
      | None -> Lwt.return [] 


    let process_synch _ _ msg =
      let asn = Message.Synch.sn msg in
      Lwt.return [Message.with_markers (Message.AckNack (Message.AckNack.create asn None)) (Message.markers (Message.Synch msg))]

    let process_ack_nack _ _ _ = Lwt.return []

    let forward_data_to_mapping pe srcresname dstres dstmapsession dstmapid reliable payload =
      let open Resource in 
      let open ResName in 
      match SIDMap.find_opt dstmapsession pe.smap with
      | None -> Lwt.return_unit
      | Some s ->
        let%lwt _ = Logs_lwt.debug (fun m -> m  "Forwarding data to session %s" (Id.show s.sid)) in
        let oc = Session.out_channel s in
        let fsn = if reliable then OutChannel.next_rsn oc else  OutChannel.next_usn oc in
        let msg = match srcresname with 
          | ID id -> Message.StreamData(Message.StreamData.create (true, reliable) fsn id None payload)
          | URI uri -> match srcresname = dstres.name with 
            | true -> Message.StreamData(Message.StreamData.create (true, reliable) fsn dstmapid None payload)
            | false -> WriteData(Message.WriteData.create (true, reliable) fsn uri payload) 
        in

        let sock = TxSession.socket s.tx_sex in 
        let open Lwt.Infix in 
        (Mcodec.ztcp_write_frame_pooled sock @@ Frame.Frame.create [msg]) pe.buffer_pool >>= fun _ -> Lwt.return_unit


    let rspace msg =       
      let open Message in 
      List.fold_left (fun res marker -> 
          match marker with 
          | RSpace rs -> RSpace.id rs 
          | _ -> res) 0L (markers msg)

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

    let forward_data pe sid srcres reliable payload = 
      let open Resource in
      let (_, ps) = List.fold_left (fun (sss, pss) name -> 
          match ResMap.find_opt name pe.rmap with 
          | None -> (sss, pss)
          | Some r -> 
            List.fold_left (fun (ss, ps) m ->
                match m.sub = Some false && m.session != sid && not @@ List.exists (fun s -> m.session == s) ss with
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
                match m.sub = Some false && m.session != sid && not @@ List.exists (fun s -> m.session == s) ss with
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
                                                    (ResName.to_string name) (Id.show session.sid)) in Lwt.return (pe, [])
      | (pe, Some res) -> 
        let%lwt _ = Logs_lwt.debug (fun m -> 
                                    let nid = match List.find_opt (fun (peer:ZRouter.peer) -> 
                                        TxSession.id peer.tsex = session.sid) pe.router.peers with 
                                    | Some peer -> peer.pid
                                    | None -> "UNKNOWN" in
                                    m "Handling StreamData Message. nid[%s] sid[%s] rid[%Ld] res[%s]"
                                     nid (Id.show session.sid) rid (match res.name with URI u -> u | ID _ -> "UNNAMED")) in
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
                                    nid (Id.show session.sid) (Message.WriteData.resource msg)) in
      let name = ResName.URI(Message.WriteData.resource msg) in
      match store_data pe name (Message.WriteData.payload msg) with 
      | (pe, None) -> 
        let%lwt _ = forward_oneshot_data pe session.sid name (Message.Reliable.reliable msg) (Message.WriteData.payload msg) in
        Lwt.return (pe, [])
      | (pe, Some res) -> 
        let%lwt _ = forward_data pe session.sid res (Message.Reliable.reliable msg) (Message.WriteData.payload msg) in
        Lwt.return (pe, [])

    let process_broker_data pe session msg = 
      let open Session in      
      let%lwt _ = Logs_lwt.debug (fun m -> m "Received tree state on %s\n" (Id.show session.sid)) in
      let b = Lwt_bytes.to_bytes @@ IOBuf.to_bytes @@ Message.StreamData.payload msg in
      let node = Marshal.from_bytes b 0 in
      let pe = {pe with router = ZRouter.update pe.router node} in
      let%lwt _ = Logs_lwt.debug (fun m -> m "Spanning trees status :\n%s" (ZRouter.report pe.router)) in
      on_tree_change pe;
      Lwt.return (pe, []) 

    let process_stream_data engine tsex msg =
      MVar.guarded engine
      @@ fun pe ->
      let%lwt (pe, ms) = 
        let sid = TxSession.id tsex in 
        let session = SIDMap.find_opt sid pe.smap in 
        match session with 
        | None -> let%lwt _ = Logs_lwt.warn (fun m -> m "Received StreamData on unknown session %s: Ignore it!" 
                                                (Id.show sid)) in Lwt.return (pe, [])
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
                                                (Id.show sid)) in Lwt.return (pe, [])
        | Some session -> 
          match rspace (Message.WriteData(msg)) with 
          | 1L -> Lwt.return (pe, []) 
          | _ -> process_user_writedata pe session msg
      in MVar.return ms pe

    let process_pull engine tsex msg =
      let open Lwt.Infix in 
      MVar.read engine >>= fun pe -> 
      let sid = TxSession.id tsex in 
      let session = SIDMap.find_opt sid pe.smap in 
      match session with 
      | None -> let%lwt _ = Logs_lwt.warn (fun m -> m "Received Pull on unknown session %s: Ignore it!" 
                                              (Id.show sid)) in Lwt.return []
      | Some session -> 
        let rid = Message.Pull.id msg in
        let name = match VleMap.find_opt rid session.rmap with 
          | None -> ResName.ID(rid)
          | Some name -> name in 
        match ResMap.find_opt name pe.rmap with 
        | None -> let%lwt _ = Logs_lwt.warn (fun m -> m "Received Pull for unknown resource %s on session %s: Ignore it!" 
                                                (ResName.to_string name) (Id.show session.sid)) in Lwt.return []
        | Some res -> 
          let%lwt _ = Logs_lwt.debug (fun m -> m "Handling Pull Message for resource: [%s:%Ld] (%s)" 
                                         (Id.show session.sid) rid (match res.name with URI u -> u | ID _ -> "UNNAMED")) in
          let%lwt _ = Lwt_list.iter_p (fun mresname -> 
              let mres = ResMap.find mresname pe.rmap in
              match mres.last_value with 
              | None -> Lwt.return_unit
              | Some v -> forward_data_to_mapping pe mres.name res sid rid true v
            ) res.matches in Lwt.return []

    let process_close (engine:t) _ = 
      let open Lwt.Infix in 
      MVar.read engine 
      >>= fun pe -> Lwt.return [Message.Close (Message.Close.create pe.pid '0')]

    let handle_message engine (tsex : TxSession.t) (msgs: Message.t list)  = 
      let open Lwt.Infix in
      let%lwt _ = Logs_lwt.debug (fun m -> m  "Received Frame") in      
      let dispatch = function
        | Message.Scout msg -> process_scout engine tsex msg 
        | Message.Hello msg -> process_hello engine tsex msg 
        | Message.Open msg -> process_open engine tsex msg 
        | Message.Accept msg -> process_accept engine tsex msg
        | Message.Close _ -> process_close engine tsex 
        | Message.Declare msg -> process_declare engine tsex msg
        | Message.Synch msg -> process_synch engine tsex msg
        | Message.AckNack msg -> process_ack_nack engine tsex msg
        | Message.StreamData msg -> process_stream_data engine tsex msg
        | Message.WriteData msg -> process_write_data engine tsex msg
        | Message.Pull msg -> process_pull engine tsex msg
        | Message.KeepAlive _ -> Lwt.return []
        | _ -> Lwt.return []
      in Lwt_list.map_p dispatch msgs >|= List.flatten 

  end
end