open Apero
open Channel
open NetService
open R_name
open Engine_state


module Make (MVar : MVar) = struct

    let next_mapping pe = 
        let next = pe.next_mapping in
        ({pe with next_mapping = Vle.add next 1L}, next)

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


    let forward_all_sdecl pe = 
        let _ = ResMap.for_all (fun _ res -> Lwt.ignore_result @@ forward_sdecl pe res pe.router; true) pe.rmap in ()


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
end
