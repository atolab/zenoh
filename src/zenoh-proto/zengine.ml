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

  let rec hash = function 
    | "" -> 0
    | s -> (31 * (hash (String.sub s 1 ((String.length s) - 1))) + int_of_char s.[0]) mod 4294967295

  let hostid = Printf.sprintf "%08X" @@ hash @@ Unix.gethostname ()

  module Config = struct
    type nid_t = string
    type prio_t = int
    type dist_t = int

    let local_id = hostid ^ Printf.sprintf "%08d" (Unix.getpid ())
    let local_prio = Unix.getpid ()
    let max_dist = 2
    let max_trees = 1
  end

  module Router = ZRouter.Make(Config)

  module ProtocolEngine = struct

    type engine_state = {
      pid : IOBuf.t;
      lease : Vle.t;
      locators : Locators.t;
      smap : Session.t SIDMap.t;
      rmap : Resource.t ResMap.t;      
      peers : Locator.t list;
      router : Router.t;
      next_mapping : Vle.t;
    }

    type t = engine_state MVar.t

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
          Lwt.ignore_result @@ Mcodec.ztcp_write_frame_alloc (TxSession.socket Router.(peer.tsex)) (Frame.create [sdata]) ) _nodes

    let send_nodes peers nodes = List.iter (fun peer -> send_nodes peer nodes) peers

    let create (pid : IOBuf.t) (lease : Vle.t) (ls : Locators.t) (peers : Locator.t list) = 
      MVar.create @@ { 
        pid; 
        lease; 
        locators = ls; 
        smap = SIDMap.empty; 
        rmap = ResMap.empty; 
        peers;
        router = Router.create send_nodes;
        next_mapping = 0L; }


    let remove_session pe tsex =    
      let sid = TxSession.id tsex in 
      let%lwt _ = Logs_lwt.debug (fun m -> m  "Un-registering Session %s \n" (Id.to_string sid)) in
      let smap = SIDMap.remove sid pe.smap in
      let rmap = ResMap.map (fun r -> Resource.remove_mapping r sid) pe.rmap in 
      Lwt.return {pe with rmap; smap}


    let guarded_remove_session engine tsex =
      let%lwt _ = Logs_lwt.debug (fun m -> m "Cleaning up session because of a connection drop %s" (Id.show  @@ TxSession.id tsex)) in 
      MVar.guarded engine 
      @@ fun pe -> 
      let%lwt pe = remove_session pe tsex in
      let%lwt _ = Logs_lwt.debug (fun m -> m "Cleaned up session because of a connection drop %s" (Id.show  @@ TxSession.id tsex)) in 
      MVar.return pe pe 

    let add_session engine tsex mask = 
      MVar.guarded engine 
      @@ fun pe ->      
      let sid = TxSession.id tsex in    
      let%lwt _ = Logs_lwt.debug (fun m -> m "Registering Session %s: \n" (Id.to_string sid)) in
      let s = Session.create (tsex:TxSession.t) mask in    
      let smap = SIDMap.add (TxSession.id tsex) s pe.smap in         
      let _ = Lwt.bind (TxSession.when_closed tsex)  (fun _ -> guarded_remove_session engine tsex) in
      let pe' = {pe with smap} in
      MVar.return pe' pe'

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

    let pid_to_string pid = fst @@ Result.get (IOBuf.get_string (IOBuf.available pid) pid)

    let make_scout = Message.Scout (Message.Scout.create (Vle.of_char Message.ScoutFlags.scoutBroker) [])

    let make_hello pe = Message.Hello (Message.Hello.create (Vle.of_char Message.ScoutFlags.scoutBroker) pe.locators [])

    let make_open pe = Message.Open (Message.Open.create (char_of_int 0) pe.pid 0L pe.locators [])

    let make_accept pe opid = Message.Accept (Message.Accept.create opid pe.pid pe.lease [])


    let process_scout engine tsex msg =
      let open Lwt.Infix in
      add_session engine tsex (Message.Scout.mask msg) 
      >>= fun pe' -> Lwt.return [make_hello pe']


    let process_hello engine tsex msg  =
      let open Lwt.Infix in 
      let sid = TxSession.id tsex in       
      let%lwt pe' = add_session engine tsex (Message.Hello.mask msg) in 
      let _ = TxSession.when_closed tsex >>= fun _ -> guarded_remove_session engine tsex in            
      match Vle.logand (Message.Hello.mask msg) (Vle.of_char Message.ScoutFlags.scoutBroker) <> 0L with 
      | false -> Lwt.return  []
      | true -> (
          let%lwt _ = Logs_lwt.debug (fun m -> m "Try to open ZENOH session with broker on transport session: %s\n" (Id.show sid)) in
          Lwt.return [make_open pe'])

    let process_broker_open engine tsex msg = 
      MVar.guarded engine 
      @@ fun pe ->
      let%lwt _ = Logs_lwt.debug (fun m -> m "Accepting Open from remote broker: %s\n" (pid_to_string @@ Message.Open.pid msg)) in
      let pe' = {pe with router = Router.new_node pe.router {pid = pid_to_string @@ Message.Open.pid msg; tsex}} in
      MVar.return [make_accept pe' (Message.Open.pid msg)] pe'

    let process_open engine tsex msg  =
      let open Lwt.Infix in 
      MVar.read engine >>= fun pe -> 
      match SIDMap.find_opt (TxSession.id tsex) pe.smap with
      | None -> 
        let%lwt _ = Logs_lwt.debug (fun m -> m "Accepting Open from unscouted remote peer: %s\n" (pid_to_string @@ Message.Open.pid msg)) in
        let%lwt pe' = add_session engine tsex Vle.zero in 
        Lwt.return [make_accept pe' (Message.Open.pid msg)] 
      | Some session -> match Vle.logand session.mask (Vle.of_char Message.ScoutFlags.scoutBroker) <> 0L with 
        | false -> 
          let%lwt _ = Logs_lwt.debug (fun m -> m "Accepting Open from remote peer: %s\n" (pid_to_string @@ Message.Open.pid msg)) in
          Lwt.return ([make_accept pe (Message.Open.pid msg)])     
        | true -> process_broker_open engine tsex msg

    let process_accept_broker engine tsex msg = 
      MVar.guarded engine
      @@ fun pe ->
      let%lwt _ = Logs_lwt.debug (fun m -> m "Accepted from remote broker: %s\n" (pid_to_string @@ Message.Accept.apid msg)) in
      let pe' = {pe with router = Router.new_node pe.router {pid = pid_to_string @@ Message.Accept.apid msg; tsex}} in
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


    (* ======================== PUB DECL =========================== *)

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

    (* TODO: Add-back router management  *)

    let forward_pdecl_to_session pe res zsex = 
      let open ResName in 
      let open Resource in       
      let oc = Session.out_channel zsex in
      let (pe, ds) = match res.name with 
        | ID id -> (
            let pubdecl = Message.Declaration.PublisherDecl (Message.PublisherDecl.create id []) in
            (pe, [pubdecl]))
        | URI uri -> 
          let resdecl = Message.Declaration.ResourceDecl (Message.ResourceDecl.create res.local_id uri []) in
          let pubdecl = Message.Declaration.PublisherDecl (Message.PublisherDecl.create res.local_id []) in
          let (pe, _) = update_resource_mapping pe res.name zsex res.local_id 
              (fun m -> match m with 
                 | Some mapping -> mapping
                 | None -> {id = res.local_id; session = zsex.sid; pub = false; sub = None; matched_pub = false; matched_sub=false}) in 
          (pe, [resdecl; pubdecl]) in
      let decl = Message.Declare (Message.Declare.create (true, true) (OutChannel.next_rsn oc) ds) in
      (* TODO: This is going to throw an exception if the channel is out of places... need to handle that! *)  
      let open Lwt.Infix in      
      (pe, Mcodec.ztcp_write_frame_alloc (TxSession.socket @@ Session.tx_sex zsex) (Frame.Frame.create [decl]) >|= fun _ -> ())

    (* TODO: Add-back router management  *)

    let forward_pdecl_to_parents pe res = 
      let open Router in
      let (pe, ps) = Router.TreeSet.parents pe.router.tree_set
                     |> List.map (fun (node:Router.TreeSet.Tree.Node.t) -> 
                         (List.find (fun x -> x.pid = node.node_id) pe.router.peers).tsex)
                     |> List.fold_left (fun x tsex -> 
                         let (pe, ps) = x in
                         let s = Option.get @@ SIDMap.find_opt (TxSession.id tsex) pe.smap in
                         let (pe, p) = forward_pdecl_to_session pe res s in 
                         (pe, p :: ps)
                       ) (pe, []) in 
      let%lwt _ = Lwt.join ps in
      Lwt.return pe

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

    let process_pdecl pe tsex pd =      
      let%lwt (pe, pr) = register_publication pe tsex pd in
      match pr with 
      | None -> Lwt.return (pe, [])
      | Some pr -> 
        let%lwt pe = forward_pdecl_to_parents pe pr in
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
      (pe, Mcodec.ztcp_write_frame_alloc (TxSession.socket @@ Session.tx_sex zsex) (Frame.Frame.create [decl]) >|= fun _ -> ())


    let forward_sdecl_to_parents pe res =      
      let open Router in
      let (pe, ps) = Router.TreeSet.parents pe.router.tree_set
                     |> List.map (fun (node:Router.TreeSet.Tree.Node.t) -> 
                         (List.find (fun x -> x.pid = node.node_id) pe.router.peers).tsex )
                     |> List.fold_left (fun x tsex -> 
                         let (pe, ps) = x in
                         let s = Option.get @@ SIDMap.find_opt (TxSession.id tsex) pe.smap in
                         let (pe, p) = forward_sdecl_to_session pe res s in 
                         (pe, p :: ps)
                       ) (pe, []) in 
      let%lwt _ = Lwt.join ps in
      Lwt.return pe

    let register_subscription pe tsex sd =
      let sid = TxSession.id tsex in 
      let session = SIDMap.find_opt sid pe.smap in 
      match session with 
      | None -> let%lwt _ = Logs_lwt.warn (fun m -> m "Received SubscriptionDecl on unknown session %s: Ignore it!" 
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
                  let r = Mcodec.ztcp_write_frame_alloc (TxSession.socket @@ Session.tx_sex session) (Frame.Frame.create [decl]) in 
                  let open Lwt.Infix in 
                  (pe, (r >>= fun _ -> Lwt.return_unit) :: ps)
              ) (pe, ps) mres.mappings
        ) (pe, []) Resource.(res.matches) in
      let%lwt _ = Lwt.join ps in
      Lwt.return pe

    let process_sdecl pe tsex sd =      
      let%lwt (pe, res) = register_subscription pe tsex sd in
      match res with 
      | None -> Lwt.return (pe, [])
      | Some res -> 
        let%lwt pe = forward_sdecl_to_parents pe res in
        let%lwt _ = notify_pub_matching_res pe tsex res in
        Lwt.return (pe, [])

    (* ======================== ======== =========================== *)

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
      Lwt.return [Message.AckNack (Message.AckNack.create asn None)]

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
        (Mcodec.ztcp_write_frame_alloc sock @@ Frame.Frame.create [msg]) >>= fun _ -> Lwt.return_unit


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

    let process_user_streamdata pe session msg =      
      let open Session in
      let rid = Message.StreamData.id msg in
      let name = match VleMap.find_opt rid session.rmap with 
        | None -> ResName.ID(rid)
        | Some name -> name in 
      match store_data pe name (Message.StreamData.payload msg) with 
      | (pe, None) -> let%lwt _ = Logs_lwt.warn (fun m -> m "Received StreamData for unknown resource %s on session %s: Ignore it!" 
                                                    (ResName.to_string name) (Id.show session.sid)) in Lwt.return (pe, [])
      | (pe, Some res) -> 
        let%lwt _ = Logs_lwt.debug (fun m -> m "Handling Stream Data Message for resource: [%s:%Ld] (%s)" 
                                       (Id.show session.sid) rid (match res.name with URI u -> u | ID _ -> "UNNAMED")) in
        Lwt.ignore_result @@ forward_data pe session.sid res (Message.Reliable.reliable msg) (Message.StreamData.payload msg);
        Lwt.return (pe, [])

    let process_user_writedata pe session msg =      
      let open Session in 
      let%lwt _ = Logs_lwt.debug (fun m -> m "Handling WriteData Message for resource: (%s)" (Message.WriteData.resource msg)) in
      let name = ResName.URI(Message.WriteData.resource msg) in
      match store_data pe name (Message.WriteData.payload msg) with 
      | (pe, None) -> 
        Lwt.ignore_result @@ forward_oneshot_data pe session.sid name (Message.Reliable.reliable msg) (Message.WriteData.payload msg);
        Lwt.return (pe, [])
      | (pe, Some res) -> 
        Lwt.ignore_result @@ forward_data pe session.sid res (Message.Reliable.reliable msg) (Message.WriteData.payload msg);
        Lwt.return (pe, [])

    let process_broker_data pe session msg = 
      let open Session in      
      let%lwt _ = Logs_lwt.debug (fun m -> m "Received tree state on %s\n" (Id.show session.sid)) in
      let b = Lwt_bytes.to_bytes @@ IOBuf.to_bytes @@ Message.StreamData.payload msg in 
      let node = Marshal.from_bytes b 0 in
      let pe = {pe with router = Router.update pe.router node} in
      Router.print pe.router; 
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


    (* let rec connect_peer peer tx = 
       let module TxTcp = (val tx : Transport.S) in 
       let open Lwt in
       Lwt.catch(fun () ->
          TxTcp.connect peer >>= (fun (sid, push) -> 
              let%lwt _ = Logs_lwt.debug (fun m -> m "Connected to %s (sid = %s)" (Locator.to_string peer) (Id.show sid)) in 
              let%lwt _ = push (Event.SessionMessage (Frame.create [make_scout], sid, None)) in 
              Lwt.return_unit))
        (fun _ -> 
           let%lwt _ = Logs_lwt.debug (fun m -> m "Failed to connect to %s" (Locator.to_string peer)) in 
           let%lwt _ = Lwt_unix.sleep 2.0 in 
           connect_peer peer tx)

       let connect_peers peers tx = 
       let module TxTcp = (val tx : Transport.S) in 
       Lwt_list.iter_p (fun p -> connect_peer p tx) peers

       let start pe tx = 
       let rec loop pe =      
        let open Lwt.Infix in 
        let%lwt _ = Logs_lwt.debug (fun m -> m "Processing Protocol Engine Events") in
        (match%lwt Lwt_stream.get pe.evt_sink with 
         | Some(Event.SessionMessage (f, sid, Some push)) -> 
           let%lwt _ = Logs_lwt.debug (fun m -> m "Processing SessionMessage") in
           let msgs = Frame.to_list f in
           List.fold_left (fun pe msg -> 
               pe >>= (fun pe -> process pe sid msg push)) (Lwt.return pe) msgs
         |Some _ -> 
           let%lwt _ = Logs_lwt.debug (fun m -> m "Processing Some other Event...") in
           Lwt.return pe
         | None -> 
           let%lwt _ = Logs_lwt.debug (fun m -> m "Processing None!!!") in
           Lwt.return pe)  
        >>= loop
       in 
       let%lwt _ = Logs_lwt.debug (fun m -> m "Starting Protocol Engine") in
       Lwt.ignore_result @@ connect_peers pe.peers tx;
       loop pe *)
  end
end