open Apero
open Apero_net
open NetService
open R_name
open Engine_state

module Make (MVar : MVar) = struct
 
    module MDiscovery = Discovery.Make(MVar) open MDiscovery

    let pid_to_string = IOBuf.hexdump

    let make_scout = Message.Scout (Message.Scout.create (Vle.of_char Message.ScoutFlags.scoutBroker) [])

    let make_hello pe = Message.Hello (Message.Hello.create (Vle.of_char Message.ScoutFlags.scoutBroker) pe.locators [])

    let make_open pe = 
        let buf = IOBuf.create 16 in 
        let buf = Result.get @@ IOBuf.put_string "broker" buf in
        Message.Open (Message.Open.create (char_of_int 0) pe.pid 0L pe.locators [Zproperty.ZProperty.make Vle.zero buf])

    let make_accept pe opid = Message.Accept (Message.Accept.create opid pe.pid pe.lease [])


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


    let remove_session pe tsex peer =    
        let sid = TxSession.id tsex in 
        let%lwt _ = Logs_lwt.debug (fun m -> m  "Un-registering Session %s \n" (Id.to_string sid)) in
        let smap = SIDMap.remove sid pe.smap in
        let rmap = ResMap.map (fun r -> Resource.remove_mapping r sid) pe.rmap in 
        
        let optpeer = List.find_opt (fun (x:ZRouter.peer) -> TxSession.id x.tsex = TxSession.id tsex) pe.router.peers in
        let%lwt router = match optpeer with
        | Some peer ->
            let%lwt _ = Logs_lwt.debug (fun m -> m "Delete node %s" peer.pid) in
            let%lwt _ = Logs_lwt.debug (fun m -> m "Spanning trees status :\n%s" (ZRouter.report pe.router)) in
            Lwt.return @@ ZRouter.delete_node pe.router peer.pid
        | None -> Lwt.return pe.router in
        let pe = {pe with rmap; smap; router} in
        forward_all_decls pe;
        let%lwt pe = notify_all_pubs pe in
        Lwt.ignore_result @@ Lwt.catch
        (fun _ -> match Locator.of_string peer with 
            | Some loc -> if List.exists (fun l -> l = loc) pe.peers 
                        then connect_peer loc pe.tx_connector 1000
                        else Lwt.return 0
            | None -> Lwt.return 0)
        (fun ex -> let%lwt _ = Logs_lwt.warn (fun m -> m "%s" (Printexc.to_string ex)) in Lwt.return 0);
        Lwt.return pe


    let guarded_remove_session engine tsex peer =
        let%lwt _ = Logs_lwt.debug (fun m -> m "Cleaning up session %s (%s) because of a connection drop" (Id.to_string  @@ TxSession.id tsex) peer) in 
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
            let%lwt _ = Logs_lwt.debug (fun m -> m "Try to open ZENOH session with broker on transport session: %s\n" (Id.to_string sid)) in
            Lwt.return [make_open pe'])

    let process_broker_open engine tsex msg = 
        MVar.guarded engine 
        @@ fun pe ->
        let%lwt _ = Logs_lwt.debug (fun m -> m "Accepting Open from remote broker: %s\n" (pid_to_string @@ Message.Open.pid msg)) in
        let pe' = {pe with router = ZRouter.new_node pe.router {pid = IOBuf.hexdump @@ Message.Open.pid msg; tsex}} in
        forward_all_decls pe;
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
        forward_all_decls pe;
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

    let process_close (engine) _ = 
        let open Lwt.Infix in 
        MVar.read engine 
        >>= fun pe -> Lwt.return [Message.Close (Message.Close.create pe.pid '0')]

end