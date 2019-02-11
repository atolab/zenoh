open Apero
open Apero_net

open Transport
open Frame

module type TcpTransportConfig = sig 
  val name : string
  val id : Transport.Id.t
  val locators :  TcpLocator.t list
  val backlog : int
  val bufsize : int
  val channel_bound : int
end

(** Transport.S
    val info : Info.t
    val start : Event.push -> EventSource.pull Lwt.t
    val stop : unit -> unit Lwt.t
    val info : Info.t  
    val connect : Locator.t -> Session.Id.t Lwt.t
    val session_info : Session.Id.t -> Session.Info.t option
  *)
module TcpTransport = struct

  module Make (C: TcpTransportConfig) = struct         
    type session_context = {       
      sock : Lwt_unix.file_descr; 
      inbuf: IOBuf.t; 
      outbuf: IOBuf.t;
      lenbuf : IOBuf.t;
      info: Transport.Session.Info.t }

    module SessionMap = Map.Make (Transport.Session.Id)
    
    type t = { 
      socks : Lwt_unix.file_descr list;  
      info : Transport.Info.t;
      mutable smap: session_context SessionMap.t;
      mutable push: Transport.Event.push option;
    }
    
    let create_server_socket locator = 
      (** Initialises the sever socket and buffer*)
      let open Lwt_unix in      
      let sock = socket PF_INET SOCK_STREAM 0 in
      let _ = setsockopt sock SO_REUSEADDR true in
      let _ = setsockopt sock TCP_NODELAY true in
      let saddr = IpEndpoint.to_sockaddr @@ TcpLocator.endpoint locator in
      let _ = bind sock saddr in
      let _ = listen sock C.backlog in sock
      

    let info = Transport.Info.create C.name C.id true Transport.Info.Stream None

    let self =       
      { socks = List.map create_server_socket C.locators;          
        info = info;
        smap = SessionMap.empty;
        push = None }
    
    
    let stop () = 
      (** TODO: Complete implementation *)      
      Lwt.return_unit

    let decode_frame_length sock buf =
      let rec extract_length v bc buf =       
        Result.try_get 
          ~on:(IOBuf.reset_with 0 1 buf)
          ~run:(fun buf ->
            match%lwt Net.recv sock buf with
            | 0 -> Lwt.fail @@ Exception (`ClosedSession (`Msg "Peer closed the session unexpectedly"))
            | _ ->
              Result.try_get 
                ~on:(IOBuf.get_char buf)
                ~run:(fun (b, buf) -> 
                  match int_of_char b with
                  | c when c <= 0x7f -> Lwt.return (v lor (c lsl (bc * 7)))
                  | c  -> extract_length  (v lor ((c land 0x7f) lsl bc)) (bc + 1) buf)
                ~fail_with:(fun e -> Lwt.fail @@ Exception e))            
          ~fail_with:(fun e -> Lwt.fail @@ Exception e)        
      in extract_length 0 0 buf 
    
    let close_session sock  = Lwt_unix.close sock

    let make_frame buf = 
      let open Result.Infix in
      let rec parse_messages msr buf =      
        if IOBuf.available buf > 0 then                     
          Mcodec.decode_msg buf
          >>= (fun (m, buf) -> msr 
            >>= (fun ms -> 
              parse_messages (Result.ok (m::ms)) buf
            )
          )          
        else msr
      in 
        let ms = parse_messages (Result.ok []) buf in 
        Frame.create <$> ms        

      
    (* NOTE: We assume that a frame is not bigger than 64K bytes. This ensures that
       we can easily foward a frame received on TCP/IP on UDP -- if we are 
       doing just worm-hole forwarding *)
    let decode_frame sock buf = 
      let%lwt len = decode_frame_length sock buf in
      let buf = IOBuf.clear buf in 
      match IOBuf.set_limit len buf with 
        | Ok buf -> 
          let%lwt len = Net.read sock buf in
          if len > 0 then
            Lwt.return @@ make_frame buf 
          else 
            begin
              let%lwt _ = Logs_lwt.warn (fun m -> m "Peer closed connection") in
              Lwt.return @@ (Result.fail (`ClosedSession (`Msg "Peer Closed Session")))
            end 
        | Error _ -> 
          let%lwt _ = Logs_lwt.warn (fun m -> m "Received frame of %d bytes " len) in
          let%lwt _ = close_session sock  in          
          Lwt.fail @@ Exception (`InvalidFormat  (`Msg "Frame exeeds the 64K limit" ))


    let handle_session (sctx: session_context) push (spush : Transport.Event.push) = 
      let module Sx = Transport.Session in
      let module Inf = Transport.Session.Info in
      let module I = Transport.Session.Id in
      let module E = Transport.Event in
      let sid = Inf.id sctx.info in   
      let socket = sctx.sock in   
      let ssid = I.to_string sid in       
      let rbuf = sctx.inbuf in
    
      let rec serve_session () =                       
        match%lwt decode_frame socket rbuf with
        | Ok frame -> 
          List.iter (fun _ -> let _ = push (E.SessionMessage (frame, sid, Some spush)) in ()) 
          (Frame.to_list frame) |> serve_session 
        | Error e -> 
          let%lwt _ = Logs_lwt.warn (fun m -> m "Received invalid frame. Closing session %s" ssid) in
          let%lwt _ = close_session socket in
          Lwt.return (Exception e)  
      in serve_session ()     

    let string_of_locators ls = 
      match ls with 
      | [l] -> "[" ^ (TcpLocator.to_string l) ^ "]"
      | l::_ -> "[" ^ (TcpLocator.to_string l) ^ (List.fold_left (fun a b  -> a ^ ", " ^ (TcpLocator.to_string b)) "" C.locators) ^ "]"
      | _ -> "[]"

      (* List.fold_left (fun a b  -> a ^ ", " ^ (TcpLocator.to_string b)) "" C.locators  *)

    let process_event sctx evt = 
      let open Transport.Event in 
      match evt with 
      | SessionClose _ -> Lwt.return_unit 
      | SessionMessage (f, _, _) -> 
          let buf = IOBuf.reset sctx.outbuf in
          let lbuf = IOBuf.reset sctx.lenbuf in
          (match Result.fold_m Mcodec.encode_msg (Frame.to_list f) buf with 
          | Ok buf ->            
            let%lwt lbuf = lwt_of_result (encode_vle (Vle.of_int (IOBuf.position buf)) lbuf) in
            let%lwt _ = Net.send_vec_all sctx.sock [IOBuf.flip lbuf; IOBuf.flip buf] in
            Lwt.return_unit
          | Error _ -> 
            let%lwt _ = Logs_lwt.err (fun m -> m "Error while encoding frame -- this is a bug!") in           
            Lwt.return_unit
          )        

      | LocatorMessage (_, _, _) -> Lwt.return_unit
      | Events _ -> Lwt.return_unit 
    
    let rec event_loop sctx stream : unit Lwt.t = 
      let%lwt evt = Lwt_stream.get stream in 
      match evt with 
      | Some e -> let%lwt _ = (process_event sctx e) in  event_loop sctx stream
      | None -> event_loop sctx stream


    let start (push : Transport.Event.push) =
      let%lwt _ = Logs_lwt.debug (fun m -> m "Starting TcpTransport@%s" (string_of_locators C.locators)) in 
      self.push <- Some push;
      let rec acceptor_loop (ssock, locator) = 
        let%lwt (sock, addr) = Lwt_unix.accept ssock in
        let ep = IpEndpoint.of_sockaddr addr in
        let src = Locator.TcpLocator (TcpLocator.make ep) in
        let sid = (Transport.Session.Id.next_id ()) in
        let info = Transport.Session.Info.create sid src locator self.info in
        let inbuf = IOBuf.create C.bufsize in
        let outbuf = IOBuf.create C.bufsize in
        let lenbuf = IOBuf.create 4 in
        let sctx = { sock; inbuf; outbuf; lenbuf; info } in 
        let sm = SessionMap.add sid sctx self.smap in
        self.smap <- sm ;        (* fixme : race condition*)  
        let%lwt _ = Logs_lwt.debug (fun m -> m "Accepted connection with session-id = %s" (Transport.Session.Id.to_string sid)) in 
        let (sch, p) = Lwt_stream.create_bounded C.channel_bound in
        let spush : Transport.Event.push  = fun e -> p#push e in
        let _ = handle_session sctx push spush in       
        let _ = event_loop sctx sch  in
        acceptor_loop (ssock, locator)
      in 
        let ls = List.map (fun l -> Locator.TcpLocator l) C.locators in
        let zs = List.zip self.socks ls in
        let als = List.map acceptor_loop zs  in
        (* let eloop = event_loop () in  *)
        Lwt.join als
    

    (* TODO: Operation to implement *)
    let listen _ = Lwt.return @@ Transport.Session.Id.next_id ()
    
    let connect loc = 
      match loc with 
      | Locator.TcpLocator tcploc -> (
        let open Lwt in
        let open Lwt_unix in
        let sock = socket PF_INET SOCK_STREAM 0 in
        let _ = setsockopt sock SO_REUSEADDR true in
        let _ = setsockopt sock TCP_NODELAY true in
        let saddr = IpEndpoint.to_sockaddr @@ TcpLocator.endpoint tcploc in
        connect sock saddr >>= ( fun () ->
          let ep = IpEndpoint.of_sockaddr saddr in
          let src = Locator.TcpLocator (TcpLocator.make ep) in
          let sid = (Transport.Session.Id.next_id ()) in
          let info = Transport.Session.Info.create sid src loc self.info in
          let inbuf = IOBuf.create C.bufsize in
          let outbuf = IOBuf.create C.bufsize in
          let lenbuf = IOBuf.create 4 in
          let sctx = { sock; inbuf; outbuf; lenbuf; info } in 
          let sm = SessionMap.add sid sctx self.smap in
          self.smap <- sm ;      (* fixme : race condition*)  
          Lwt.ignore_result @@ Logs_lwt.debug (fun m -> m "Openned cennection with session-id = %s" (Transport.Session.Id.to_string sid));
          let (sch, p) = Lwt_stream.create_bounded C.channel_bound in
          let spush : Transport.Event.push  = fun e -> p#push e in
          let _ = handle_session sctx (Option.get self.push) spush in       
          let _ = event_loop sctx sch in 
          Lwt.return (sid, spush)))
      | _ -> Lwt.fail @@ Exception (`InvalidAddress)
    
    (* let close _ = Lwt.return_unit *)

    let session_info sid = 
      let open Option.Infix in 
      (SessionMap.find_opt sid self.smap) >>= (fun cx -> Some cx.info)
      
  end
end


let makeTcpTransport ?(bufsize=65536) ?(backlog=10) ?(channel_bound=128) locators =   
  let module T = TcpTransport.Make( 
    struct 
      let name = "tcp"
      let id = Transport.Id.next_id ()
      let locators = locators
      let backlog = backlog      
      let bufsize = bufsize
      let channel_bound = channel_bound
    end) in Lwt.return (module T : Transport.S)

(* 
val name : string
  val id : Transport.Id.t
  val locators :  TcpLocator.t list
  (** The list of locators at which connection will be accepted.  This is useful
      for multihomed machines that want to deal with traffic coming from different
      interfaces *)
  val backlog : int
  val bufsize : int
  val channel_bound : int
   *)