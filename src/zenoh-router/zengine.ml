open Apero
open Apero_net
open Zenoh_tx_inet
open Zenoh_proto
open NetService
open R_name
open Engine_state
open Cmdliner


module Engine (MVar : MVar) = struct
 
  open Scouting
  open Discovery
  open Routing
  open Querying

  module ProtocolEngine = struct

    type t = engine_state Guard.t

    let send_nodes peer _nodes = 
      let open Message in
      List.iter (fun node -> 
          let b = Marshal.to_bytes node [] in
          let sdata = Message.with_marker 
              (CompactData(CompactData.create (true, true) 0L 0L None (Abuf.from_bytes b |> Payload.create)))
              (RSpace (RSpace.create 1L)) in           
          Lwt.ignore_result @@ Mcodec.ztcp_safe_write_frame_alloc ZRouter.(peer.tsex) (Frame.create [sdata]) ) _nodes

    let send_nodes peers nodes = List.iter (fun peer -> send_nodes peer nodes) peers

    let create ?(bufn = 32) ?(buflen=65536) (pid : Abuf.t) (lease : Vle.t) (ls : Locators.t) (peers : Locator.t list) strength (tx_connector: tx_session_connector) = 
      Guard.create @@ { 
        pid; 
        lease; 
        locators = ls; 
        smap = SIDMap.empty; 
        rmap = ResMap.empty; 
        qmap = QIDMap.empty;
        peers;
        router = ZRouter.create send_nodes (Abuf.hexdump pid) strength 2 0;
        next_mapping = 0L; 
        tx_connector;
        buffer_pool = Lwt_pool.create bufn (fun () -> Lwt.return @@ Abuf.create_bigstring ~grow:8192 buflen) }

    let start engine = 
      connect_peers (Guard.get engine)

    let process_synch _ _ msg =
      let asn = Message.Synch.sn msg in
      Lwt.return [Message.with_markers (Message.AckNack (Message.AckNack.create asn None)) (Message.markers (Message.Synch msg))]

    let process_ack_nack _ _ _ = Lwt.return []

    let handle_message engine (tsex : Session.tx_sex) (msgs: Message.t list)  = 
      let open Lwt.Infix in   
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
        | Message.BatchedStreamData msg -> process_batched_stream_data engine tsex msg
        | Message.CompactData msg -> process_compact_data engine tsex msg
        | Message.WriteData msg -> process_write_data engine tsex msg
        | Message.Query msg -> process_query engine tsex msg
        | Message.Reply msg -> process_reply engine tsex msg
        | Message.Pull msg -> process_pull engine tsex msg
        | Message.KeepAlive _ -> Lwt.return []
        | _ -> Lwt.return []
      in Lwt_list.map_p dispatch msgs >|= List.flatten 

  end
end

let locator = Apero.Option.get @@ TcpLocator.of_string "tcp/0.0.0.0:7447"
let listen_address = Unix.inet_addr_any
let port = 7447
let backlog = 10
let max_connections = 1000
let buf_size = 64 * 1024
let svc_id = 0x01

let lease = 0L
let version = Char.chr 0x01

let pid  = 
  Abuf.create_bigstring 32 |> fun buf -> 
  Abuf.write_bytes (Bytes.unsafe_of_string ((Uuidm.to_bytes @@ Uuidm.v5 (Uuidm.create `V4) (string_of_int @@ Unix.getpid ())))) buf; 
  buf

let to_string peers = 
  peers
  |> List.map (fun p -> Locator.to_string p) 
  |> String.concat "," 

module ZEngine = Engine(MVar_lwt)

let run tcpport peers strength bufn (local_session:Session.local_sex option) = 
  let open ZEngine in 
  let peers = String.split_on_char ',' peers 
  |> List.filter (fun s -> not (String.equal s ""))
  |> List.map (fun s -> Option.get @@ Locator.of_string s) in
  let%lwt _ = Logs_lwt.info (fun m -> m "pid : %s" (Abuf.hexdump pid)) in
  let%lwt _ = Logs_lwt.info (fun m -> m "tcpport : %d" tcpport) in
  let%lwt _ = Logs_lwt.info (fun m -> m "peers : %s" (to_string peers)) in
  let locator = Option.get @@ Iplocator.TcpLocator.of_string (Printf.sprintf "tcp/0.0.0.0:%d" tcpport);  in

  let config = ZTcpConfig.make ~backlog ~max_connections ~buf_size ~svc_id locator in 
  let tx = ZTcpTransport.make config in 
  let tx_connector = ZTcpTransport.establish_session tx in 
  let engine = ProtocolEngine.create ~bufn pid lease (Locators.of_list [Locator.TcpLocator(locator)]) peers strength tx_connector in

  let open Lwt.Infix in 

  let dispatcher_svc txsex  = 
    let sex = Session.(TxSex(txsex)) in
    let wbuf = Abuf.create ~grow:4096 buf_size in
    let zreader = ztcp_read_frame sex in 
    let zwriter = ztcp_safe_write_frame sex in
    fun (freebufp, usedbufp) ->
      let%lwt readbuf = freebufp in
      Abuf.clear readbuf;
      zreader readbuf () >>= fun frame ->
        Lwt.catch
          (fun () -> ProtocolEngine.handle_message engine sex (Frame.to_list frame)) 
          (fun e -> 
            Logs_lwt.warn (fun m -> m "Error handling messages from session %s : %s" 
              (Id.to_string (Session.txid sex))
              (Printexc.to_string e))
            >>= fun _ -> Lwt.return []) 
        >>= function
      | [] -> Lwt.return (usedbufp, Lwt.return readbuf)
      | ms -> Abuf.clear wbuf; zwriter (Frame.create ms) wbuf >>= fun _ -> Lwt.return (usedbufp, Lwt.return readbuf)
  in 

  let%lwt () = match local_session with 
  | Some lsex -> 
    let tx_sex = Session.Local lsex in
    let fakebuf = Abuf.create 0 in
    let rec local_loop () = 
      let%lwt frame = ztcp_read_frame tx_sex fakebuf () in 
      Lwt.catch
        (fun () -> ProtocolEngine.handle_message engine tx_sex (Frame.to_list frame)) 
        (fun e -> 
          Logs_lwt.warn (fun m -> m "Error handling messages from local session : %s" 
            (Printexc.to_string e))
              >>= fun _ -> Lwt.return []) 
      >>= (function
      | [] -> Lwt.return_unit
      | ms -> ztcp_safe_write_frame tx_sex (Frame.create ms) fakebuf >>= fun _ -> Lwt.return_unit)
      >>= fun () -> local_loop ()
    in 
    local_loop () |> Lwt.ignore_result;
    Scouting.add_session engine tx_sex 0L >>= fun _ -> Lwt.return_unit
  | None -> Lwt.return_unit
  in

  Lwt.join [ZTcpTransport.start tx (fun _ -> 
                                    let rbuf1 = Abuf.create ~grow:4096 buf_size in 
                                    let rbuf2 = Abuf.create ~grow:4096 buf_size in 
                                    Lwt.return (Lwt.return rbuf1, Lwt.return rbuf2)) 
                                    dispatcher_svc; ProtocolEngine.start engine]


let tcpport = Arg.(value & opt int 7447 & info ["t"; "tcpport"] ~docv:"TCPPORT" ~doc:"The listening tcp port.")
let peers = Arg.(value & opt string "" & info ["p"; "peers"] ~docv:"PEERS" ~doc:"The peers to connect to.")
let strength = Arg.(value & opt int 0 & info ["s"; "strength"] ~docv:"STRENGTH" ~doc:"The router strength.")
let bufn = Arg.(value & opt int 8 & info ["w"; "wbufn"] ~docv:"BUFN" ~doc:"The number of write buffers.")
let plugins = Arg.(value & opt_all string [] & info ["g"; "plugin"] ~docv:"PLUGIN" ~doc:"Plugin to load at startup.")