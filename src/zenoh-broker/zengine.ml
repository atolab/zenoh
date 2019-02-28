open Apero
open Apero_net
open NetService
open R_name
open Engine_state


module ZEngine (MVar : MVar) = struct
 
  open Scouting
  open Discovery
  open Routing
  open Querying

  module ProtocolEngine = struct

    type t = engine_state Guard.t

    let send_nodes peer _nodes = 
      let open Message in
      let open Frame in 
      List.iter (fun node -> 
          let b = Marshal.to_bytes node [] in
          let sdata = Message.with_marker 
              (CompactData(CompactData.create (true, true) 0L 0L None (Abuf.from_bytes b |> Payload.create)))
              (RSpace (RSpace.create 1L)) in           
          Lwt.ignore_result @@ Mcodec.ztcp_write_frame_alloc (TxSession.socket ZRouter.(peer.tsex)) (Frame.create [sdata]) ) _nodes

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

    let handle_message engine (tsex : TxSession.t) (msgs: Message.t list)  = 
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