(*
 * Copyright (c) 2014, 2020 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 *
 * Contributors:
 * Angelo Corsaro, <angelo.corsaro@adlinktech.com>
 * Olivier Hecart, <olivier.hecart@adlinktech.com>
 * Julien Enoch, <julien.enoch@adlinktech.com>.
 *)
open Apero_net 
(* open Zenoh_proto *)

module ZTcpTransport = NetServiceTcp
module ZTcpConfig = NetServiceTcp.TcpConfig

(* TODO: the functions below should be implemented to really deal
   with frames of arbitrary lenght and do that efficiently. 
   One approach could be to use the provide buffer if the frame 
   to read/write fits, and otherwise to switch to another buffer *)

(*   
let run_ztcp_svc buf_size reader writer (svc:ZTcpTransport.t) (engine: ProtocolEngine.t) (sex: TxSession.t) = 
  let rbuf = Abuf.create buf_size in 
  let wbuf = Abuf.create buf_size in
  let socket = (TxSession.socket sex) in
  let zreader = reader  rbuf socket in 
  let zwriter = writer wbuf socket  in
  let push = ProtocolEngine.event_push engine in 
  fun () ->
    Lwt.bind 
      @@ zreader ()
      @@ fun frame -> Frame.to_list
       *)


