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
open Zenoh_net
open Cmdliner
open Lwt.Infix

let peers = Arg.(value & opt string "tcp/127.0.0.1:7447" & info ["p"; "peers"] ~docv:"PEERS" ~doc:"peers")
let size = Arg.(value & opt int 8 & info ["s"; "size"] ~docv:"SIZE" ~doc:"payload size")
let batch_size = 16 * 1024 

let create_batch n size =
  let rec rec_create_batch n bs =
    if n > 0 then 
      let buf = Abuf.create size in 
      Abuf.set_w_pos size buf; 
      rec_create_batch (n-1) (buf::bs)
    else 
      bs
  in 
    rec_create_batch n []

let run peers size = 
  let batch = batch_size / size in 
  let bufs = create_batch batch size in 

  Lwt_main.run 
  (
    let%lwt z = zopen peers in 
    let%lwt home1pub = publish z "/home1" in 
    let buf = Abuf.create size in
    Abuf.set_w_pos size buf;
    let rec loop () = lstream home1pub bufs >>= loop in 
    loop ()
  )

let () = 
  let _ = Term.(eval (const run $ peers $ size, Term.info "throughput_pub")) in  ()