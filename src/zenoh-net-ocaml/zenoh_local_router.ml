type local_sex = (Frame.Frame.t Lwt_stream.t * Frame.Frame.t Lwt_stream.bounded_push)
type local_sex_listener = local_sex -> unit Lwt.t

let router_ref:local_sex_listener option ref = ref None

let register_router listener = 
  router_ref := Some listener; Lwt.return_unit

let open_local_session () =  
  match !router_ref with 
  | None -> Lwt.return None 
  | Some listener -> 
    let (instream, inpush) = Lwt_stream.create_bounded 256 in
    let (outstream, outpush) = Lwt_stream.create_bounded 256 in
    let%lwt () = listener (instream, outpush) in
    Lwt.return @@ Some (outstream, inpush)




