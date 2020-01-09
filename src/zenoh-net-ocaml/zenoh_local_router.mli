type local_sex = (Frame.Frame.t Lwt_stream.t * Frame.Frame.t Lwt_stream.bounded_push)
type local_sex_listener = local_sex -> unit Lwt.t

val register_router : local_sex_listener -> unit Lwt.t

val open_local_session : unit -> local_sex option Lwt.t