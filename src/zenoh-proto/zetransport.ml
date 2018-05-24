open Ztypes
open Zframe
open Lwt
open Zlocator

module Int64Id = struct 
  include Int64
  let session_count = ref 0L
  let next_id () =
    let r = !session_count in  
    (** roll-over to negatives and eventually back to zero*)
    session_count := add !session_count 1L ; r    
end


module Session = struct 
  module Id = struct 
    type t = int64 * int64
    let create sid tid = sid,tid 
    let id (a, _) = a
    let tid (_, b) = b
    let compare a b = compare a b 
  end 
  module Info = struct 
    type t = { id : Id.t; src : Locator.t; dest : Locator.t; transport_info : TransportProtoInfo.t }

    let create id src dest transport_info = {id; src; dest; transport_info}
    let id si = si.id
    let source si = si.src
    let dest si = si.dest 
    let transport_info si = si.transport_info
  end
end

module Transport = struct

  type event =
    | SessionClose of Session.Id.t
    | SessionMessage of  Frame.t * Session.Id.t
    | LocatorMessage of Frame.t * Locator.t   
    | Events of event list

  type transport_react = event -> unit

  module type S = sig     
    val start : unit -> unit Lwt.t
    val stop : unit -> unit Lwt.t
    val tranport_info : TransportProtoInfo.t  
    val engine :  transport_react -> unit
    val listen : Locator.t -> Session.Id.t Lwt.t
    val connect : Locator.t -> Session.Id.t Lwt.t
    val close :  Session.Id.t -> unit Lwt.t  
    val process : event -> unit Lwt.t 
    val send : Session.Id.t -> Frame.t -> unit Lwt.t
    val send_to : Locator.t -> Frame.t -> unit Lwt.t 
    val session_info : Session.Id.t -> Session.Info.t option
  end  

  module Engine = struct
    open Lwt
    type t = int   
    type transport_react = event list -> event list

    let failw_with_not_impl () = fail (ZError Error.(NotImplemented))
    let create () = return 0
    let add_transport e m = return 0L

    let remove_transport e id = return false
    let listen e loc = fail (ZError Error.(NotImplemented))

    let connect e loc = fail (ZError Error.(NotImplemented))
    let close e loc = fail (ZError Error.(NotImplemented))
    let react e reaction = fail (ZError Error.(NotImplemented))
    let process e evt = fail (ZError Error.(NotImplemented))
    let send e sid frame = fail (ZError Error.(NotImplemented))
    let send_to e loc frame = fail (ZError Error.(NotImplemented))
    let session_info e sid = None
  end
end