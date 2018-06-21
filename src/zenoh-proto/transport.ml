open Ztypes
open Frame
open Lwt
open Locator

module ZId = Id

module Transport = struct

  module Id = ZId.Make ( 
    struct 
      include Int32 
      let show = Int32.to_string       
    end)

  module  Info = struct
    type kind = Packet| Stream  
    type t = {name : string; id : Id.t; reliable : bool; kind : kind; mtu : int option }
    let create name id reliable kind mtu = {name; id; reliable; kind; mtu}
    let name i = i.name 
    let id i = i.id 
    let reliable i = i.reliable
    let kind i = i.kind
    let mtu i = i.mtu 
    let compare a b = compare a b
  end

  module Session = struct 
    module Id = ZId.Make ( 
      struct 
        include Int64
        let show = Int64.to_string
      end)
    module Info = struct 
      type t = { id : Id.t; src : Locator.t; dest : Locator.t; transport_info : Info.t }

      let create id src dest transport_info = {id; src; dest; transport_info}
      let id si = si.id
      let source si = si.src
      let dest si = si.dest 
      let transport_info si = si.transport_info
    end
  end


   
  module Event = struct
    type event = 
      | SessionClose of Session.Id.t
      | SessionMessage of  Frame.t * Session.Id.t * (push option)
      | LocatorMessage of Frame.t * Locator.t * (push option)
      | Events of event list 
    and  pull = unit -> event Lwt.t
    and  push = event -> unit Lwt.t
  end
  
  module type S = sig 
    val start : Event.push -> unit Lwt.t
    val stop : unit -> unit Lwt.t
    val info : Info.t  
    val listen : Locator.t -> Session.Id.t Lwt.t
    val connect : Locator.t -> Session.Id.t Lwt.t
    val session_info : Session.Id.t -> Session.Info.t option
  end  

  module Engine = struct
    open Lwt
    type t = int   

    let create () = Lwt.return 0
    let add_transport (e: t) (m : (module S)) = return Id.zero 
    let remove_transport e id = return false
    let listen e loc = fail (ZError Error.(NotImplemented))
    let connect e loc = fail (ZError Error.(NotImplemented))
    let start e pull = fail (ZError Error.(NotImplemented))    
    let session_info e sid = None    
  end
end
