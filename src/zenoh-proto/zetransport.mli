open Zframe
open Lwt
open Zlocator


module Int64Id :  sig
  include (module type of Int64)
  val next_id : unit -> t
end

module Transport : sig 

  module  Info : sig 
      type kind = Packet| Stream  
      type t      
      val make : string -> int -> bool -> kind -> int option -> t
      val name : t -> string
      val id : t -> int
      val reliable : t -> bool
      val kind : t -> kind
      val mtu : t -> int option
      val compare : t -> t -> int
  end

  module Session : sig 
    module Id : sig 
      type t
      val create : int64 -> int64 -> t
      val id : t -> int64
      val tid : t -> int64
      val compare : t -> t -> int
    end
    module Info : sig  
      type t
      val create : Id.t -> Locator.t -> Locator.t -> Info.t -> t
      val id : t -> Id.t
      val source : t -> Locator.t
      val dest : t -> Locator.t
      val transport_info : t -> Info.t
    end
  end

  type event =
    | SessionClose of Session.Id.t
    | SessionMessage of  Frame.t * Session.Id.t
    | LocatorMessage of Frame.t * Locator.t   
    | Events of event list

  type transport_react = event -> unit
  

  module type S = sig       
    val info : Info.t
    val start : unit -> unit Lwt.t
    val stop : unit -> unit Lwt.t
    val info : Info.t  
    val engine :  transport_react -> unit
    val listen : Locator.t -> Session.Id.t Lwt.t
    val connect : Locator.t -> Session.Id.t Lwt.t
    val close :  Session.Id.t -> unit Lwt.t  
    val process : event -> unit Lwt.t 
    val send : Session.Id.t -> Frame.t -> unit Lwt.t
    val send_to : Locator.t -> Frame.t -> unit Lwt.t 
    val session_info : Session.Id.t -> Session.Info.t option
  end  

  module Engine : sig
    type t    
    type transport_react = event list -> event list

    val create : unit -> t Lwt.t
    val add_transport : t -> (module S) -> int64 Lwt.t
    val remove_transport : t -> int64 -> bool Lwt.t
    val listen : t -> Locator.t -> Session.Id.t Lwt.t
    val connect : t -> Locator.t -> Session.Id.t Lwt.t
    val close : t -> Session.Id.t -> unit Lwt.t
    val react : t -> transport_react -> unit Lwt.t 
    val process : t -> event -> unit Lwt.t 
    val send : t -> Session.Id.t -> Frame.t -> unit Lwt.t
    val send_to : t -> Locator.t -> Frame.t -> unit Lwt.t 
    val session_info : t -> Session.Id.t -> Session.Info.t option
  end
end
