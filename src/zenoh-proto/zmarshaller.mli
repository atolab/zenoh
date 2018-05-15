open Ziobuf
open Zmessage
open Zframe

module Marshaller : sig
  val read_msg : IOBuf.t -> (Message.t * IOBuf.t) Lwt.t
  val write_msg : IOBuf.t -> Message.t -> IOBuf.t Lwt.t

  val read_frame : IOBuf.t -> (Frame.t * IOBuf.t) Lwt.t
  val write_frame : IOBuf.t -> Frame.t -> IOBuf.t Lwt.t

end
