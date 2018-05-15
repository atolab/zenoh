open Lwt
open Zlwt
open Ztypes
open Ziobuf
open Zmessage

module Frame = struct
  type t = {msgs : Message.t list}
  let empty = {msgs = [];}
  let create msgs = {msgs}
  let add msg f = {msgs = msg::(f.msgs)}
  let length f = List.length f.msgs
  let to_list f = f.msgs
  let from_list msgs = {msgs}
end
