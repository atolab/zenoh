open Ztypes
open Zenoh
open Lwt
open Lwt.Infix
open Session

(* module SessionInfo = struct
  type t = {
    sid : SessionId.t;
    mutable pubs : Vle.t list;
    mutable subs : Vle.t list;
    lease_period : Vle.t
  }


end *)

module ProtocolEngine = struct
  type t = {
    pid : Lwt_bytes.t;
    lease : Vle.t;
    locators : Locators.t
  }

  let create pid lease ls = { pid; lease; locators = ls }

  let make_hello pe = Message.Hello (Hello.create (Vle.of_char ScoutFlags.scoutBroker) pe.locators [])

  let make_accept pe opid = Message.Accept (Accept.create pe.pid opid pe.lease Properties.empty)

  let process_scout pe scout =
    if Vle.logand (Scout.mask scout) (Vle.of_char ScoutFlags.scoutBroker) <> 0L then Some (make_hello pe)
    else None

  let process_open pe msg =
    ignore_result @@ Lwt_log.debug (Printf.sprintf "Accepting Open from remote peer: %s\n" (Lwt_bytes.to_string @@ Open.pid msg)) ;
    Some (make_accept pe (Open.pid msg))

  let process pe s msg =
    ignore_result @@ Lwt_log.debug (Printf.sprintf "Received message: %s" (Message.to_string msg));
    match msg with
    | Message.Scout s -> process_scout pe s
    | Message.Hello _ -> None
    | Message.Open o -> process_open pe o
    | Message.Close c -> ignore_result @@ Session.(s.close ()) ; None
    | _ -> None


end
