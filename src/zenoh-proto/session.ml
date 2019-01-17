open Apero
open Channel
open NetService
open R_name

let framing_buf_len = 16

module SessionId :  sig
  include (module type of Int64)
  val next_id : unit -> t
end = struct
  include Int64
  let session_count = ref 0L

  let next_id () =
    let r = !session_count in  session_count := add !session_count 1L ; r
end


type t = {    
  tx_sex : TxSession.t;      
  ic : InChannel.t;
  oc : OutChannel.t;
  rmap : ResName.t VleMap.t;
  mask : Vle.t;
  sid : Id.t
}

let create tx_sex mask =
  let ic = InChannel.create Int64.(shift_left 1L 16) in
  let oc = OutChannel.create Int64.(shift_left 1L 16) in        
  {      
    tx_sex;
    ic;
    oc;
    rmap = VleMap.empty; 
    mask = mask;
    sid = TxSession.id tx_sex
  }
let in_channel s = s.ic
let out_channel s = s.oc
let tx_sex s = s.tx_sex
let id s = TxSession.id s.tx_sex
let is_broker s = Message.ScoutFlags.hasFlag s.mask Message.ScoutFlags.scoutBroker