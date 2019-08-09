open Apero
open Apero_net
open NetService
open R_name
open Query

module SIDMap = Map.Make(NetService.Id)
module QIDMap = Map.Make(Qid)

type tx_session_connector = Locator.t -> TxSession.t Lwt.t 

type engine_state = {
    pid : Abuf.t;
    lease : Vle.t;
    locators : Locators.t;
    hlc : Ztypes.HLC.t;
    timestamp : bool;
    smap : Session.t SIDMap.t;
    rmap : Resource.t ResMap.t;
    qmap : Query.t QIDMap.t;
    peers : Locator.t list;
    users : (string * string) list option;
    trees : Spn_trees_mgr.t;
    next_mapping : Vle.t;
    tx_connector : tx_session_connector;
    buffer_pool : Abuf.t Lwt_pool.t;
    next_local_id : NetService.Id.t;
}

let report_resources e = 
    List.fold_left (fun s (_, r) -> s ^ Resource.report r ^ "\n") "" (ResMap.bindings e.rmap)