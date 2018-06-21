open Ztypes
open Apero
open Transport

module TransportEngine = struct
  module TxMap = Map.Make(String) 
  let register name  tx_module = () 
  let load name = ResultM.fail Error.NotImplemented 
  let resolve name = ResultM.fail Error.NotImplemented 
end
