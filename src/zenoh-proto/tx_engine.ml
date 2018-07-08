open Apero
open Transport

module TransportEngine = struct
  module TxMap = Map.Make(String) 
  let register name  tx_module = () 
  let load name = Result.fail `NotImplemented 
  let resolve name = Result.fail `NotImplemented 
end
