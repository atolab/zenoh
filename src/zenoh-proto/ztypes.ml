module HLC = Apero_time.HLC.Make (Apero_time.Clock_unix)
module Timestamp = HLC.Timestamp

type query_dest = 
| Partial
| Complete of int
| All

type data_info = {
  srcid:    Abuf.t option;
  srcsn:    int64 option;
  bkrid:    Abuf.t option;
  bkrsn:    int64 option;
  ts:       Timestamp.t option;
  kind:     int64 option;
  encoding: int64 option;
}

let empty_data_info = {srcid=None; srcsn=None; bkrid=None; bkrsn=None; ts=None; encoding=None; kind=None}