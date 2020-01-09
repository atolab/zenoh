module HLC = Apero_time.HLC.Make (Apero_time.Clock_unix)
module Timestamp = HLC.Timestamp
module Time = HLC.Timestamp.Time
