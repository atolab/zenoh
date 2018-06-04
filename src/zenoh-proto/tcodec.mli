open Ztypes
open Ziobuf
open Zproperty
open Zlocator

(** This module encodes and decodes primitive zeno types *)

val encode_vle : Vle.t -> IOBuf.t -> (IOBuf.t, Error.e) result
val decode_vle : IOBuf.t -> ((Vle.t * IOBuf.t), Error.e) result

val encode_bytes : IOBuf.t -> IOBuf.t -> (IOBuf.t, Error.e) result
val decode_bytes : IOBuf.t -> (IOBuf.t * IOBuf.t, Error.e) result

val encode_string : string -> IOBuf.t -> (IOBuf.t, Error.e) result
val decode_string : IOBuf.t -> ((string * IOBuf.t), Error.e) result

val encode_seq : ('a -> IOBuf.t -> (IOBuf.t, Error.e) result) -> 'a list -> IOBuf.t -> (IOBuf.t, Error.e) result
val decode_seq : (IOBuf.t -> ('a * IOBuf.t, Error.e) result) ->  IOBuf.t -> ('a list * IOBuf.t, Error.e) result

val decode_property : IOBuf.t -> ((Property.t * IOBuf.t), Error.e) result
val encode_property : Property.t -> IOBuf.t -> (IOBuf.t, Error.e) result

val decode_locator : IOBuf.t -> (((Locator.t option) * IOBuf.t), Error.e) result
val encode_locator : Locator.t -> IOBuf.t -> (IOBuf.t, Error.e) result

val decode_locators : IOBuf.t -> ((Locators.t * IOBuf.t), Error.e) result
val encode_locators : Locators.t -> IOBuf.t -> (IOBuf.t, Error.e) result
