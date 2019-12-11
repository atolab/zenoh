type error_info = [`NoMsg | `Msg of string | `Code of int | `Pos of (string * int * int * int) | `Loc of string] [@@deriving show]  

type yerror = [
  | `InvalidBackendProperty of error_info
  | `InsufficientStorage
  | `InvalidPath of error_info
  | `Forbidden of error_info
  | `InvalidParameters
  | `ConflictingStorage of error_info
  | `NoCompatibleBackend of error_info
  | `UnknownStorage of error_info
  | `UnknownAccess of error_info
  | `UnknownStorageKind 
  | `UnavailableStorageFactory of error_info
  | `UnkownAccessId of error_info
  | `StoreError of error_info
  | `UnauthorizedAccess of error_info
  | `UnsupportedTranscoding of error_info
  | `UnsupportedOperation
  | `InternalError of error_info
] [@@deriving show]


exception YException of yerror [@@deriving show]

let () = Printexc.register_printer @@ function | YException(e) -> Some ("YException: "^(show_yerror e)) | _ -> None
