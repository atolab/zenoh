open Engine_state
open Message
open Apero


module HLC = Apero_time.HLC.Make (Apero.MVar_lwt) (Apero_time.Clock_unix)

let hlc = HLC.create (Uuid.make ())

let admin_prefix = "/_z_/"

let is_admin q = String.equal admin_prefix (String.sub (Message.Query.resource q) 0 (String.length admin_prefix))

let broker_json pe = 
  let locators = pe.locators |> Locator.Locators.to_list |> List.map (fun l -> `String(Locator.Locator.to_string l)) in
  `Assoc [ 
      ("pid",      `String (IOBuf.hexdump pe.pid));      
      ("locators", `List locators);
      ("lease",    `Int (Vle.to_int pe.lease));
    ]

let full_broker_json pe = 
  let locators = pe.locators |> Locator.Locators.to_list |> List.map (fun l -> `String(Locator.Locator.to_string l)) in
  `Assoc [ 
      ("pid",      `String (IOBuf.hexdump pe.pid));      
      ("locators", `List locators);
      ("lease",    `Int (Vle.to_int pe.lease));
      ("router",   (ZRouter.to_yojson pe.router));
    ]

let broker_path_str pe = String.concat "" [admin_prefix; "services/"; IOBuf.hexdump pe.pid]
let broker_path pe = Path.of_string @@ broker_path_str pe
let router_path pe = Path.of_string (String.concat "" [admin_prefix; "services/"; IOBuf.hexdump pe.pid; "/routing"])
let resource_path pe res_name = Path.of_string @@ String.concat "" [broker_path_str pe; "/resources/"; R_name.ResName.to_string res_name] 

let json_replies pe q = 
  let qexpr = PathExpr.of_string (Message.Query.resource q) in 
  List.concat [
    (* match PathExpr.is_matching_path (broker_path pe) qexpr with 
    | true -> [((broker_path pe), (broker_json pe))]
    | false -> []
    ;
    match PathExpr.is_matching_path (router_path pe) qexpr with 
    | true -> [((router_path pe), (ZRouter.to_yojson pe.router))]
    | false -> []
    ; *)
    match PathExpr.is_matching_path (broker_path pe) qexpr with 
    | true -> [((broker_path pe), (full_broker_json pe))]
    | false -> []
  ]

let replies pe q = 
  let%lwt time = HLC.new_timestamp hlc in
  Lwt.return @@ List.mapi (fun idx (p, j) -> 
    let pl = IOBuf.create ~grow:65536 1024 
      |> HLC.Timestamp.encode time |> Result.get 
      |> IOBuf.put_char @@ char_of_int 0x03 |> Result.get (*JSON encoding*)
      |> Apero.encode_string (Yojson.Safe.pretty_to_string j) |> Result.get 
      |> IOBuf.flip in
    Reply.create (Query.pid q) (Query.qid q) (Some (pe.pid, Vle.of_int (idx + 1), Path.to_string p, pl))
  ) (json_replies pe q)
