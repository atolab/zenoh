open Engine_state
open Message
open Apero


let admin_prefix = "/_z_/"

let is_admin q = 
  String.length (Message.Query.resource q) >= String.length admin_prefix &&
  String.equal admin_prefix (String.sub (Message.Query.resource q) 0 (String.length admin_prefix))

let broker_json pe = 
  let locators = pe.locators |> Locator.Locators.to_list |> List.map (fun l -> `String(Locator.Locator.to_string l)) in
  `Assoc [ 
      ("pid",      `String (Abuf.hexdump pe.pid));
      ("locators", `List locators);
      ("lease",    `Int (Vle.to_int pe.lease));
    ]

let full_broker_json pe = 
  let locators = pe.locators |> Locator.Locators.to_list |> List.map (fun l -> `String(Locator.Locator.to_string l)) in
  `Assoc [ 
      ("pid",      `String (Abuf.hexdump pe.pid));
      ("locators", `List locators);
      ("lease",    `Int (Vle.to_int pe.lease));
      ("router",   (ZRouter.to_yojson pe.router));
    ]

let broker_path_str pe = String.concat "" [admin_prefix; "services/"; Abuf.hexdump pe.pid]
let broker_path pe = Path.of_string @@ broker_path_str pe
let router_path pe = Path.of_string (String.concat "" [admin_prefix; "services/"; Abuf.hexdump pe.pid; "/routing"])
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
  let open Ztypes in
  Lwt.return @@ List.mapi (fun idx (p, j) -> 
    let data = Abuf.create ~grow:65536 1024 in 
    Apero.encode_string (Yojson.Safe.pretty_to_string j) data;
    let info = {srcid=None; srcsn=None; bkrid=None; bkrsn=None; ts=None; encoding=Some 4L (* JSON *); kind=None} in
    let pl = Payload.create ~header:info data in
    Reply.create (Query.pid q) (Query.qid q) (Some (pe.pid, Vle.of_int (idx + 1), Path.to_string p, pl))
  ) (json_replies pe q)
