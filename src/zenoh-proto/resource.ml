open Apero
open NetService
open R_name

type mapping = {
    id : Vle.t;
    session : Id.t;
    pub : bool;
    sub : bool option;
    sto : bool;
    matched_pub : bool;
    matched_sub : bool;
}

type t = {
    name : ResName.t;
    mappings : mapping list;
    matches : ResName.t list;
    local_id : Vle.t;
    last_value : IOBuf.t option;
}

let create_mapping id session = 
    {
        id;
        session;
        pub = false;
        sub = None;
        sto = false;
        matched_pub = false;
        matched_sub = false;
    }

let report_mapping m = 
    Printf.sprintf "SID:%2s RID:%2d PUB:%-4s SUB:%-4s MPUB:%-4s MSUB:%-4s" 
    (Id.to_string m.session) (Vle.to_int m.id)
    (match m.pub with true -> "YES" | false -> "NO")
    (match m.sub with None -> "NO" | Some true -> "PULL" | Some false -> "PUSH")
    (match m.matched_pub with true -> "YES" | false -> "NO")
    (match m.matched_sub with true -> "YES" | false -> "NO")

let report res = 
    Printf.sprintf "Resource name %s\n  mappings:\n" (ResName.to_string res.name) |> fun s ->
    List.fold_left (fun s m -> s ^ "    " ^ report_mapping m ^ "\n") s res.mappings ^ 
    "  matches:\n" |> fun s -> List.fold_left (fun s mr -> s ^ "    " ^ (ResName.to_string mr) ^ "\n") s res.matches

let with_mapping res mapping = 
    {res with mappings = mapping :: List.filter (fun m -> not (Id.equal m.session mapping.session)) res.mappings}

let update_mapping res sid updater = 
    let mapping = List.find_opt (fun m -> m.session = sid) res.mappings in 
    let mapping = updater mapping in
    with_mapping res mapping

let remove_mapping res sid = 
    {res with mappings = List.filter (fun m -> not (Id.equal m.session sid)) res.mappings}

let with_match res mname = 
    {res with matches = mname :: List.filter (fun r -> r != mname) res.matches}

let remove_match res mname = 
    {res with matches = List.filter (fun r -> r != mname) res.matches}

let res_match res1 res2 = ResName.name_match res1.name res2.name