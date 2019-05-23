open Lwt.Infix

module Storage:Plugins.Plugin = struct 
  module StrMap = Map.Make(String)

  let storagevar = Lwt_mvar.create (StrMap.empty)

  let listener spath src datas = 
    Lwt_mvar.take storagevar >>= fun storage -> 
    let storage = List.fold_left (
      fun storage data -> (data |> fst |> Apero.decode_string |> Printf.printf "PLUGIN 2 : STORAGE LISTENER [%-8s] RECIEVED RESOURCE [%-20s] : %s\n%!" spath src);
                          Abuf.reset_r_pos (fst data);
                          StrMap.add src data storage) 
      storage datas in 
    Lwt_mvar.put storagevar storage

  let qhandler spath resname predicate = 
    Printf.printf "PLUGIN 2 : STORAGE QHANDLER [%-8s] RECIEVED QUERY : %s?%s\n%!" spath resname predicate;
    Lwt_mvar.take storagevar >>= fun storage -> 
    Lwt_mvar.put storagevar storage >>= fun () -> 
    storage
    |> StrMap.filter (fun storesname _ -> Apero.PathExpr.intersect (Apero.PathExpr.of_string storesname) (Apero.PathExpr.of_string resname)) 
    |> StrMap.bindings 
    |> List.map (fun (k, (data, info)) -> (k, data, info))
    |> Lwt.return


  let run z args = 
    Printf.printf "PLUGIN 2 : %s\n%!" (Array.to_list args |> String.concat " "); 
    Zenoh.store z "/**" (listener "/**") (qhandler "/**") 
    >>= fun _ -> Lwt.return_unit
end


let () = 
  Plugins.register_plugin (module Storage:Plugins.Plugin)

(* let () = 
  Printf.printf "PLUGIN 2 %s\n%!" (Array.to_list Sys.argv |> String.concat " ") *)