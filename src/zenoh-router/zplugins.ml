module PluginsArgs = Map.Make(String)

let add_plugin_arg plugin arg pluginargs =
  match PluginsArgs.find_opt plugin pluginargs with
  | None -> PluginsArgs.add plugin [arg] pluginargs
  | Some args -> PluginsArgs.add plugin (arg::args) pluginargs

let sep = Filename.dir_sep
let sepchar = String.get sep 0
let exe_dir = Filename.dirname Sys.executable_name

let get_plugin plugin = 
  List.find_opt (fun file -> Sys.file_exists file)
    [
      plugin;
      plugin ^ ".cmxs";
      plugin ^ "-plugin.cmxs";
    ] 

let plugin_locations plugin = 
  [
    plugin;
    exe_dir ^ sep ^ ".." ^ sep ^ "lib" ^ sep ^ plugin;
    "~/.zenoh/lib/" ^ plugin;
    "/usr/local/lib/" ^ plugin;
    "/usr/lib/" ^ plugin;
  ]

let lookup_plugin plugin = 
  let rec lookup plugins = 
    match plugins with 
    | [] -> None
    | plugin::plugins -> 
      match get_plugin plugin with 
      | Some plugin -> Some plugin 
      | None -> lookup plugins
  in
  lookup @@ plugin_locations plugin

let plugin_default_dirs = 
  [
    exe_dir ^ sep ^ ".." ^ sep ^ "lib";
    "~/.zenoh/lib/";
    "/usr/local/lib/";
    "/usr/lib/";
  ]

let lookup_default_plugins () = 
  let rec lookup dirs = 
    match dirs with 
    | [] -> None
    | dir::dirs -> 
      match Sys.file_exists dir with 
      | true -> (Sys.readdir dir |> Array.to_list
                |> List.filter (fun file -> String.length file > 12 
                                         && String.equal (Str.last_chars file 12) "-plugin.cmxs")
                |> List.map (fun file -> dir ^ sep ^file)
                |> function 
                | [] -> lookup dirs 
                | files -> Some(files))
      | false -> lookup dirs
  in
  match lookup plugin_default_dirs with
  | None -> []
  | Some ls -> ls
  

let plugin_name_of_file f =
  let startidx = match String.rindex_opt f sepchar with
    | Some i -> i+1
    | None -> 0
  in
  let suffixlen = if Apero.Astring.is_suffix ~affix:"-plugin.cmxs" f then
    String.length "-plugin.cmxs"
    else String.length ".cmxs"
  in
  String.sub f startidx (String.length f -startidx-suffixlen)

let load_plugins plugins plugins_args = match plugins with 
    | ["None"] -> ()
    | ["none"] -> ()
    | plugins -> 
        let plugins = match plugins with 
        | [] -> lookup_default_plugins ()
        | plugins -> plugins in
        Lwt_list.iter_p (fun plugin ->
        let args = String.split_on_char ' ' plugin |> Array.of_list in
        (try
            match lookup_plugin args.(0) with
            | Some plugin ->
              let plugin_name = plugin_name_of_file plugin in
              let args = match PluginsArgs.find_opt plugin_name plugins_args with
                | Some pargs -> Array.append args (Array.of_list pargs)
                | None -> args
              in
              Logs.info (fun m -> m "Loading plugin '%s' from '%s' with args: '%s'..." plugin_name plugin (String.concat " " @@ Array.to_list args));
              Dynload.loadfile plugin args
            | None -> Logs.warn (fun m -> m "Unable to find plugin %s !" plugin)
        with e -> Logs.warn (fun m -> m "Unable to load plugin %s ! Error: %s" plugin (Printexc.to_string e)));
        Lwt.return_unit
        ) plugins |> Lwt.ignore_result