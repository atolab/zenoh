open Pervasives
open Spn_tree
open Spn_tree.Node

  type peer = 
    {
      pid : string;
      tsex : NetService.TxSession.t
    }
  type t =
    {
      tree_mod : (module Spn_tree.Set.S);
      tree_set : Spn_tree.Set.t;
      peers    : peer list;
      send_to  : peer list -> Spn_tree.Node.t list -> unit
    }

  let create sender id prio max_dist max_trees =
    let module Conf = struct 
        let local_id = id
        let local_prio = prio
        let max_dist = max_dist
        let max_trees = max_trees 
    end in
    let module TreeSet = Spn_tree.Set.Configure(Conf) in
    {
      tree_mod = (module TreeSet);
      tree_set = TreeSet.create;
      peers = [];
      send_to = sender
    }

  let report router = 
    let module TreeSet = (val router.tree_mod: Spn_tree.Set.S) in
    TreeSet.report router.tree_set

  let update router node =
    let module TreeSet = (val router.tree_mod: Spn_tree.Set.S) in
    let increased_node = {
      node_id  = node.node_id;
      tree_nb  = node.tree_nb;
      priority = node.priority;
      distance = node.distance + 1;
      parent   = node.parent;
      rank     = node.rank} in
    let new_set = TreeSet.update_tree_set router.tree_set increased_node in
    let old_state = List.find_opt (fun x -> node.tree_nb = x.local.tree_nb) router.tree_set in
    let new_state = List.find_opt (fun x -> node.tree_nb = x.local.tree_nb) new_set in
    let modified_trees =
      match new_state with
      | None -> []
      | Some state ->
        match old_state with
        | None -> [state.local]
        | Some old_state ->
          match (old_state.local = state.local) with
          | true -> []
          | false -> [state.local] in
    let to_send_nodes = match List.length new_set > List.length router.tree_set with
      | false -> modified_trees
      | true -> (List.hd new_set).local :: modified_trees in
    router.send_to router.peers to_send_nodes;
    {router with tree_set = new_set;}

  let new_node router node_id =
    router.tree_set
    |> List.map (fun tree -> tree.local)
    |> router.send_to [node_id];
    {router with peers = node_id :: router.peers;}

  let nodes router = router.peers

  let delete_node router node_id =
    let module TreeSet = (val router.tree_mod: Spn_tree.Set.S) in
    let new_set = TreeSet.delete_node router.tree_set node_id in
    let new_peers = List.filter (fun peer -> peer.pid <> node_id) router.peers in
    new_set
    |> List.filter (fun tree ->
          let old_item = List.find_opt (fun oldtree -> 
              tree.local.tree_nb = oldtree.local.tree_nb)
              router.tree_set in
          match old_item with
          | None -> false
          | Some item -> tree.local != item.local)
    |> List.map (fun tree -> tree.local)
    |> router.send_to new_peers;
    {router with tree_set = new_set; peers = new_peers;}

