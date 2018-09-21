open Spn_tree
open Sexplib.Std

module IntLBRange : LowBoundedRange with type t = int = struct
  type t = int [@@deriving sexp]
  let compare = Pervasives.compare
  let min = 0
end

module type Configuration = Spn_tree.Configuration with type nid_t = string and type prio_t = int and type dist_t = int

module Make(Conf : Configuration) = struct
  module TreeSet = Make_tree_set(struct type t = string [@@deriving sexp] end)(IntLBRange)(IntLBRange)(Conf)
  open TreeSet.Tree
  open TreeSet.Tree.Node
  open Pervasives

  type peer = 
    {
      pid : string;
      tsex : NetService.TxSession.t
    }

  type t =
    {
      tree_set : TreeSet.t;
      peers : peer list;
      send_to : peer list -> TreeSet.Tree.Node.t list -> unit
    }
  type node_t = TreeSet.Tree.Node.t

  let create sender =
    {
      tree_set = TreeSet.create;
      peers = [];
      send_to = sender
    }

  let print router = TreeSet.print router.tree_set

  let update router node =
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
    {tree_set = new_set; peers = router.peers; send_to = router.send_to}

  let new_node router node_id =
    router.tree_set
    |> List.map (fun tree -> tree.local)
    |> router.send_to [node_id];
    {tree_set = router.tree_set; peers = node_id :: router.peers; send_to = router.send_to}

  let nodes router = router.peers

  let delete_node router node_id =
    let new_set = TreeSet.delete_node router.tree_set node_id in
    new_set
    |> List.filter (fun tree ->
          let old_item = List.find_opt (fun oldtree -> 
              tree.local.tree_nb = oldtree.local.tree_nb)
              router.tree_set in
          match old_item with
          | None -> false
          | Some item -> tree.local = item.local)
    |> List.map (fun tree -> tree.local)
    |>router.send_to router.peers;
    let new_peers = List.filter (fun peer -> peer.pid <> node_id) router.peers in
    {tree_set = new_set; peers = new_peers; send_to = router.send_to}

end
