open Sexplib.Std
open Printf


module type Sexpable = sig
  type t [@@deriving sexp]
end

module type LowBoundedRange = sig
  type t [@@deriving sexp]
  val compare : t -> t -> int
  val min : t
end

module Make_node
      (NodeId : Sexpable)
      (Priority : LowBoundedRange)
      (Distance : LowBoundedRange) = struct

  type t = {
    node_id  : NodeId.t;
    tree_nb  : int;
    priority : Priority.t;
    distance : Distance.t;
    parent   : NodeId.t option;
    rank     : int
  }  [@@deriving sexp]

  let compare t1 t2 =
    let c1 = compare t1.rank t2.rank in
    if c1 <> 0 then c1 else 
    begin
      let c2 = Priority.compare t1.priority t2.priority in
      if c2 <> 0 then c2 else Distance.compare t2.distance t1.distance
    end
end

module Make_tree
      (NodeId : Sexpable)
      (Priority : LowBoundedRange)
      (Distance : LowBoundedRange) = struct

  module Node = Make_node(NodeId)(Priority)(Distance)
  open Node

  type t = {
    local : Node.t;
    peers : Node.t list;
  }

  let update tree node =
    let open Node in
    {
      local =
        if compare node tree.local > 0 then
        {
          node_id  = tree.local.node_id;
          tree_nb  = node.tree_nb;
          priority = node.priority;
          distance = node.distance;
          parent   = Some node.node_id;
          rank     = node.rank
        }
        else tree.local;
      peers =
        match List.find_opt (fun peer -> peer.node_id = node.node_id) tree.peers with
        | None -> node :: tree.peers
        | Some _ -> List.map (fun peer -> if peer.node_id = node.node_id then node else peer) tree.peers
    }

  let delete_node tree node =
    let new_peers = List.filter (fun peer -> (peer.node_id <> node)) tree.peers in
    {
      peers = new_peers;
      local = match tree.local.parent with
        | None -> tree.local
        | Some id -> if id = node then 
          begin
            let rec max_list l = match l with
            | [] -> invalid_arg "empty list"
            | x :: [] -> x
            | x :: remain -> max x (max_list remain) in
              max_list new_peers
          end 
          else tree.local;
          (* TODO : more to do if dead parent was also root *)
    }

  let is_stable tree =
    List.for_all (fun peer -> peer.priority = tree.local.priority) tree.peers

  let get_parent tree =
    match tree.local.parent with
    | None -> None
    | Some parent ->
    List.find_opt (fun peer -> peer.node_id = parent) tree.peers

  let get_childs tree =
    List.filter (fun peer -> 
      match peer.parent with
      | None -> false
      | Some parent -> parent = tree.local.node_id) tree.peers

  let get_broken_links tree =
    tree.peers
    |> List.filter (fun (peer:Node.t) -> 
        match peer.parent with
        | None -> false
        | Some parent -> parent <> tree.local.node_id)
    |> List.filter (fun (peer:Node.t) -> 
        match get_parent tree with
        | None -> true
        | Some parent -> parent.node_id <> peer.node_id)

  let print tree =
    printf "   Local : %s\n%!" (Sexplib.Sexp.to_string (Node.sexp_of_t tree.local));
    printf "      Parent      : %s\n%!"
      (match get_parent tree with
      | None -> "none"
      | Some parent -> (Sexplib.Sexp.to_string (Node.sexp_of_t (parent))));
    List.iter (fun peer -> printf "      Children    : %s\n%!" (Sexplib.Sexp.to_string (Node.sexp_of_t peer))) (get_childs tree);
    List.iter (fun peer -> printf "      Broken link : %s\n%!" (Sexplib.Sexp.to_string (Node.sexp_of_t peer))) (get_broken_links tree)
end

module type Configuration = sig
  type nid_t
  type prio_t
  type dist_t
  val local_id : nid_t
  val local_prio : prio_t
  val max_dist : dist_t
  val max_trees : int
end

module Make_tree_set
      (NodeId : Sexpable)
      (Priority : LowBoundedRange)
      (Distance : LowBoundedRange)
      (Conf : Configuration with
      type nid_t = NodeId.t and
      type prio_t = Priority.t and
      type dist_t = Distance.t) = struct

  module Tree = Make_tree(NodeId)(Priority)(Distance)
  open Tree

  type t = Tree.t list

  let create =
    [{
      local = 
      {
        node_id  = Conf.local_id;
        tree_nb  = 0;
        priority = Conf.local_prio;
        distance = Distance.min;
        parent   = None;
        rank     = 0 
      };
      peers = []
    }]

  let is_stable tree_set =
    List.for_all (fun x -> Tree.is_stable x) tree_set

  let parents tree_set = 
    List.map (fun tree -> Tree.get_parent tree) tree_set 
    |> Common.Option.flatten 
    |> Common.Option.get
    |> List.sort_uniq (Tree.Node.compare) 

  let min_dist tree_set =
    (List.fold_left (fun a b -> if a.local.distance < b.local.distance then a else b) (List.hd tree_set) tree_set).local.distance

  let next_tree tree_set =
    List.length tree_set

  let update_tree_set tree_set (node:Tree.Node.t) =
    let tree_set = match List.exists (fun tree -> tree.local.tree_nb = node.tree_nb) tree_set with
    | true -> tree_set
    | false ->
      {
        local =
          {
            node_id  = Conf.local_id;
            tree_nb  = node.tree_nb;
            distance = Distance.min;
            parent   = None;
            rank     = 0;
            priority = match (min_dist tree_set > Conf.max_dist) with
            | true -> Conf.local_prio
            | false -> Priority.min
          };
        peers = [];
      } :: tree_set in
    let tree_set =
      List.map (fun tree -> 
        if tree.local.tree_nb = node.tree_nb then Tree.update tree node else tree)
        tree_set in
    let tree_set = match is_stable tree_set && List.length tree_set < Conf.max_trees with
    | false -> tree_set
    | true -> match min_dist tree_set > Conf.max_dist with
      | false -> tree_set
      | true ->
        {
          local =
            {
              node_id  = Conf.local_id;
              tree_nb  = next_tree tree_set;
              distance = Distance.min;
              parent   = None;
              rank     = 0;
              priority = Conf.local_prio
            };
          peers = [];
        } :: tree_set in
    tree_set

  let delete_node tree_set node =
    List.map (fun x -> Tree.delete_node x node) tree_set

  let print tree_set =
    tree_set
    |> List.sort (fun a b -> compare a.local.tree_nb b.local.tree_nb)
    |> List.iter (fun tree -> printf "Tree nb %i:\n%!" tree.local.tree_nb; print tree)
end
