module Vle = struct
  include Int64
end

module Property = struct
  type t = string * bytes
  let create n v = (n, v)
end

module Properties = struct
  type t = Property.t list
  let empty = []
  let singleton p = [p]
  let add ps p = p::ps
  let find f ps  = List.find_opt f
  let get name ps = List.find_opt (fun (n, v) -> if n = name then true else false)
  let length ps = List.length ps
end


module UDPLocator = struct
  type t = {
    addr : string;
    port : int;
  }

  let to_string l =  "udp/" ^ l.addr ^ (string_of_int l.port)

  let is_multicast l = match (String.split_on_char '.' l.addr) with
    | h::tl ->
      let a = int_of_string h  in
      if a >= 224 && a <= 239 then true else false
    | _ -> false
end

module TCPLocator = struct
  type t = {
    addr : string;
    port : int;
  }

  let to_string l =  "tcp/" ^ l.addr ^ (string_of_int l.port)
end

module Locator = struct
  type t =
    | UDPLocator of UDPLocator.t
    | TCPLocator of TCPLocator.t
end

module Locators = struct
  type t = Locator.t list
  let empty = []
  let singleton l = [l]
  let add l ls = l::ls
  let length ls = List.length ls
end
