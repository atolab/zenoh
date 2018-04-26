module Vle = struct
  include Int64
  let max_byte = Int64.of_int 0x7f
  let shift_len = 7
  let max_bits = 64

  let to_list v =
    let rec to_list_rec v xs =
      if v <= max_byte then (v::xs)
      else
        begin
          let c = Int64.logand v max_byte in
          let u = Int64.shift_right v shift_len in
          to_list_rec u (c::xs)
        end
    in to_list_rec v []


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
  let find f ps = List.find_opt f ps
  let get name ps = List.find_opt (fun (n, _) -> if n = name then true else false) ps
  let length ps = List.length ps
end


module UDPLocator = struct
  type t = {
    addr : string;
    port : int;
  }

  let to_string l =  "udp/" ^ l.addr ^ (string_of_int l.port)

  let is_multicast l = match (String.split_on_char '.' l.addr) with
    | h::_ ->
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
