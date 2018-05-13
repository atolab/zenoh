
module UDPLocator = struct
  type t = {
    addr : string;
    port : int;
  }

  let to_string l =  "udp/" ^ l.addr ^ ":" ^ (string_of_int l.port)

  let of_string s =
    let inet_addr = String.split_on_char ':' List.(hd (rev (String.split_on_char '/' s))) in
    {addr= List.nth inet_addr 0; port=int_of_string (List.nth inet_addr 1)}

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

  let to_string l =  "tcp/" ^ l.addr ^ ":" ^ (string_of_int l.port)

  let of_string s =
    let inet_addr = String.split_on_char ':' List.(hd (rev (String.split_on_char '/' s))) in
    {addr=List.nth inet_addr 0; port=int_of_string (List.nth inet_addr 1)}
end

module Locator = struct
  type t =
    | UDPLocator of UDPLocator.t
    | TCPLocator of TCPLocator.t

  let to_string l =
    match l with
    | UDPLocator l -> UDPLocator.to_string l
    | TCPLocator l -> TCPLocator.to_string l

  let of_string s =
    match (List.hd (String.split_on_char '/' s)) with
    | trans when trans = "udp" -> UDPLocator(UDPLocator.of_string s)
    | trans when trans = "tcp" -> TCPLocator(TCPLocator.of_string s)
    | _ -> raise (Failure ("Unable to read locator from string \"" ^ s ^ "\"" ))
end

module Locators = struct
  type t = Locator.t list
  let empty = []
  let singleton l = [l]
  let add l ls = l::ls
  let length ls = List.length ls
end
