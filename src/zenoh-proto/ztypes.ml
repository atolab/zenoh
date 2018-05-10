open Apero

module Error = struct
  type kind = NoMsg | Msg of string | Code of int | Pos of (string * int * int * int) | Loc of string


  type e = ..
  type e +=
    | OutOfBounds of kind
    | OutOfRange of kind
    | IOError of kind
    | InvalidFormat of kind
    | ProtocolError of kind
    | InvalidSession of kind
    | NotImplemented
    | ErrorStack of e list


end

module Result = Result.Make(Error)

module Vle = struct
  include Int64
  [@@deriving sexp]

  let of_char = Int64.of_int <.> int_of_char

  let byte_mask =  0x7fL
  let more_bytes_flag = 0x80L
  let shift_len = 7
  let max_bits = 64
  let max_bytes = 10

  let to_list v =
    let to_list_negative v =
      let rec to_list_negative_rec v xs n =
        if n < max_bytes then
          begin
            let mv = Int64.logand v byte_mask in
            let sv = Int64.shift_right v shift_len in
            to_list_negative_rec sv (mv::xs) (n+1)
          end
        else List.rev (1L :: xs)
      in to_list_negative_rec v [] 1
    in
    let to_list_positive v =
      let rec to_list_positive_rec v xs =
        if v <= byte_mask then List.rev (v::xs)
        else
          begin
            let mv = Int64.logand v byte_mask in
            let sv = Int64.shift_right v shift_len in
            to_list_positive_rec sv (mv::xs)
          end
      in to_list_positive_rec v []
    in
    if v >= Int64.zero then to_list_positive v
    else to_list_negative v


  let from_list xs =
    if List.length xs > max_bytes then Result.fail Error.(OutOfRange NoMsg)
    else
      begin
        let rec from_list_rec v xs n =
          if n <= max_bytes then
            match xs with
            | y::ys ->
              let nv = Int64.logor (Int64.shift_left y (n* shift_len)) v in
              from_list_rec nv ys (n+1)
            | [] -> Result.ok v
          else Result.fail Error.(OutOfRange NoMsg)
        in from_list_rec Int64.zero xs 0
      end
end

module Property = struct
  type t = Vle.t * Lwt_bytes.t
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
