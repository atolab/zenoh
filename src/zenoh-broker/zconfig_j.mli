(* Auto-generated from "zconfig.atd" *)
[@@@ocaml.warning "-27-32-35-39"]

type udp_config = Zconfig_t.udp_config = {
  addr: string;
  port: int;
  multicast: string option;
  bufsize: int
}

type tcp_config = Zconfig_t.tcp_config = {
  addr: string;
  port: int;
  connection_backlog: int;
  bufsize: int
}

type transport_config = Zconfig_t.transport_config

type config = Zconfig_t.config = { transports: transport_config list }

val write_udp_config :
  Bi_outbuf.t -> udp_config -> unit
  (** Output a JSON value of type {!udp_config}. *)

val string_of_udp_config :
  ?len:int -> udp_config -> string
  (** Serialize a value of type {!udp_config}
      into a JSON string.
      @param len specifies the initial length
                 of the buffer used internally.
                 Default: 1024. *)

val read_udp_config :
  Yojson.Safe.lexer_state -> Lexing.lexbuf -> udp_config
  (** Input JSON data of type {!udp_config}. *)

val udp_config_of_string :
  string -> udp_config
  (** Deserialize JSON data of type {!udp_config}. *)

val write_tcp_config :
  Bi_outbuf.t -> tcp_config -> unit
  (** Output a JSON value of type {!tcp_config}. *)

val string_of_tcp_config :
  ?len:int -> tcp_config -> string
  (** Serialize a value of type {!tcp_config}
      into a JSON string.
      @param len specifies the initial length
                 of the buffer used internally.
                 Default: 1024. *)

val read_tcp_config :
  Yojson.Safe.lexer_state -> Lexing.lexbuf -> tcp_config
  (** Input JSON data of type {!tcp_config}. *)

val tcp_config_of_string :
  string -> tcp_config
  (** Deserialize JSON data of type {!tcp_config}. *)

val write_transport_config :
  Bi_outbuf.t -> transport_config -> unit
  (** Output a JSON value of type {!transport_config}. *)

val string_of_transport_config :
  ?len:int -> transport_config -> string
  (** Serialize a value of type {!transport_config}
      into a JSON string.
      @param len specifies the initial length
                 of the buffer used internally.
                 Default: 1024. *)

val read_transport_config :
  Yojson.Safe.lexer_state -> Lexing.lexbuf -> transport_config
  (** Input JSON data of type {!transport_config}. *)

val transport_config_of_string :
  string -> transport_config
  (** Deserialize JSON data of type {!transport_config}. *)

val write_config :
  Bi_outbuf.t -> config -> unit
  (** Output a JSON value of type {!config}. *)

val string_of_config :
  ?len:int -> config -> string
  (** Serialize a value of type {!config}
      into a JSON string.
      @param len specifies the initial length
                 of the buffer used internally.
                 Default: 1024. *)

val read_config :
  Yojson.Safe.lexer_state -> Lexing.lexbuf -> config
  (** Input JSON data of type {!config}. *)

val config_of_string :
  string -> config
  (** Deserialize JSON data of type {!config}. *)

