(* Auto-generated from "zconfig.atd" *)


type verbosity = [ `INFO | `ERROR | `DEBUG ]

type udp_config = { iface: string; port: int; multicast: string option }

type tcp_config = { port: int; connection_backlog: int }

type transport_config = [
    `TCPConfig of tcp_config
  | `UDPConfig of udp_config
]

type config = { transports: transport_config list; log_level: verbosity }
