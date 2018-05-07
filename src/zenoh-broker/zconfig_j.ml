(* Auto-generated from "zconfig.atd" *)


type verbosity = Zconfig_t.verbosity

type udp_config = Zconfig_t.udp_config = {
  iface: string;
  port: int;
  multicast: string option
}

type tcp_config = Zconfig_t.tcp_config = {
  port: int;
  connection_backlog: int
}

type transport_config = Zconfig_t.transport_config

type config = Zconfig_t.config = {
  transports: transport_config list;
  log_level: verbosity
}

let write_verbosity = (
  fun ob sum ->
    match sum with
      | `INFO -> Bi_outbuf.add_string ob "<\"INFO\">"
      | `ERROR -> Bi_outbuf.add_string ob "<\"ERROR\">"
      | `DEBUG -> Bi_outbuf.add_string ob "<\"DEBUG\">"
)
let string_of_verbosity ?(len = 1024) x =
  let ob = Bi_outbuf.create len in
  write_verbosity ob x;
  Bi_outbuf.contents ob
let read_verbosity = (
  fun p lb ->
    Yojson.Safe.read_space p lb;
    
    match Yojson.Safe.start_any_variant p lb with
      | `Edgy_bracket -> (
          Yojson.Safe.read_space p lb;
          let f =
            fun s pos len ->
              if pos < 0 || len < 0 || pos + len > String.length s then
                invalid_arg "out-of-bounds substring position or length";
              try
                match len with
                  | 4 -> (
                      if String.unsafe_get s pos = 'I' && String.unsafe_get s (pos+1) = 'N' && String.unsafe_get s (pos+2) = 'F' && String.unsafe_get s (pos+3) = 'O' then (
                        0
                      )
                      else (
                        raise (Exit)
                      )
                    )
                  | 5 -> (
                      match String.unsafe_get s pos with
                        | 'D' -> (
                            if String.unsafe_get s (pos+1) = 'E' && String.unsafe_get s (pos+2) = 'B' && String.unsafe_get s (pos+3) = 'U' && String.unsafe_get s (pos+4) = 'G' then (
                              2
                            )
                            else (
                              raise (Exit)
                            )
                          )
                        | 'E' -> (
                            if String.unsafe_get s (pos+1) = 'R' && String.unsafe_get s (pos+2) = 'R' && String.unsafe_get s (pos+3) = 'O' && String.unsafe_get s (pos+4) = 'R' then (
                              1
                            )
                            else (
                              raise (Exit)
                            )
                          )
                        | _ -> (
                            raise (Exit)
                          )
                    )
                  | _ -> (
                      raise (Exit)
                    )
              with Exit -> (
                  Ag_oj_run.invalid_variant_tag p (String.sub s pos len)
                )
          in
          let i = Yojson.Safe.map_ident p f lb in
          match i with
            | 0 ->
              Yojson.Safe.read_space p lb;
              Yojson.Safe.read_gt p lb;
              `INFO
            | 1 ->
              Yojson.Safe.read_space p lb;
              Yojson.Safe.read_gt p lb;
              `ERROR
            | 2 ->
              Yojson.Safe.read_space p lb;
              Yojson.Safe.read_gt p lb;
              `DEBUG
            | _ -> (
                assert false
              )
        )
      | `Double_quote -> (
          let f =
            fun s pos len ->
              if pos < 0 || len < 0 || pos + len > String.length s then
                invalid_arg "out-of-bounds substring position or length";
              try
                match len with
                  | 4 -> (
                      if String.unsafe_get s pos = 'I' && String.unsafe_get s (pos+1) = 'N' && String.unsafe_get s (pos+2) = 'F' && String.unsafe_get s (pos+3) = 'O' then (
                        0
                      )
                      else (
                        raise (Exit)
                      )
                    )
                  | 5 -> (
                      match String.unsafe_get s pos with
                        | 'D' -> (
                            if String.unsafe_get s (pos+1) = 'E' && String.unsafe_get s (pos+2) = 'B' && String.unsafe_get s (pos+3) = 'U' && String.unsafe_get s (pos+4) = 'G' then (
                              2
                            )
                            else (
                              raise (Exit)
                            )
                          )
                        | 'E' -> (
                            if String.unsafe_get s (pos+1) = 'R' && String.unsafe_get s (pos+2) = 'R' && String.unsafe_get s (pos+3) = 'O' && String.unsafe_get s (pos+4) = 'R' then (
                              1
                            )
                            else (
                              raise (Exit)
                            )
                          )
                        | _ -> (
                            raise (Exit)
                          )
                    )
                  | _ -> (
                      raise (Exit)
                    )
              with Exit -> (
                  Ag_oj_run.invalid_variant_tag p (String.sub s pos len)
                )
          in
          let i = Yojson.Safe.map_string p f lb in
          match i with
            | 0 ->
              `INFO
            | 1 ->
              `ERROR
            | 2 ->
              `DEBUG
            | _ -> (
                assert false
              )
        )
      | `Square_bracket -> (
          Yojson.Safe.read_space p lb;
          let f =
            fun s pos len ->
              if pos < 0 || len < 0 || pos + len > String.length s then
                invalid_arg "out-of-bounds substring position or length";
              Ag_oj_run.invalid_variant_tag p (String.sub s pos len)
          in
          let i = Yojson.Safe.map_ident p f lb in
          match i with
            | _ -> (
                assert false
              )
        )
)
let verbosity_of_string s =
  read_verbosity (Yojson.Safe.init_lexer ()) (Lexing.from_string s)
let write__2 = (
  Ag_oj_run.write_option (
    Yojson.Safe.write_string
  )
)
let string_of__2 ?(len = 1024) x =
  let ob = Bi_outbuf.create len in
  write__2 ob x;
  Bi_outbuf.contents ob
let read__2 = (
  fun p lb ->
    Yojson.Safe.read_space p lb;
    
    match Yojson.Safe.start_any_variant p lb with
      | `Edgy_bracket -> (
          Yojson.Safe.read_space p lb;
          let f =
            fun s pos len ->
              if pos < 0 || len < 0 || pos + len > String.length s then
                invalid_arg "out-of-bounds substring position or length";
              try
                if len = 4 then (
                  match String.unsafe_get s pos with
                    | 'N' -> (
                        if String.unsafe_get s (pos+1) = 'o' && String.unsafe_get s (pos+2) = 'n' && String.unsafe_get s (pos+3) = 'e' then (
                          0
                        )
                        else (
                          raise (Exit)
                        )
                      )
                    | 'S' -> (
                        if String.unsafe_get s (pos+1) = 'o' && String.unsafe_get s (pos+2) = 'm' && String.unsafe_get s (pos+3) = 'e' then (
                          1
                        )
                        else (
                          raise (Exit)
                        )
                      )
                    | _ -> (
                        raise (Exit)
                      )
                )
                else (
                  raise (Exit)
                )
              with Exit -> (
                  Ag_oj_run.invalid_variant_tag p (String.sub s pos len)
                )
          in
          let i = Yojson.Safe.map_ident p f lb in
          match i with
            | 0 ->
              Yojson.Safe.read_space p lb;
              Yojson.Safe.read_gt p lb;
              (None : _ option)
            | 1 ->
              Ag_oj_run.read_until_field_value p lb;
              let x = (
                  Ag_oj_run.read_string
                ) p lb
              in
              Yojson.Safe.read_space p lb;
              Yojson.Safe.read_gt p lb;
              (Some x : _ option)
            | _ -> (
                assert false
              )
        )
      | `Double_quote -> (
          let f =
            fun s pos len ->
              if pos < 0 || len < 0 || pos + len > String.length s then
                invalid_arg "out-of-bounds substring position or length";
              try
                if len = 4 && String.unsafe_get s pos = 'N' && String.unsafe_get s (pos+1) = 'o' && String.unsafe_get s (pos+2) = 'n' && String.unsafe_get s (pos+3) = 'e' then (
                  0
                )
                else (
                  raise (Exit)
                )
              with Exit -> (
                  Ag_oj_run.invalid_variant_tag p (String.sub s pos len)
                )
          in
          let i = Yojson.Safe.map_string p f lb in
          match i with
            | 0 ->
              (None : _ option)
            | _ -> (
                assert false
              )
        )
      | `Square_bracket -> (
          Yojson.Safe.read_space p lb;
          let f =
            fun s pos len ->
              if pos < 0 || len < 0 || pos + len > String.length s then
                invalid_arg "out-of-bounds substring position or length";
              try
                if len = 4 && String.unsafe_get s pos = 'S' && String.unsafe_get s (pos+1) = 'o' && String.unsafe_get s (pos+2) = 'm' && String.unsafe_get s (pos+3) = 'e' then (
                  0
                )
                else (
                  raise (Exit)
                )
              with Exit -> (
                  Ag_oj_run.invalid_variant_tag p (String.sub s pos len)
                )
          in
          let i = Yojson.Safe.map_ident p f lb in
          match i with
            | 0 ->
              Yojson.Safe.read_space p lb;
              Yojson.Safe.read_comma p lb;
              Yojson.Safe.read_space p lb;
              let x = (
                  Ag_oj_run.read_string
                ) p lb
              in
              Yojson.Safe.read_space p lb;
              Yojson.Safe.read_rbr p lb;
              (Some x : _ option)
            | _ -> (
                assert false
              )
        )
)
let _2_of_string s =
  read__2 (Yojson.Safe.init_lexer ()) (Lexing.from_string s)
let write_udp_config : _ -> udp_config -> _ = (
  fun ob x ->
    Bi_outbuf.add_char ob '{';
    let is_first = ref true in
    if !is_first then
      is_first := false
    else
      Bi_outbuf.add_char ob ',';
    Bi_outbuf.add_string ob "\"iface\":";
    (
      Yojson.Safe.write_string
    )
      ob x.iface;
    if !is_first then
      is_first := false
    else
      Bi_outbuf.add_char ob ',';
    Bi_outbuf.add_string ob "\"port\":";
    (
      Yojson.Safe.write_int
    )
      ob x.port;
    if !is_first then
      is_first := false
    else
      Bi_outbuf.add_char ob ',';
    Bi_outbuf.add_string ob "\"multicast\":";
    (
      write__2
    )
      ob x.multicast;
    Bi_outbuf.add_char ob '}';
)
let string_of_udp_config ?(len = 1024) x =
  let ob = Bi_outbuf.create len in
  write_udp_config ob x;
  Bi_outbuf.contents ob
let read_udp_config = (
  fun p lb ->
    Yojson.Safe.read_space p lb;
    Yojson.Safe.read_lcurl p lb;
    let field_iface = ref (Obj.magic (Sys.opaque_identity 0.0)) in
    let field_port = ref (Obj.magic (Sys.opaque_identity 0.0)) in
    let field_multicast = ref (Obj.magic (Sys.opaque_identity 0.0)) in
    let bits0 = ref 0 in
    try
      Yojson.Safe.read_space p lb;
      Yojson.Safe.read_object_end lb;
      Yojson.Safe.read_space p lb;
      let f =
        fun s pos len ->
          if pos < 0 || len < 0 || pos + len > String.length s then
            invalid_arg "out-of-bounds substring position or length";
          match len with
            | 4 -> (
                if String.unsafe_get s pos = 'p' && String.unsafe_get s (pos+1) = 'o' && String.unsafe_get s (pos+2) = 'r' && String.unsafe_get s (pos+3) = 't' then (
                  1
                )
                else (
                  -1
                )
              )
            | 5 -> (
                if String.unsafe_get s pos = 'i' && String.unsafe_get s (pos+1) = 'f' && String.unsafe_get s (pos+2) = 'a' && String.unsafe_get s (pos+3) = 'c' && String.unsafe_get s (pos+4) = 'e' then (
                  0
                )
                else (
                  -1
                )
              )
            | 9 -> (
                if String.unsafe_get s pos = 'm' && String.unsafe_get s (pos+1) = 'u' && String.unsafe_get s (pos+2) = 'l' && String.unsafe_get s (pos+3) = 't' && String.unsafe_get s (pos+4) = 'i' && String.unsafe_get s (pos+5) = 'c' && String.unsafe_get s (pos+6) = 'a' && String.unsafe_get s (pos+7) = 's' && String.unsafe_get s (pos+8) = 't' then (
                  2
                )
                else (
                  -1
                )
              )
            | _ -> (
                -1
              )
      in
      let i = Yojson.Safe.map_ident p f lb in
      Ag_oj_run.read_until_field_value p lb;
      (
        match i with
          | 0 ->
            field_iface := (
              (
                Ag_oj_run.read_string
              ) p lb
            );
            bits0 := !bits0 lor 0x1;
          | 1 ->
            field_port := (
              (
                Ag_oj_run.read_int
              ) p lb
            );
            bits0 := !bits0 lor 0x2;
          | 2 ->
            field_multicast := (
              (
                read__2
              ) p lb
            );
            bits0 := !bits0 lor 0x4;
          | _ -> (
              Yojson.Safe.skip_json p lb
            )
      );
      while true do
        Yojson.Safe.read_space p lb;
        Yojson.Safe.read_object_sep p lb;
        Yojson.Safe.read_space p lb;
        let f =
          fun s pos len ->
            if pos < 0 || len < 0 || pos + len > String.length s then
              invalid_arg "out-of-bounds substring position or length";
            match len with
              | 4 -> (
                  if String.unsafe_get s pos = 'p' && String.unsafe_get s (pos+1) = 'o' && String.unsafe_get s (pos+2) = 'r' && String.unsafe_get s (pos+3) = 't' then (
                    1
                  )
                  else (
                    -1
                  )
                )
              | 5 -> (
                  if String.unsafe_get s pos = 'i' && String.unsafe_get s (pos+1) = 'f' && String.unsafe_get s (pos+2) = 'a' && String.unsafe_get s (pos+3) = 'c' && String.unsafe_get s (pos+4) = 'e' then (
                    0
                  )
                  else (
                    -1
                  )
                )
              | 9 -> (
                  if String.unsafe_get s pos = 'm' && String.unsafe_get s (pos+1) = 'u' && String.unsafe_get s (pos+2) = 'l' && String.unsafe_get s (pos+3) = 't' && String.unsafe_get s (pos+4) = 'i' && String.unsafe_get s (pos+5) = 'c' && String.unsafe_get s (pos+6) = 'a' && String.unsafe_get s (pos+7) = 's' && String.unsafe_get s (pos+8) = 't' then (
                    2
                  )
                  else (
                    -1
                  )
                )
              | _ -> (
                  -1
                )
        in
        let i = Yojson.Safe.map_ident p f lb in
        Ag_oj_run.read_until_field_value p lb;
        (
          match i with
            | 0 ->
              field_iface := (
                (
                  Ag_oj_run.read_string
                ) p lb
              );
              bits0 := !bits0 lor 0x1;
            | 1 ->
              field_port := (
                (
                  Ag_oj_run.read_int
                ) p lb
              );
              bits0 := !bits0 lor 0x2;
            | 2 ->
              field_multicast := (
                (
                  read__2
                ) p lb
              );
              bits0 := !bits0 lor 0x4;
            | _ -> (
                Yojson.Safe.skip_json p lb
              )
        );
      done;
      assert false;
    with Yojson.End_of_object -> (
        if !bits0 <> 0x7 then Ag_oj_run.missing_fields p [| !bits0 |] [| "iface"; "port"; "multicast" |];
        (
          {
            iface = !field_iface;
            port = !field_port;
            multicast = !field_multicast;
          }
         : udp_config)
      )
)
let udp_config_of_string s =
  read_udp_config (Yojson.Safe.init_lexer ()) (Lexing.from_string s)
let write_tcp_config : _ -> tcp_config -> _ = (
  fun ob x ->
    Bi_outbuf.add_char ob '{';
    let is_first = ref true in
    if !is_first then
      is_first := false
    else
      Bi_outbuf.add_char ob ',';
    Bi_outbuf.add_string ob "\"port\":";
    (
      Yojson.Safe.write_int
    )
      ob x.port;
    if !is_first then
      is_first := false
    else
      Bi_outbuf.add_char ob ',';
    Bi_outbuf.add_string ob "\"connection_backlog\":";
    (
      Yojson.Safe.write_int
    )
      ob x.connection_backlog;
    Bi_outbuf.add_char ob '}';
)
let string_of_tcp_config ?(len = 1024) x =
  let ob = Bi_outbuf.create len in
  write_tcp_config ob x;
  Bi_outbuf.contents ob
let read_tcp_config = (
  fun p lb ->
    Yojson.Safe.read_space p lb;
    Yojson.Safe.read_lcurl p lb;
    let field_port = ref (Obj.magic (Sys.opaque_identity 0.0)) in
    let field_connection_backlog = ref (Obj.magic (Sys.opaque_identity 0.0)) in
    let bits0 = ref 0 in
    try
      Yojson.Safe.read_space p lb;
      Yojson.Safe.read_object_end lb;
      Yojson.Safe.read_space p lb;
      let f =
        fun s pos len ->
          if pos < 0 || len < 0 || pos + len > String.length s then
            invalid_arg "out-of-bounds substring position or length";
          match len with
            | 4 -> (
                if String.unsafe_get s pos = 'p' && String.unsafe_get s (pos+1) = 'o' && String.unsafe_get s (pos+2) = 'r' && String.unsafe_get s (pos+3) = 't' then (
                  0
                )
                else (
                  -1
                )
              )
            | 18 -> (
                if String.unsafe_get s pos = 'c' && String.unsafe_get s (pos+1) = 'o' && String.unsafe_get s (pos+2) = 'n' && String.unsafe_get s (pos+3) = 'n' && String.unsafe_get s (pos+4) = 'e' && String.unsafe_get s (pos+5) = 'c' && String.unsafe_get s (pos+6) = 't' && String.unsafe_get s (pos+7) = 'i' && String.unsafe_get s (pos+8) = 'o' && String.unsafe_get s (pos+9) = 'n' && String.unsafe_get s (pos+10) = '_' && String.unsafe_get s (pos+11) = 'b' && String.unsafe_get s (pos+12) = 'a' && String.unsafe_get s (pos+13) = 'c' && String.unsafe_get s (pos+14) = 'k' && String.unsafe_get s (pos+15) = 'l' && String.unsafe_get s (pos+16) = 'o' && String.unsafe_get s (pos+17) = 'g' then (
                  1
                )
                else (
                  -1
                )
              )
            | _ -> (
                -1
              )
      in
      let i = Yojson.Safe.map_ident p f lb in
      Ag_oj_run.read_until_field_value p lb;
      (
        match i with
          | 0 ->
            field_port := (
              (
                Ag_oj_run.read_int
              ) p lb
            );
            bits0 := !bits0 lor 0x1;
          | 1 ->
            field_connection_backlog := (
              (
                Ag_oj_run.read_int
              ) p lb
            );
            bits0 := !bits0 lor 0x2;
          | _ -> (
              Yojson.Safe.skip_json p lb
            )
      );
      while true do
        Yojson.Safe.read_space p lb;
        Yojson.Safe.read_object_sep p lb;
        Yojson.Safe.read_space p lb;
        let f =
          fun s pos len ->
            if pos < 0 || len < 0 || pos + len > String.length s then
              invalid_arg "out-of-bounds substring position or length";
            match len with
              | 4 -> (
                  if String.unsafe_get s pos = 'p' && String.unsafe_get s (pos+1) = 'o' && String.unsafe_get s (pos+2) = 'r' && String.unsafe_get s (pos+3) = 't' then (
                    0
                  )
                  else (
                    -1
                  )
                )
              | 18 -> (
                  if String.unsafe_get s pos = 'c' && String.unsafe_get s (pos+1) = 'o' && String.unsafe_get s (pos+2) = 'n' && String.unsafe_get s (pos+3) = 'n' && String.unsafe_get s (pos+4) = 'e' && String.unsafe_get s (pos+5) = 'c' && String.unsafe_get s (pos+6) = 't' && String.unsafe_get s (pos+7) = 'i' && String.unsafe_get s (pos+8) = 'o' && String.unsafe_get s (pos+9) = 'n' && String.unsafe_get s (pos+10) = '_' && String.unsafe_get s (pos+11) = 'b' && String.unsafe_get s (pos+12) = 'a' && String.unsafe_get s (pos+13) = 'c' && String.unsafe_get s (pos+14) = 'k' && String.unsafe_get s (pos+15) = 'l' && String.unsafe_get s (pos+16) = 'o' && String.unsafe_get s (pos+17) = 'g' then (
                    1
                  )
                  else (
                    -1
                  )
                )
              | _ -> (
                  -1
                )
        in
        let i = Yojson.Safe.map_ident p f lb in
        Ag_oj_run.read_until_field_value p lb;
        (
          match i with
            | 0 ->
              field_port := (
                (
                  Ag_oj_run.read_int
                ) p lb
              );
              bits0 := !bits0 lor 0x1;
            | 1 ->
              field_connection_backlog := (
                (
                  Ag_oj_run.read_int
                ) p lb
              );
              bits0 := !bits0 lor 0x2;
            | _ -> (
                Yojson.Safe.skip_json p lb
              )
        );
      done;
      assert false;
    with Yojson.End_of_object -> (
        if !bits0 <> 0x3 then Ag_oj_run.missing_fields p [| !bits0 |] [| "port"; "connection_backlog" |];
        (
          {
            port = !field_port;
            connection_backlog = !field_connection_backlog;
          }
         : tcp_config)
      )
)
let tcp_config_of_string s =
  read_tcp_config (Yojson.Safe.init_lexer ()) (Lexing.from_string s)
let write_transport_config = (
  fun ob sum ->
    match sum with
      | `TCPConfig x ->
        Bi_outbuf.add_string ob "<\"TCPConfig\":";
        (
          write_tcp_config
        ) ob x;
        Bi_outbuf.add_char ob '>'
      | `UDPConfig x ->
        Bi_outbuf.add_string ob "<\"UDPConfig\":";
        (
          write_udp_config
        ) ob x;
        Bi_outbuf.add_char ob '>'
)
let string_of_transport_config ?(len = 1024) x =
  let ob = Bi_outbuf.create len in
  write_transport_config ob x;
  Bi_outbuf.contents ob
let read_transport_config = (
  fun p lb ->
    Yojson.Safe.read_space p lb;
    
    match Yojson.Safe.start_any_variant p lb with
      | `Edgy_bracket -> (
          Yojson.Safe.read_space p lb;
          let f =
            fun s pos len ->
              if pos < 0 || len < 0 || pos + len > String.length s then
                invalid_arg "out-of-bounds substring position or length";
              try
                if len = 9 then (
                  match String.unsafe_get s pos with
                    | 'T' -> (
                        if String.unsafe_get s (pos+1) = 'C' && String.unsafe_get s (pos+2) = 'P' && String.unsafe_get s (pos+3) = 'C' && String.unsafe_get s (pos+4) = 'o' && String.unsafe_get s (pos+5) = 'n' && String.unsafe_get s (pos+6) = 'f' && String.unsafe_get s (pos+7) = 'i' && String.unsafe_get s (pos+8) = 'g' then (
                          0
                        )
                        else (
                          raise (Exit)
                        )
                      )
                    | 'U' -> (
                        if String.unsafe_get s (pos+1) = 'D' && String.unsafe_get s (pos+2) = 'P' && String.unsafe_get s (pos+3) = 'C' && String.unsafe_get s (pos+4) = 'o' && String.unsafe_get s (pos+5) = 'n' && String.unsafe_get s (pos+6) = 'f' && String.unsafe_get s (pos+7) = 'i' && String.unsafe_get s (pos+8) = 'g' then (
                          1
                        )
                        else (
                          raise (Exit)
                        )
                      )
                    | _ -> (
                        raise (Exit)
                      )
                )
                else (
                  raise (Exit)
                )
              with Exit -> (
                  Ag_oj_run.invalid_variant_tag p (String.sub s pos len)
                )
          in
          let i = Yojson.Safe.map_ident p f lb in
          match i with
            | 0 ->
              Ag_oj_run.read_until_field_value p lb;
              let x = (
                  read_tcp_config
                ) p lb
              in
              Yojson.Safe.read_space p lb;
              Yojson.Safe.read_gt p lb;
              `TCPConfig x
            | 1 ->
              Ag_oj_run.read_until_field_value p lb;
              let x = (
                  read_udp_config
                ) p lb
              in
              Yojson.Safe.read_space p lb;
              Yojson.Safe.read_gt p lb;
              `UDPConfig x
            | _ -> (
                assert false
              )
        )
      | `Double_quote -> (
          let f =
            fun s pos len ->
              if pos < 0 || len < 0 || pos + len > String.length s then
                invalid_arg "out-of-bounds substring position or length";
              Ag_oj_run.invalid_variant_tag p (String.sub s pos len)
          in
          let i = Yojson.Safe.map_string p f lb in
          match i with
            | _ -> (
                assert false
              )
        )
      | `Square_bracket -> (
          Yojson.Safe.read_space p lb;
          let f =
            fun s pos len ->
              if pos < 0 || len < 0 || pos + len > String.length s then
                invalid_arg "out-of-bounds substring position or length";
              try
                if len = 9 then (
                  match String.unsafe_get s pos with
                    | 'T' -> (
                        if String.unsafe_get s (pos+1) = 'C' && String.unsafe_get s (pos+2) = 'P' && String.unsafe_get s (pos+3) = 'C' && String.unsafe_get s (pos+4) = 'o' && String.unsafe_get s (pos+5) = 'n' && String.unsafe_get s (pos+6) = 'f' && String.unsafe_get s (pos+7) = 'i' && String.unsafe_get s (pos+8) = 'g' then (
                          0
                        )
                        else (
                          raise (Exit)
                        )
                      )
                    | 'U' -> (
                        if String.unsafe_get s (pos+1) = 'D' && String.unsafe_get s (pos+2) = 'P' && String.unsafe_get s (pos+3) = 'C' && String.unsafe_get s (pos+4) = 'o' && String.unsafe_get s (pos+5) = 'n' && String.unsafe_get s (pos+6) = 'f' && String.unsafe_get s (pos+7) = 'i' && String.unsafe_get s (pos+8) = 'g' then (
                          1
                        )
                        else (
                          raise (Exit)
                        )
                      )
                    | _ -> (
                        raise (Exit)
                      )
                )
                else (
                  raise (Exit)
                )
              with Exit -> (
                  Ag_oj_run.invalid_variant_tag p (String.sub s pos len)
                )
          in
          let i = Yojson.Safe.map_ident p f lb in
          match i with
            | 0 ->
              Yojson.Safe.read_space p lb;
              Yojson.Safe.read_comma p lb;
              Yojson.Safe.read_space p lb;
              let x = (
                  read_tcp_config
                ) p lb
              in
              Yojson.Safe.read_space p lb;
              Yojson.Safe.read_rbr p lb;
              `TCPConfig x
            | 1 ->
              Yojson.Safe.read_space p lb;
              Yojson.Safe.read_comma p lb;
              Yojson.Safe.read_space p lb;
              let x = (
                  read_udp_config
                ) p lb
              in
              Yojson.Safe.read_space p lb;
              Yojson.Safe.read_rbr p lb;
              `UDPConfig x
            | _ -> (
                assert false
              )
        )
)
let transport_config_of_string s =
  read_transport_config (Yojson.Safe.init_lexer ()) (Lexing.from_string s)
let write__1 = (
  Ag_oj_run.write_list (
    write_transport_config
  )
)
let string_of__1 ?(len = 1024) x =
  let ob = Bi_outbuf.create len in
  write__1 ob x;
  Bi_outbuf.contents ob
let read__1 = (
  Ag_oj_run.read_list (
    read_transport_config
  )
)
let _1_of_string s =
  read__1 (Yojson.Safe.init_lexer ()) (Lexing.from_string s)
let write_config : _ -> config -> _ = (
  fun ob x ->
    Bi_outbuf.add_char ob '{';
    let is_first = ref true in
    if !is_first then
      is_first := false
    else
      Bi_outbuf.add_char ob ',';
    Bi_outbuf.add_string ob "\"transports\":";
    (
      write__1
    )
      ob x.transports;
    if !is_first then
      is_first := false
    else
      Bi_outbuf.add_char ob ',';
    Bi_outbuf.add_string ob "\"log_level\":";
    (
      write_verbosity
    )
      ob x.log_level;
    Bi_outbuf.add_char ob '}';
)
let string_of_config ?(len = 1024) x =
  let ob = Bi_outbuf.create len in
  write_config ob x;
  Bi_outbuf.contents ob
let read_config = (
  fun p lb ->
    Yojson.Safe.read_space p lb;
    Yojson.Safe.read_lcurl p lb;
    let field_transports = ref (Obj.magic (Sys.opaque_identity 0.0)) in
    let field_log_level = ref (Obj.magic (Sys.opaque_identity 0.0)) in
    let bits0 = ref 0 in
    try
      Yojson.Safe.read_space p lb;
      Yojson.Safe.read_object_end lb;
      Yojson.Safe.read_space p lb;
      let f =
        fun s pos len ->
          if pos < 0 || len < 0 || pos + len > String.length s then
            invalid_arg "out-of-bounds substring position or length";
          match len with
            | 9 -> (
                if String.unsafe_get s pos = 'l' && String.unsafe_get s (pos+1) = 'o' && String.unsafe_get s (pos+2) = 'g' && String.unsafe_get s (pos+3) = '_' && String.unsafe_get s (pos+4) = 'l' && String.unsafe_get s (pos+5) = 'e' && String.unsafe_get s (pos+6) = 'v' && String.unsafe_get s (pos+7) = 'e' && String.unsafe_get s (pos+8) = 'l' then (
                  1
                )
                else (
                  -1
                )
              )
            | 10 -> (
                if String.unsafe_get s pos = 't' && String.unsafe_get s (pos+1) = 'r' && String.unsafe_get s (pos+2) = 'a' && String.unsafe_get s (pos+3) = 'n' && String.unsafe_get s (pos+4) = 's' && String.unsafe_get s (pos+5) = 'p' && String.unsafe_get s (pos+6) = 'o' && String.unsafe_get s (pos+7) = 'r' && String.unsafe_get s (pos+8) = 't' && String.unsafe_get s (pos+9) = 's' then (
                  0
                )
                else (
                  -1
                )
              )
            | _ -> (
                -1
              )
      in
      let i = Yojson.Safe.map_ident p f lb in
      Ag_oj_run.read_until_field_value p lb;
      (
        match i with
          | 0 ->
            field_transports := (
              (
                read__1
              ) p lb
            );
            bits0 := !bits0 lor 0x1;
          | 1 ->
            field_log_level := (
              (
                read_verbosity
              ) p lb
            );
            bits0 := !bits0 lor 0x2;
          | _ -> (
              Yojson.Safe.skip_json p lb
            )
      );
      while true do
        Yojson.Safe.read_space p lb;
        Yojson.Safe.read_object_sep p lb;
        Yojson.Safe.read_space p lb;
        let f =
          fun s pos len ->
            if pos < 0 || len < 0 || pos + len > String.length s then
              invalid_arg "out-of-bounds substring position or length";
            match len with
              | 9 -> (
                  if String.unsafe_get s pos = 'l' && String.unsafe_get s (pos+1) = 'o' && String.unsafe_get s (pos+2) = 'g' && String.unsafe_get s (pos+3) = '_' && String.unsafe_get s (pos+4) = 'l' && String.unsafe_get s (pos+5) = 'e' && String.unsafe_get s (pos+6) = 'v' && String.unsafe_get s (pos+7) = 'e' && String.unsafe_get s (pos+8) = 'l' then (
                    1
                  )
                  else (
                    -1
                  )
                )
              | 10 -> (
                  if String.unsafe_get s pos = 't' && String.unsafe_get s (pos+1) = 'r' && String.unsafe_get s (pos+2) = 'a' && String.unsafe_get s (pos+3) = 'n' && String.unsafe_get s (pos+4) = 's' && String.unsafe_get s (pos+5) = 'p' && String.unsafe_get s (pos+6) = 'o' && String.unsafe_get s (pos+7) = 'r' && String.unsafe_get s (pos+8) = 't' && String.unsafe_get s (pos+9) = 's' then (
                    0
                  )
                  else (
                    -1
                  )
                )
              | _ -> (
                  -1
                )
        in
        let i = Yojson.Safe.map_ident p f lb in
        Ag_oj_run.read_until_field_value p lb;
        (
          match i with
            | 0 ->
              field_transports := (
                (
                  read__1
                ) p lb
              );
              bits0 := !bits0 lor 0x1;
            | 1 ->
              field_log_level := (
                (
                  read_verbosity
                ) p lb
              );
              bits0 := !bits0 lor 0x2;
            | _ -> (
                Yojson.Safe.skip_json p lb
              )
        );
      done;
      assert false;
    with Yojson.End_of_object -> (
        if !bits0 <> 0x3 then Ag_oj_run.missing_fields p [| !bits0 |] [| "transports"; "log_level" |];
        (
          {
            transports = !field_transports;
            log_level = !field_log_level;
          }
         : config)
      )
)
let config_of_string s =
  read_config (Yojson.Safe.init_lexer ()) (Lexing.from_string s)
