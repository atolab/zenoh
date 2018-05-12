
## Message types

- Messages declared in zenoh.mli/ml shoud always use IOBuf as opposed to
  expose Lwt_bytes

- variable names should avoid using camel case.

## Small composable functions

- do not repeat code, use fuctions and function composition, as an example
replace the use of:
; match ((int_of_char header) land (int_of_char Flags.pFlag)) with
  | 0x00 -> Result.ok (Scout.create mask [], buf)
  | _ -> Result.do_
         ; (props, buf) <-- read_prop_seq buf
         ; Result.ok (Scout.create mask props, buf)

with the new function write_properties

## Properties
Complete properties Id declaration (See zenoh.mli/ml)

## Marshaller

- Replace the use of (read/write)byte_seq with (read|write)io_buf in marshaller.ml

- Be consistent on return types, notice the for messages we are not returning
  the Message.t but for Declarations we are...

## Debugging

- Properly use Lwt_log to create the right sections and levels

- Use Fmt or similar library to provide a nice to_string for all messages
