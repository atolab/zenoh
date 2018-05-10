# Consistency

## Message types

- Messages declared in zenoh.mli/ml shoud always use IOBuf as opposed to
  expose Lwt_bytes

- variable names should avoid using camel case.

## Marshaller

- Replace the use of (read/write)byte_seq with (read|write)io_buf in marshaller.ml

- Be consistent on return types, notice the for messages we are not returning
  the Message.t but for Declarations we are... 
