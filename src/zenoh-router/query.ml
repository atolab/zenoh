open Apero

module Qid = struct
    type t = (Abuf.t * Vle.t)
    let compare (pid1, qid1) (pid2, qid2) = 
        let c1 = compare (Vle.to_int qid1) (Vle.to_int qid2) in
        if c1 <> 0 then c1 else compare (Abuf.hexdump pid1) (Abuf.hexdump pid2)
end

type t = {
    srcFace : NetService.Id.t;
    fwdFaces : NetService.Id.t list;
}