open Apero

module ZConduit = struct
  type t = {
    id : int;
    mutable rsn : Vle.t;
    mutable usn : Vle.t;
    
  }
  let make id = { id ; rsn = 0L; usn = 0L}
  let next_rsn c =
    let n = c.rsn in c.rsn <- Vle.add c.rsn 1L ; n

  let next_usn c =
    let n = c.usn in c.usn <- Vle.add c.usn 1L ; n

  let id c = c.id
end
