
use zenoh_protocol::io::rwbuf::{RWBuf,OutOfBounds};
use zenoh_protocol::proto::msg::Message;
use zenoh_protocol::proto::msg_codec::*;


fn test_scout()
{
  let msg = Message::make_scout(None, None);
  let mut buf = RWBuf::new(64);
  write_message(&mut buf, &msg);
  assert_eq!(buf.readable(), 1);
}


#[test]
fn test_messages() {  
  test_scout();
}

