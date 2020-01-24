
use zenoh_protocol::io::rwbuf::RWBuf;
use zenoh_protocol::proto::msg::Message;
use zenoh_protocol::core::*;
use std::sync::Arc;
use rand::*;

const BUFFER_SIZE: usize = 1024;
const PROPS_LENGTH: usize = 3;
const PROP_MAX_SIZE: usize = 64;


fn gen_rand_zint() -> ZInt {
  thread_rng().gen()
}

fn gen_rand_cid() -> Option<ZInt> {
  Some(gen_rand_zint())
}

fn gen_rand_buffer(max_size: usize) -> Vec<u8> {
  let len : usize = thread_rng().gen_range(1, max_size+1);
  let mut buf : Vec<u8> = Vec::with_capacity(len);
  buf.resize(len, 0);
  thread_rng().fill(buf.as_mut_slice());
  buf
}

fn gen_rand_props(len: usize, max_size: usize) -> Option<Arc<Vec<Property>>> {
    let mut props = Vec::with_capacity(len);
    for _ in 0..len {
      let key = gen_rand_zint();
      let value = gen_rand_buffer(max_size);
      props.push(Property{ key, value });
    }
    Some(Arc::new(props))
}

fn test_scout(with_decorators: bool, what: Option<ZInt>)
{
  let expected = if with_decorators {
    // Message::make_scout(what, gen_rand_cid(), gen_rand_props(PROPS_LENGTH, PROP_MAX_SIZE))
    Message::make_scout(what, gen_rand_cid(), None)
  } else {
    Message::make_scout(what, None, None)
  };
  let mut buf = RWBuf::new(BUFFER_SIZE);
  buf.write_message(&expected).unwrap();
  let msg = buf.read_message().unwrap();

  assert_eq!(expected.has_decorators(), msg.has_decorators());
}


#[test]
fn scout_tests() {
  test_scout(false, None);
  test_scout(true, None);
  test_scout(false, Some(gen_rand_zint()));
  test_scout(true, Some(gen_rand_zint()));
}

