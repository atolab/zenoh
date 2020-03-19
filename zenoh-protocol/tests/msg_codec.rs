
use zenoh_protocol::io::{ArcSlice, WBuf};
use zenoh_protocol::proto::*;
use zenoh_protocol::link::Locator;
use zenoh_protocol::core::*;
use std::sync::Arc;
use rand::*;

const PROPS_LENGTH: usize = 3;
const PID_MAX_SIZE: usize = 128;
const PROP_MAX_SIZE: usize = 64;
const MAX_INFO_SIZE: usize = 128;
const MAX_PAYLOAD_SIZE: usize = 256;

macro_rules! gen {
  ($name:ty) => (thread_rng().gen::<$name>());
}

fn gen_cid() -> Option<ZInt> {
  Some(gen!(ZInt))
}

fn gen_buffer(max_size: usize) -> Vec<u8> {
  let len : usize = thread_rng().gen_range(1, max_size+1);
  let mut buf : Vec<u8> = Vec::with_capacity(len);
  buf.resize(len, 0);
  thread_rng().fill(buf.as_mut_slice());
  buf
}

fn gen_pid() -> PeerId {
  PeerId{ id: gen_buffer(PID_MAX_SIZE) }
}

fn gen_props(len: usize, max_size: usize) -> Option<Arc<Vec<Property>>> {
    let mut props = Vec::with_capacity(len);
    for _ in 0..len {
      let key = gen!(ZInt);
      let value = gen_buffer(max_size);
      props.push(Property{ key, value });
    }
    Some(Arc::new(props))
}

fn gen_reply_context(is_final: bool) -> ReplyContext {
  let qid = gen!(ZInt);
  let source = if thread_rng().gen::<bool>() { ReplySource::Eval } else { ReplySource::Storage };
  let replier_id = if is_final { None } else { Some(gen_pid()) };
  ReplyContext::make(qid, source, replier_id)
}

fn gen_declarations() -> Vec<Declaration> {
  use zenoh_protocol::proto::Declaration::*;
  let mut decls = Vec::new();
  decls.push(Resource         { rid: gen!(ZInt), key: gen_key() });
  decls.push(ForgetResource   { rid: gen!(ZInt) });
  decls.push(Publisher        { key: gen_key() });
  decls.push(ForgetPublisher  { key: gen_key() });
  decls.push(Subscriber       { key: gen_key(), info:
    SubInfo { 
      reliability: Reliability::Reliable,
      mode: SubMode::Push,
      period: None,
    } });
  decls.push(Subscriber       { key: gen_key(), info:
    SubInfo { 
      reliability: Reliability::BestEffort,
      mode: SubMode::Pull,
      period: None,
    } });
  decls.push(Subscriber       { key: gen_key(), info:
    SubInfo { 
      reliability: Reliability::Reliable,
      mode: SubMode::Pull,
      period: Some(Period {origin: gen!(ZInt), period: gen!(ZInt), duration: gen!(ZInt)} ),
    } });
  decls.push(Subscriber       { key: gen_key(), info:
    SubInfo { 
      reliability: Reliability::BestEffort,
      mode: SubMode::Push,
      period: Some(Period {origin: gen!(ZInt), period: gen!(ZInt), duration: gen!(ZInt)} ),
    } });
  decls.push(ForgetSubscriber { key: gen_key() });
  decls.push(Storage          { key: gen_key() });
  decls.push(ForgetStorage    { key: gen_key() });
  decls.push(Eval             { key: gen_key() });
  decls.push(ForgetEval       { key: gen_key() });  
  decls
}

fn gen_key() -> ResKey {
  match thread_rng().gen_range::<u8, u8, u8>(0, 3) {
    0 => ResKey::from(gen!(ZInt)),
    1 => ResKey::from("my_resource".to_string()),
    _ => ResKey::from((gen!(ZInt), "my_resource".to_string()))
  }
}

fn gen_query_target() -> QueryTarget {
  let storage = gen_target();
  let eval = gen_target();
  QueryTarget{ storage, eval }
}

fn gen_target() -> Target {
  match thread_rng().gen_range::<u8, u8, u8>(0, 4) {
    0 => Target::BestMatching,
    1 => Target::Complete { n:3 },
    2 => Target::All,
    _ => Target::None
  }
}

fn gen_consolidation() -> QueryConsolidation {
  match thread_rng().gen_range::<u8, u8, u8>(0, 3) {
    0 => QueryConsolidation::None,
    1 => QueryConsolidation::LastBroker,
    _ => QueryConsolidation::Incremental
  }
}

fn test_write_read_message(msg: Message)
{
  let mut buf = WBuf::new(64);
  println!("Write message: {:?}", msg);
  buf.write_message(&msg);
  println!("Read message from: {:?}", buf);
  let result = buf.as_rbuf().read_message().unwrap();
  println!("Message read: {:?}", result);

  assert_eq!(msg, result);
}


fn test_scout(with_decorators: bool, what: Option<ZInt>)
{
  let msg = if with_decorators {
    Message::make_scout(what, gen_cid(), gen_props(PROPS_LENGTH, PROP_MAX_SIZE))
  } else {
    Message::make_scout(what, None, None)
  };
  test_write_read_message(msg);
}

#[test]
fn scout_tests() {
  test_scout(false, None);
  test_scout(true, None);
  test_scout(false, Some(gen!(ZInt)));
  test_scout(true, Some(gen!(ZInt)));
}

fn test_hello(with_decorators: bool, whatami: WhatAmI, locators: Option<Vec<Locator>>)
{
  let msg = if with_decorators {
    Message::make_hello(whatami, locators, gen_cid(), gen_props(PROPS_LENGTH, PROP_MAX_SIZE))
  } else {
    Message::make_hello(whatami, locators, None, None)
  };
  test_write_read_message(msg);
}

#[test]
fn hello_tests() {
  let locators: Vec<Locator> = vec!["tcp/1.2.3.4:1234".parse().unwrap(), "tcp/4.5.6.7:4567".parse().unwrap()];
  test_hello(false, WhatAmI::Broker, None);
  test_hello(true, WhatAmI::Broker, None);
  test_hello(false, WhatAmI::Client, Some(locators.clone()));
  test_hello(true, WhatAmI::Client, Some(locators.clone()));
}

fn test_open(with_decorators: bool, version: u8, whatami: WhatAmI, pid: PeerId, lease: ZInt, locators: Option<Vec<Locator>>)
{
  let msg = if with_decorators {
    Message::make_open(version, whatami, pid, lease, locators, gen_cid(), gen_props(PROPS_LENGTH, PROP_MAX_SIZE))
  } else {
    Message::make_open(version, whatami, pid, lease, locators, None, None)
  };
  test_write_read_message(msg);
}

#[test]
fn open_tests() {
  let locators = vec!["tcp/1.2.3.4:1234".parse().unwrap(), "tcp/4.5.6.7:4567".parse().unwrap()];
  test_open(false, gen!(u8), WhatAmI::Broker, gen_pid(), gen!(ZInt), None);
  test_open(true, gen!(u8), WhatAmI::Broker, gen_pid(), gen!(ZInt), None);
  test_open(false, gen!(u8), WhatAmI::Client, gen_pid(), gen!(ZInt), Some(locators.clone()));
  test_open(true, gen!(u8), WhatAmI::Client, gen_pid(), gen!(ZInt), Some(locators.clone()));
}

fn test_accept(with_decorators: bool, whatami: WhatAmI, opid: PeerId, apid: PeerId, lease: ZInt)
{
  let msg = if with_decorators {
    Message::make_accept(whatami, opid, apid, lease, gen_cid(), gen_props(PROPS_LENGTH, PROP_MAX_SIZE))
  } else {
    Message::make_accept(whatami, opid, apid, lease, None, None)
  };
  test_write_read_message(msg);
}

#[test]
fn accept_tests() {
  test_accept(false, WhatAmI::Broker, gen_pid(), gen_pid(), gen!(ZInt));
  test_accept(true, WhatAmI::Broker, gen_pid(), gen_pid(), gen!(ZInt));
  test_accept(false, WhatAmI::Client, gen_pid(), gen_pid(), gen!(ZInt));
  test_accept(true, WhatAmI::Client, gen_pid(), gen_pid(), gen!(ZInt));
}

fn test_close(with_decorators: bool, pid: Option<PeerId>, reason: u8)
{
  let msg = if with_decorators {
    Message::make_close(pid, reason, gen_cid(), gen_props(PROPS_LENGTH, PROP_MAX_SIZE))
  } else {
    Message::make_close(pid, reason, None, None)
  };
  test_write_read_message(msg);
}

#[test]
fn close_tests() {
  test_close(false, None, gen!(u8));
  test_close(true, None, gen!(u8));
  test_close(false, Some(gen_pid()), gen!(u8));
  test_close(true, Some(gen_pid()), gen!(u8));
}

fn test_keep_alive(with_decorators: bool, pid: Option<PeerId>, reply_context: Option<ReplyContext>)
{
  let msg = if with_decorators {
    Message::make_keep_alive(pid, reply_context, gen_cid(), gen_props(PROPS_LENGTH, PROP_MAX_SIZE))
  } else {
    Message::make_keep_alive(pid, reply_context, None, None)
  };
  test_write_read_message(msg);
}

#[test]
fn keep_alive_tests() {
  test_keep_alive(false, None, None);
  test_keep_alive(true, None, None);
  test_keep_alive(false, Some(gen_pid()), Some(gen_reply_context(false)));
  test_keep_alive(true, Some(gen_pid()), Some(gen_reply_context(false)));
  test_keep_alive(false, Some(gen_pid()), Some(gen_reply_context(true)));
  test_keep_alive(true, Some(gen_pid()), Some(gen_reply_context(true)));
}

fn test_declare(with_decorators: bool, sn: ZInt, declarations: Vec<Declaration>)
{
  let msg = if with_decorators {
    Message::make_declare(sn, declarations, gen_cid(), gen_props(PROPS_LENGTH, PROP_MAX_SIZE))
  } else {
    Message::make_declare(sn, declarations, None, None)
  };
  test_write_read_message(msg);
}

#[test]
fn declare_tests() {
  test_declare(false, gen!(ZInt), gen_declarations());
  test_declare(true, gen!(ZInt), gen_declarations());
}

fn test_data(with_decorators: bool,
  kind: MessageKind,
  reliable: bool,
  sn: ZInt,
  key: ResKey,
  info: Option<ArcSlice>,
  payload: ArcSlice,
  reply_context: Option<ReplyContext>)
{
  let msg = if with_decorators {
    Message::make_data(kind, reliable, sn, key, info, payload, reply_context, gen_cid(), gen_props(PROPS_LENGTH, PROP_MAX_SIZE))
  } else {
    Message::make_data(kind, reliable, sn, key, info, payload, reply_context, None, None)
  };
  test_write_read_message(msg);
}

#[test]
fn data_tests() {
  use MessageKind::*;
  let info = ArcSlice::from(gen_buffer(MAX_INFO_SIZE));
  let payload = ArcSlice::from(gen_buffer(MAX_PAYLOAD_SIZE));
  test_data(false, FullMessage, gen!(bool), gen!(ZInt), gen_key(), None, payload.clone(), None);
  test_data(true, FullMessage, gen!(bool), gen!(ZInt), gen_key(), None, payload.clone(), None);
  test_data(false, FullMessage, gen!(bool), gen!(ZInt), gen_key(), Some(info.clone()), payload.clone(), Some(gen_reply_context(false)));
  test_data(true, FullMessage, gen!(bool), gen!(ZInt), gen_key(), Some(info.clone()), payload.clone(), Some(gen_reply_context(false)));
  test_data(false, FullMessage, gen!(bool), gen!(ZInt), gen_key(), Some(info.clone()), payload.clone(), Some(gen_reply_context(true)));
  test_data(true, FullMessage, gen!(bool), gen!(ZInt), gen_key(), Some(info.clone()), payload.clone(), Some(gen_reply_context(true)));

  test_data(true, FirstFragment{n: None}, gen!(bool), gen!(ZInt), gen_key(), None, payload.clone(), None);
  test_data(true, FirstFragment{n: Some(gen!(ZInt))}, gen!(bool), gen!(ZInt), gen_key(), None, payload.clone(), None);
  test_data(true, InbetweenFragment, gen!(bool), gen!(ZInt), gen_key(), None, payload.clone(), None);
  test_data(true, LastFragment, gen!(bool), gen!(ZInt), gen_key(), None, payload.clone(), None);
}

fn test_pull(with_decorators: bool, is_final: bool, sn: ZInt, key: ResKey, pull_id: ZInt, max_samples: Option<ZInt>)
{
  let msg = if with_decorators {
    Message::make_pull(is_final, sn, key, pull_id, max_samples, gen_cid(), gen_props(PROPS_LENGTH, PROP_MAX_SIZE))
  } else {
    Message::make_pull(is_final, sn, key, pull_id, max_samples, None, None)
  };
  test_write_read_message(msg);
}

#[test]
fn pull_tests() {
  test_pull(false, gen!(bool), gen!(ZInt), gen_key(), gen!(ZInt), None);
  test_pull(true, gen!(bool), gen!(ZInt), gen_key(), gen!(ZInt), None);
  test_pull(false, gen!(bool), gen!(ZInt), gen_key(), gen!(ZInt), Some(gen!(ZInt)));
  test_pull(true, gen!(bool), gen!(ZInt), gen_key(), gen!(ZInt), Some(gen!(ZInt)));
}

fn test_query(with_decorators: bool, sn: ZInt, key: ResKey, predicate: String, qid: ZInt, target: Option<QueryTarget>, consolidation: QueryConsolidation)
{
  let msg = if with_decorators {
    Message::make_query(sn, key, predicate, qid, target, consolidation, gen_cid(), gen_props(PROPS_LENGTH, PROP_MAX_SIZE))
  } else {
    Message::make_query(sn, key, predicate, qid, target, consolidation, None, None)
  };
  test_write_read_message(msg);
}

#[test]
fn query_tests() {
  let predicate = "my_predicate".to_string();
  let no_predicate = String::default();
  test_query(false, gen!(ZInt), gen_key(), no_predicate.clone(), gen!(ZInt), None, gen_consolidation());
  test_query(true, gen!(ZInt), gen_key(), no_predicate.clone(), gen!(ZInt), None, gen_consolidation());
  test_query(false, gen!(ZInt), gen_key(), predicate.clone(), gen!(ZInt), Some(gen_query_target()), gen_consolidation());
  test_query(true, gen!(ZInt), gen_key(), predicate.clone(), gen!(ZInt), Some(gen_query_target()), gen_consolidation());
}

fn test_ping(with_decorators: bool, hash: ZInt)
{
  let msg = if with_decorators {
    Message::make_ping(hash, gen_cid(), gen_props(PROPS_LENGTH, PROP_MAX_SIZE))
  } else {
    Message::make_ping(hash, None, None)
  };
  test_write_read_message(msg);
}

#[test]
fn ping_tests() {
  test_ping(false, gen!(ZInt));
  test_ping(true, gen!(ZInt));
}

fn test_pong(with_decorators: bool, hash: ZInt)
{
  let msg = if with_decorators {
    Message::make_pong(hash, gen_cid(), gen_props(PROPS_LENGTH, PROP_MAX_SIZE))
  } else {
    Message::make_pong(hash, None, None)
  };
  test_write_read_message(msg);
}

#[test]
fn pong_tests() {
  test_pong(false, gen!(ZInt));
  test_pong(true, gen!(ZInt));
}

fn test_sync(with_decorators: bool, reliable: bool, sn: ZInt, count: Option<ZInt>)
{
  let msg = if with_decorators {
    Message::make_sync(reliable, sn, count, gen_cid(), gen_props(PROPS_LENGTH, PROP_MAX_SIZE))
  } else {
    Message::make_sync(reliable, sn, count, None, None)
  };
  test_write_read_message(msg);
}

#[test]
fn sync_tests() {
  test_sync(false, gen!(bool), gen!(ZInt), None);
  test_sync(true, gen!(bool), gen!(ZInt), None);
  test_sync(false, gen!(bool), gen!(ZInt), Some(gen!(ZInt)));
  test_sync(true, gen!(bool), gen!(ZInt), Some(gen!(ZInt)));
}

fn test_ack_nack(with_decorators: bool, sn: ZInt, mask: Option<ZInt>)
{
  let msg = if with_decorators {
    Message::make_ack_nack(sn, mask, gen_cid(), gen_props(PROPS_LENGTH, PROP_MAX_SIZE))
  } else {
    Message::make_ack_nack(sn, mask, None, None)
  };
  test_write_read_message(msg);
}

#[test]
fn ack_nack_tests() {
  test_ack_nack(false, gen!(ZInt), None);
  test_ack_nack(true, gen!(ZInt), None);
  test_ack_nack(false, gen!(ZInt), Some(gen!(ZInt)));
  test_ack_nack(true, gen!(ZInt), Some(gen!(ZInt)));
}
