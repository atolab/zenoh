#[macro_use]
extern crate criterion;
extern crate rand;

use criterion::{Criterion, black_box};

use zenoh_protocol::core::{ZResult, ResKey};
use zenoh_protocol::io::{RBuf, WBuf};
use zenoh_protocol::proto::{Message, MessageKind};

fn _bench_zint_write((v, buf): (u64, &mut WBuf)) {
  buf.write_zint(v);
}

fn _bench_zint_write_two((v, buf): (&[u64; 2], &mut WBuf)) {  
  buf.write_zint(v[0]);
  buf.write_zint(v[1]);
}

fn _bench_zint_write_three((v, buf): (&[u64; 3], &mut WBuf)) {  
  buf.write_zint(v[0]);
  buf.write_zint(v[1]);
  buf.write_zint(v[2]);
}

fn bench_one_zint_codec((v, buf): (u64, &mut WBuf)) -> ZResult<()> {  
  buf.write_zint(v);
  RBuf::from(&*buf).read_zint().map(|_| ())
}

fn bench_two_zint_codec((v, buf): (&[u64;2], &mut WBuf)) -> ZResult<()> {  
  buf.write_zint(v[0]);
  buf.write_zint(v[1]);
  let mut rbuf = RBuf::from(&*buf);
  let _ = rbuf.read_zint()?;
  rbuf.read_zint().map(|_| ())  
}

fn bench_three_zint_codec((v, buf): (&[u64;3], &mut WBuf)) -> ZResult<()> {  
  buf.write_zint(v[0]);
  buf.write_zint(v[1]);
  buf.write_zint(v[2]);
  let mut rbuf = RBuf::from(&*buf);
  let _ = rbuf.read_zint()?;
  let _ = rbuf.read_zint()?;
  rbuf.read_zint().map(|_| ())  
}

fn bench_make_data(payload: RBuf) {
  let _  = Message::make_data(MessageKind::FullMessage, false, 42, ResKey::RId(10), None, payload, None, None, None);
}

fn bench_write_data(buf: &mut WBuf, data: &Message) {
  buf.write_message(data);  
}
fn bench_write_10bytes1((v, buf): (u8, &mut WBuf)) {
  buf.write(v);
  buf.write(v);
  buf.write(v);
  buf.write(v);
  buf.write(v);
  buf.write(v);
  buf.write(v);
  buf.write(v);
  buf.write(v);
  buf.write(v);
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut buf = WBuf::new(64, true);     
    let rs3: [u64;3] = [u64::from(rand::random::<u8>()), u64::from(rand::random::<u8>()), u64::from(rand::random::<u8>())];     
    let rs2: [u64;2] = [u64::from(rand::random::<u8>()), u64::from(rand::random::<u8>())];
    let _ns: [u64;4] = [0; 4];
    let len = String::from("u8");
    // // reliable: bool,
    // sn: ZInt,
    // key: ResKey,
    // info: Option<RBuf>,
    // payload: RBuf,
    // cid: Option<ZInt>,
    // reply_context: Option<ReplyContext>,
    // ps: Option<Arc<Vec<Property>>> 
    let payload = RBuf::from(vec![0u8, 32]);
    let data = Message::make_data(MessageKind::FullMessage, false, 42, ResKey::RId(10), None, payload.clone(), None, None, None);

    c.bench_function(&format!("bench_one_zint_codec {}", len), |b| b.iter(|| {
      let _ = bench_one_zint_codec(black_box((rs3[0], &mut buf)));
      buf.clear();
      }));

    c.bench_function(&format!("bench_two_zint_codec {}", len), |b| b.iter(|| {
      let _ = bench_two_zint_codec(black_box((&rs2, &mut buf)));
      buf.clear();
      }));

    c.bench_function("bench_three_zint_codec u8", |b| b.iter(|| {
      let _ = bench_three_zint_codec(black_box((&rs3, &mut buf)));
      buf.clear();
      }));

    let r4 = rand::random::<u8>();
    c.bench_function("bench_write_10bytes1", |b| b.iter(|| {
      let _ = bench_write_10bytes1(black_box((r4, &mut buf)));
      buf.clear();
      }));    
    c.bench_function("bench_make_data", |b| b.iter(|| {
      let _ = bench_make_data(payload.clone());
      buf.clear();
    }));
    c.bench_function("bench_write_data", |b| b.iter(|| {
      let _ = bench_write_data(&mut buf, &data);
      buf.clear();
    }));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);