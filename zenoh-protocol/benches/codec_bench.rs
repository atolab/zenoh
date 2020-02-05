#[macro_use]
extern crate criterion;
extern crate rand;

use criterion::{Criterion, black_box};

use std::sync::Arc;
use zenoh_protocol::core::ResKey;
use zenoh_protocol::io::rwbuf::{RWBuf,OutOfBounds};
use zenoh_protocol::proto::{Message, MessageKind};

fn _bench_zint_write((v, buf): (u64, &mut RWBuf)) -> Result<(), OutOfBounds> {  
  buf.write_zint(v).map(|_| ())
}

fn _bench_zint_write_two((v, buf): (&[u64; 2], &mut RWBuf)) -> Result<(), OutOfBounds> {  
  buf.write_zint(v[0])?;
  buf.write_zint(v[1])
}

fn _bench_zint_write_three((v, buf): (&[u64; 3], &mut RWBuf)) -> Result<(), OutOfBounds> {  
  buf.write_zint(v[0])?;
  buf.write_zint(v[1])?;
  buf.write_zint(v[2])
}

fn bench_one_zint_codec((v, buf): (u64, &mut RWBuf)) -> Result<(), OutOfBounds> {  
  buf.write_zint(v)?;
  buf.read_zint().map(|_| ())
}

fn bench_two_zint_codec((v, buf): (&[u64;2], &mut RWBuf)) -> Result<(), OutOfBounds> {  
  buf.write_zint(v[0])?;
  buf.write_zint(v[1])?;
  let _ = buf.read_zint()?;
  buf.read_zint().map(|_| ())  
}

fn bench_three_zint_codec((v, buf): (&[u64;3], &mut RWBuf)) -> Result<(), OutOfBounds> {  
  buf.write_zint(v[0])?;
  buf.write_zint(v[1])?;
  buf.write_zint(v[2])?;
  let _ = buf.read_zint()?;
  let _ = buf.read_zint()?;
  buf.read_zint().map(|_| ())  
}

fn bench_make_data(payload: Arc<Vec<u8>>) -> Result<(), OutOfBounds> {
  let _  = Message::make_data(MessageKind::FullMessage, false, 42, ResKey::ResId { id: 10 }, None, payload, None, None, None);
  Ok(())
}

fn bench_write_data(buf: &mut RWBuf, data: &Message) -> Result<(), OutOfBounds> {
  buf.write_message(data)?;  
  Ok(())
}
fn bench_write_10bytes1((v, buf): (u8, &mut RWBuf)) -> Result<(), OutOfBounds> {
  buf.write(v)?;
  buf.write(v)?;
  buf.write(v)?;
  buf.write(v)?;
  buf.write(v)?;
  buf.write(v)?;
  buf.write(v)?;
  buf.write(v)?;
  buf.write(v)?;
  buf.write(v)?;
  Ok(())
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut buf = RWBuf::new(32);     
    let rs3: [u64;3] = [u64::from(rand::random::<u8>()), u64::from(rand::random::<u8>()), u64::from(rand::random::<u8>())];     
    let rs2: [u64;2] = [u64::from(rand::random::<u8>()), u64::from(rand::random::<u8>())];
    let _ns: [u64;4] = [0; 4];
    let len = String::from("u8");
    // // reliable: bool,
    // sn: ZInt,
    // key: ResKey,
    // info: Option<Arc<Vec<u8>>>,
    // payload: Arc<Vec<u8>>,
    // cid: Option<ZInt>,
    // reply_context: Option<ReplyContext>,
    // ps: Option<Arc<Vec<Property>>> 
    let payload = Arc::new(vec![0u8, 32]);
    let data = Message::make_data(MessageKind::FullMessage, false, 42, ResKey::ResId { id: 10 }, None, payload.clone(), None, None, None);

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