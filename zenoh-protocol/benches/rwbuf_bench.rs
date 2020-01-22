#[macro_use]
extern crate criterion;
extern crate rand;

use criterion::{Criterion, black_box};

use zenoh_protocol::io::rwbuf::{RWBuf,OutOfBounds};
use rand::distributions::{Distribution, Standard};




fn bench_foo((v, buf): (u64, &mut RWBuf)) -> Result<(), OutOfBounds> {  
  buf.write_zint(v).map(|_| ())
}



fn criterion_benchmark(c: &mut Criterion) {
    let mut buf = RWBuf::new(32);     
    let mut rs3: [u64;3] = [u64::from(rand::random::<u8>()), u64::from(rand::random::<u8>()), u64::from(rand::random::<u8>())];     
    let mut rs2: [u64;2] = [u64::from(rand::random::<u8>()), u64::from(rand::random::<u8>())];
    let mut ns: [u64;4] = [0; 4];
    let mut len = String::from("u8");

    c.bench_function("bench_foo u8", |b| b.iter(|| {
      let _ = bench_foo(black_box((rs3[0], &mut buf)));
      buf.clear();
      }));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
