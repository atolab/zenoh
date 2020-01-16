#[macro_use]
extern crate criterion;
extern crate zenoh;
extern crate rand;

use criterion::{Criterion, black_box};

use zenoh::io::rwbuf::{RWBuf,OutOfBounds};
use rand::distributions::{Distribution, Standard};


fn unsafe_write_u32((v, buf): (u32, &mut RWBuf)) -> Result<(), OutOfBounds> {  
  
  let bs = unsafe {std::mem::transmute::<&u32, &[u8; 4]>(&v)};  
  buf.write_bytes(bs).map(|_| ())
}


fn bench_zint_write((v, buf): (u64, &mut RWBuf)) -> Result<(), OutOfBounds> {  
  buf.write_zint(v).map(|_| ())
}

fn bench_zint_write_two((v, buf): (&[u64; 2], &mut RWBuf)) -> Result<(), OutOfBounds> {  
  buf
    .write_zint(v[0])?
    .write_zint(v[1])
    .map(|_| ())
}

fn bench_zint_write_three((v, buf): (&[u64; 3], &mut RWBuf)) -> Result<(), OutOfBounds> {  
  buf
    .write_zint(v[0])?
    .write_zint(v[1])?
    .write_zint(v[2])
    .map(|_| ())
}

fn bench_write_one_c3((n, buf): (u64, &mut RWBuf)) -> Result<(), OutOfBounds> {
  buf.write_one_c3(n).map(|_| ())
}

fn bench_write_two_c3((xs, buf): (&[u64;2], &mut RWBuf)) -> Result<(), OutOfBounds> {
  buf.write_two_c3(xs).map(|_| ())
}

fn bench_write_three_c3((xs, buf): (&[u64;3], &mut RWBuf)) -> Result<(), OutOfBounds> {
  buf.write_three_c3(xs).map(|_| ())
}

fn bench_one_zint_codec((v, buf): (u64, &mut RWBuf)) -> Result<(), OutOfBounds> {  
  buf
    .write_zint(v)?
    .read_zint().map(|_| ())  
}

fn bench_two_zint_codec((v, buf): (&[u64;2], &mut RWBuf)) -> Result<(), OutOfBounds> {  
  let _ = buf
    .write_zint(v[0])?
    .write_zint(v[1])?
    .read_zint()?;
    
    buf.read_zint().map(|_| ())  
}

fn bench_three_zint_codec((v, buf): (&[u64;3], &mut RWBuf)) -> Result<(), OutOfBounds> {  
  let _ = buf
    .write_zint(v[0])?
    .write_zint(v[1])?
    .write_zint(v[2])?
    .read_zint()?;
    
    let _ = buf.read_zint()?;
    
    buf.read_zint().map(|_| ())  
}

fn bench_one_c3_codec((v, ns, buf): (u64, &mut [u64;4], &mut RWBuf)) -> Result<(), OutOfBounds> {  
  buf.write_one_c3(v)?    
  .read_c3(ns).map(|_| ())  
}

fn bench_two_c3_codec((vs, ns, buf): (&[u64;2], &mut[u64;4], &mut RWBuf)) -> Result<(), OutOfBounds> {  
  buf.write_two_c3(vs)?
  .read_c3(ns).map(|_| ())  
}

fn bench_three_c3_codec((vs, ns, buf): (&[u64;3], &mut[u64;4], &mut RWBuf)) -> Result<(), OutOfBounds> {  
  buf.write_three_c3(vs)?
  .read_c3(ns).map(|_| ())  
}



fn criterion_benchmark(c: &mut Criterion) {
    let mut buf = RWBuf::new(32);     
    let mut rs3: [u64;3] = [u64::from(rand::random::<u8>()), u64::from(rand::random::<u8>()), u64::from(rand::random::<u8>())];     
    let mut rs2: [u64;2] = [u64::from(rand::random::<u8>()), u64::from(rand::random::<u8>())];
    let mut ns: [u64;4] = [0; 4];
    let mut len = String::from("u8");

    c.bench_function(&format!("bench_one_zint_codec {}", len), |b| b.iter(|| {
      let _ = bench_one_zint_codec(black_box((rs3[0], &mut buf)));
      buf.clear();
      }));

    c.bench_function(&format!("bench_one_c3_codec {}", len), |b| b.iter(|| {
      let _ = bench_one_c3_codec(black_box((rs3[0], &mut ns, &mut buf)));
      buf.clear();
      }));  
    
    c.bench_function(&format!("bench_two_zint_codec {}", len), |b| b.iter(|| {
      let _ = bench_two_zint_codec(black_box((&rs2, &mut buf)));
      buf.clear();
      }));

    c.bench_function("bench_two_c3_codec u8", |b| b.iter(|| {
      let _ = bench_two_c3_codec(black_box((&rs2, &mut ns, &mut buf)));
      buf.clear();
      }));  

    c.bench_function("bench_three_zint_codec u8", |b| b.iter(|| {
      let _ = bench_three_zint_codec(black_box((&rs3, &mut buf)));
      buf.clear();
      }));

    c.bench_function("bench_three_c3_codec u8", |b| b.iter(|| {
      let _ = bench_three_c3_codec(black_box((&rs3, &mut ns, &mut buf)));
      buf.clear();
      }));  
    // c.bench_function("bench_zint_write u8", |b| b.iter(|| {
    //   let _ = bench_zint_write(black_box((rs3[0], &mut buf)));
    //   buf.clear();
    //   }));
    // c.bench_function("bench_write_one_c3 u8", |b| b.iter(|| {
    //   let _ = bench_write_one_c3(black_box((rs3[0], &mut buf)));
    //   buf.clear();
    //   }));

    // c.bench_function("bench_zint_write_two u8", |b| b.iter(|| {
    //   let _ = bench_zint_write_two(black_box((&rs2, &mut buf)));
    //   buf.clear();
    //   }));
    // c.bench_function("bench_write_two_c3 u8", |b| b.iter(|| {
    //   let _ = bench_write_two_c3(black_box((&rs2, &mut buf)));
    //   buf.clear();
    //   }));

    // c.bench_function("bench_zint_write_three u8", |b| b.iter(|| {
    //   let _ = bench_zint_write_three(black_box((&rs3, &mut buf)));
    //   buf.clear();
    //   }));
    // c.bench_function("bench_write_three_c3 u8", |b| b.iter(|| {
    //   let _ = bench_write_three_c3(black_box((&rs3, &mut buf)));
    //   buf.clear();
    //   }));

      ///////////////// u16 ////////////////
    rs3 = [u64::from(rand::random::<u16>()), u64::from(rand::random::<u16>()), u64::from(rand::random::<u16>())];     
    rs2 = [u64::from(rand::random::<u16>()), u64::from(rand::random::<u16>())];

    c.bench_function("bench_one_zint_codec u16", |b| b.iter(|| {
      let _ = bench_one_zint_codec(black_box((rs3[0], &mut buf)));
      buf.clear();
      }));

    c.bench_function("bench_one_c3_codec u16", |b| b.iter(|| {
      let _ = bench_one_c3_codec(black_box((rs3[0], &mut ns, &mut buf)));
      buf.clear();
      }));  
    
    c.bench_function("bench_two_zint_codec u16", |b| b.iter(|| {
      let _ = bench_two_zint_codec(black_box((&rs2, &mut buf)));
      buf.clear();
      }));

    c.bench_function("bench_two_c3_codec u16", |b| b.iter(|| {
      let _ = bench_two_c3_codec(black_box((&rs2, &mut ns, &mut buf)));
      buf.clear();
      }));  

    c.bench_function("bench_three_zint_codec u16", |b| b.iter(|| {
      let _ = bench_three_zint_codec(black_box((&rs3, &mut buf)));
      buf.clear();
      }));

    c.bench_function("bench_three_c3_codec u16", |b| b.iter(|| {
      let _ = bench_three_c3_codec(black_box((&rs3, &mut ns, &mut buf)));
      buf.clear();
      }));  
      
    //   c.bench_function("bench_zint_write u16", |b| b.iter(|| {
    //   let _ = bench_zint_write(black_box((rs3[0], &mut buf)));
    //   buf.clear();
    //   }));
    // c.bench_function("bench_write_one_c3 u16", |b| b.iter(|| {
    //   let _ = bench_write_one_c3(black_box((rs3[0], &mut buf)));
    //   buf.clear();
    //   }));

    // c.bench_function("bench_zint_write_two u16", |b| b.iter(|| {
    //   let _ = bench_zint_write_two(black_box((&rs2, &mut buf)));
    //   buf.clear();
    //   }));
    // c.bench_function("bench_write_two_c3 u16", |b| b.iter(|| {
    //   let _ = bench_write_two_c3(black_box((&rs2, &mut buf)));
    //   buf.clear();
    //   }));

    // c.bench_function("bench_zint_write_three u16", |b| b.iter(|| {
    //   let _ = bench_zint_write_three(black_box((&rs3, &mut buf)));
    //   buf.clear();
    //   }));
    // c.bench_function("bench_write_three_c3 u16", |b| b.iter(|| {
    //   let _ = bench_write_three_c3(black_box((&rs3, &mut buf)));
    //   buf.clear();
    //   }));


    ///////////////// u32 ////////////////
    rs3 = [u64::from(rand::random::<u32>()), u64::from(rand::random::<u32>()), u64::from(rand::random::<u32>())];     
    rs2 = [u64::from(rand::random::<u32>()), u64::from(rand::random::<u32>())];


    c.bench_function("bench_one_zint_codec u32", |b| b.iter(|| {
      let _ = bench_one_zint_codec(black_box((rs3[0], &mut buf)));
      buf.clear();
      }));

    c.bench_function("bench_one_c3_codec u32", |b| b.iter(|| {
      let _ = bench_one_c3_codec(black_box((rs3[0], &mut ns, &mut buf)));
      buf.clear();
      }));  
    
    c.bench_function("bench_two_zint_codec u32", |b| b.iter(|| {
      let _ = bench_two_zint_codec(black_box((&rs2, &mut buf)));
      buf.clear();
      }));

    c.bench_function("bench_two_c3_codec u32", |b| b.iter(|| {
      let _ = bench_two_c3_codec(black_box((&rs2, &mut ns, &mut buf)));
      buf.clear();
      }));  

    c.bench_function("bench_three_zint_codec u32", |b| b.iter(|| {
      let _ = bench_three_zint_codec(black_box((&rs3, &mut buf)));
      buf.clear();
      }));

    c.bench_function("bench_three_c3_codec u32", |b| b.iter(|| {
      let _ = bench_three_c3_codec(black_box((&rs3, &mut ns, &mut buf)));
      buf.clear();
      }));  

    //   c.bench_function("bench_zint_write u32", |b| b.iter(|| {
    //   let _ = bench_zint_write(black_box((rs3[0], &mut buf)));
    //   buf.clear();
    //   }));
    // c.bench_function("bench_write_one_c3 u32", |b| b.iter(|| {
    //   let _ = bench_write_one_c3(black_box((rs3[0], &mut buf)));
    //   buf.clear();
    //   }));

    // c.bench_function("bench_zint_write_two u32", |b| b.iter(|| {
    //   let _ = bench_zint_write_two(black_box((&rs2, &mut buf)));
    //   buf.clear();
    //   }));
    // c.bench_function("bench_write_two_c3 u32", |b| b.iter(|| {
    //   let _ = bench_write_two_c3(black_box((&rs2, &mut buf)));
    //   buf.clear();
    //   }));

    // c.bench_function("bench_zint_write_three u32", |b| b.iter(|| {
    //   let _ = bench_zint_write_three(black_box((&rs3, &mut buf)));
    //   buf.clear();
    //   }));
    // c.bench_function("bench_write_three_c3 u32", |b| b.iter(|| {
    //   let _ = bench_write_three_c3(black_box((&rs3, &mut buf)));
    //   buf.clear();
    //   }));      

    ///////////////// u64 ////////////////
      rs3 = [u64::from(rand::random::<u64>()), u64::from(rand::random::<u64>()), u64::from(rand::random::<u64>())];     
      rs2 = [u64::from(rand::random::<u64>()), u64::from(rand::random::<u64>())];
    
    c.bench_function("bench_one_zint_codec u64", |b| b.iter(|| {
      let _ = bench_one_zint_codec(black_box((rs3[0], &mut buf)));
      buf.clear();
      }));

    c.bench_function("bench_one_c3_codec u64", |b| b.iter(|| {
      let _ = bench_one_c3_codec(black_box((rs3[0], &mut ns, &mut buf)));
      buf.clear();
      }));  
    
    c.bench_function("bench_two_zint_codec u64", |b| b.iter(|| {
      let _ = bench_two_zint_codec(black_box((&rs2, &mut buf)));
      buf.clear();
      }));

    c.bench_function("bench_two_c3_codec u64", |b| b.iter(|| {
      let _ = bench_two_c3_codec(black_box((&rs2, &mut ns, &mut buf)));
      buf.clear();
      }));  

    c.bench_function("bench_three_zint_codec u64", |b| b.iter(|| {
      let _ = bench_three_zint_codec(black_box((&rs3, &mut buf)));
      buf.clear();
      }));

    c.bench_function("bench_three_c3_codec u64", |b| b.iter(|| {
      let _ = bench_three_c3_codec(black_box((&rs3, &mut ns, &mut buf)));
      buf.clear();
      }));  
    //   c.bench_function("bench_zint_write u64", |b| b.iter(|| {
    //   let _ = bench_zint_write(black_box((rs3[0], &mut buf)));
    //   buf.clear();
    //   }));
    // c.bench_function("bench_write_one_c3 u64", |b| b.iter(|| {
    //   let _ = bench_write_one_c3(black_box((rs3[0], &mut buf)));
    //   buf.clear();
    //   }));

    // c.bench_function("bench_zint_write_two u64", |b| b.iter(|| {
    //   let _ = bench_zint_write_two(black_box((&rs2, &mut buf)));
    //   buf.clear();
    //   }));
    // c.bench_function("bench_write_two_c3 u64", |b| b.iter(|| {
    //   let _ = bench_write_two_c3(black_box((&rs2, &mut buf)));
    //   buf.clear();
    //   }));

    // c.bench_function("bench_zint_write_three u64", |b| b.iter(|| {
    //   let _ = bench_zint_write_three(black_box((&rs3, &mut buf)));
    //   buf.clear();
    //   }));
    // c.bench_function("bench_write_three_c3 u64", |b| b.iter(|| {
    //   let _ = bench_write_three_c3(black_box((&rs3, &mut buf)));
    //   buf.clear();
    //   }));      
    // rs = [u64::from(rand::random::<u16>(), u64::from(rand::random::<u16>(), u64::from(rand::random::<u16>())];     
    // c.bench_function("bench_zint_write2 u8", |b| b.iter(|| {
    //   let _ = bench_zint_write(black_box((0x3f, &mut buf)));
    //   buf.clear();
    //   }));
    // c.bench_function("bench_zint_read u8", |b| b.iter(|| {
    //   let _ = bench_zint_read(black_box(&mut buf));
    //   buf.clear();
    //   }));      
    // c.bench_function("zint_codec u8", |b| b.iter(|| {
    //   let _ = bench_zint_codec(black_box((a, &mut buf)));
    //   buf.clear();
    //   }));

    // // u16 
    // a = u64::from(rand::random::<u16>());     
    // c.bench_function("bench_zint_write u16", |b| b.iter(|| {
    //   let _ = bench_zint_write(black_box((a, &mut buf)));
    //   buf.clear();
    //   }));
    // c.bench_function("bench_zint_write2 u16", |b| b.iter(|| {
    //   let _ = bench_zint_write(black_box((0x1fff, &mut buf)));
    //   buf.clear();
    //   }));
    // c.bench_function("bench_zint_read u16 ", |b| b.iter(|| {
    //   let _ = bench_zint_read(black_box(&mut buf));
    //   buf.clear();
    //   }));      
    // c.bench_function("zint_codec u16", |b| b.iter(|| {
    //   let _ = bench_zint_codec(black_box((a, &mut buf)));
    //   buf.clear();
    //   }));

    // // u32
    // a = u64::from(rand::random::<u32>());
    //   c.bench_function("bench_zint_write u32", |b| b.iter(|| {
    //   let _ = bench_zint_write(black_box((a, &mut buf)));
    //   buf.clear();
    //   }));

    // // unsafe_write_u32 
    // let x = u32::from(rand::random::<u32>());
    // c.bench_function("unsafe_write_u32 u32", |b| b.iter(|| {
    //   let _ = unsafe_write_u32(black_box((x, &mut buf)));
    //   buf.clear();
    //   }));
    // c.bench_function("bench_zint_read u32", |b| b.iter(|| {
    //   let _ = bench_zint_read(black_box(&mut buf));
    //   buf.clear();
    //   }));      
    // c.bench_function("zint_codec u32 ", |b| b.iter(|| {
    //   let _ = bench_zint_codec(black_box((a, &mut buf)));
    //   buf.clear();
    //   }));

    // // u64
    //   a = u64::from(rand::random::<u64>());
    //   c.bench_function("bench_zint_write u64", |b| b.iter(|| {
    //   let _ = bench_zint_write(black_box((a, &mut buf)));
    //   buf.clear();
    //   }));
    // c.bench_function("bench_zint_read u64", |b| b.iter(|| {
    //   let _ = bench_zint_read(black_box(&mut buf));
    //   buf.clear();
    //   }));      
    // c.bench_function("zint_codec u64", |b| b.iter(|| {
    //   let _ = bench_zint_codec(black_box((a, &mut buf)));
    //   buf.clear();
    //   }));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);