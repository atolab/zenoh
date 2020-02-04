#[macro_use]
extern crate criterion;
extern crate zenoh_router;
use criterion::{Criterion};
use zenoh_router::routing::tables::Tables;

fn criterion_benchmark(c: &mut Criterion) {
    let tables = Tables::new();

    let sex0 = Tables::declare_session(&tables, 0);
    Tables::declare_resource(&tables, &sex0, 1, "/bench/tables");
    Tables::declare_resource(&tables, &sex0, 2, "/bench/tables/*");

    let sex1 = Tables::declare_session(&tables, 1);

    for p in [8, 32, 256, 1024, 8192].iter() {
      for i in 1..(*p) {
        Tables::declare_resource(&tables, &sex1, i, &["/bench/tables/AA", &i.to_string()].concat());
        Tables::declare_subscription(&tables, &sex1, i, "");
      }
      
      c.bench_function("bench_route_data_1", |b| b.iter(|| {
        let _ = Tables::route_data(&tables, &sex0, &2, "");
        }));
    
      c.bench_function("bench_route_data_2", |b| b.iter(|| {
        let _ = Tables::route_data(&tables, &sex0, &0, "/bench/tables/*");
        }));
      
      c.bench_function("bench_route_data_3", |b| b.iter(|| {
        let _ = Tables::route_data(&tables, &sex0, &0, "/bench/tables/A*");
        }));
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
