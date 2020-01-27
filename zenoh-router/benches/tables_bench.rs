#[macro_use]
extern crate criterion;
extern crate zenoh_router;
use criterion::{Criterion};
use zenoh_router::routing::tables::Tables;

fn criterion_benchmark(c: &mut Criterion) {
    let tables = Tables::new();

    let sex0 = Tables::declare_session(&tables, 0);
    Tables::declare_resource(&tables, &sex0, 11, "/test/client");
    Tables::declare_subscription(&tables, &sex0, 11, "/**");
    Tables::declare_resource(&tables, &sex0, 12, "/test/client/z1_pub1");

    let sex1 = Tables::declare_session(&tables, 1);
    Tables::declare_resource(&tables, &sex1, 21, "/test/client");
    Tables::declare_subscription(&tables, &sex1, 21, "/**");
    Tables::declare_resource(&tables, &sex1, 22, "/test/client/z2_pub1");

    let sex2 = Tables::declare_session(&tables, 2);
    Tables::declare_resource(&tables, &sex2, 31, "/test/client");
    Tables::declare_subscription(&tables, &sex2, 31, "/**");

    
    c.bench_function("bench_route_data_1", |b| b.iter(|| {
      let _ = Tables::route_data(&tables, &sex0, &0, "/test/client/z1_wr1");
      }));
    
    c.bench_function("bench_route_data_2", |b| b.iter(|| {
      let _ = Tables::route_data(&tables, &sex0, &12, "");
      }));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
