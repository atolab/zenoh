#[macro_use]
extern crate criterion;
extern crate zenoh_router;
use criterion::{Criterion, BenchmarkId};
use zenoh_router::routing::tables::Tables;
use zenoh_protocol::proto::Mux;
use zenoh_protocol::session::DummyHandler;
use async_std::sync::Arc;

fn tables_bench(c: &mut Criterion) {
  let tables = Tables::new();
  let primitives = Arc::new(Mux::new(Arc::new(DummyHandler::new())));

  let sex0 = Tables::declare_session(&tables, primitives.clone());
  Tables::declare_resource(&tables, &sex0, 1, 0, "/bench/tables");
  Tables::declare_resource(&tables, &sex0, 2, 0, "/bench/tables/*");

  let sex1 = Tables::declare_session(&tables, primitives.clone());

  let mut tables_bench = c.benchmark_group("tables_bench");

  for p in [8, 32, 256, 1024, 8192].iter() {
    for i in 1..(*p) {
      Tables::declare_resource(&tables, &sex1, i, 0, &["/bench/tables/AA", &i.to_string()].concat());
      Tables::declare_subscription(&tables, &sex1, i, "");
    }

    tables_bench.bench_function(BenchmarkId::new("direct_route", p), |b| b.iter(|| {
      Tables::route_data_to_map(&tables, &sex0, &2, "")
    }));
    
    tables_bench.bench_function(BenchmarkId::new("known_resource", p), |b| b.iter(|| {
      Tables::route_data_to_map(&tables, &sex0, &0, "/bench/tables/*")
    }));
    
    tables_bench.bench_function(BenchmarkId::new("matches_lookup", p), |b| b.iter(|| {
      Tables::route_data_to_map(&tables, &sex0, &0, "/bench/tables/A*")
    }));
  }
  tables_bench.finish();
}

criterion_group!(benches, tables_bench);
criterion_main!(benches);
