#[macro_use]
extern crate criterion;
extern crate zenoh_router;
use async_std::task;
use async_std::sync::Arc;
use criterion::{Criterion, BenchmarkId};
use zenoh_protocol::proto::{Mux, SubInfo, Reliability, SubMode, whatami};
use zenoh_protocol::session::DummyHandler;
use zenoh_router::routing::broker::Tables;
use zenoh_router::routing::resource::*;
use zenoh_router::routing::pubsub::*;

fn tables_bench(c: &mut Criterion) {
  task::block_on(async{
    let tables = Tables::new();
    let primitives = Arc::new(Mux::new(Arc::new(DummyHandler::new())));

    let face0 = Tables::open_face(&tables, whatami::CLIENT, primitives.clone()).await;
    declare_resource(&mut *tables.write().await, &mut face0.upgrade().unwrap(), 1, 0, "/bench/tables").await;
    declare_resource(&mut *tables.write().await, &mut face0.upgrade().unwrap(), 2, 0, "/bench/tables/*").await;

    let face1 = Tables::open_face(&tables, whatami::CLIENT, primitives.clone()).await;

    let mut tables_bench = c.benchmark_group("tables_bench");
    let sub_info = SubInfo {
      reliability: Reliability::Reliable,
      mode: SubMode::Push,
      period: None
  };

    for p in [8, 32, 256, 1024, 8192].iter() {
      for i in 1..(*p) {
        declare_resource(&mut *tables.write().await, &mut face1.upgrade().unwrap(), i, 0, &["/bench/tables/AA", &i.to_string()].concat()).await;
        declare_subscription(&mut *tables.write().await, &mut face1.upgrade().unwrap(), i, "", &sub_info).await;
      }

      let tables = tables.read().await;
      let face0 = face0.upgrade().unwrap();
      
      tables_bench.bench_function(BenchmarkId::new("direct_route", p), |b| b.iter(|| {
        route_data_to_map(&tables, &face0, 2, "")
      }));
      
      tables_bench.bench_function(BenchmarkId::new("known_resource", p), |b| b.iter(|| {
        route_data_to_map(&tables, &face0, 0, "/bench/tables/*")
      }));
      
      tables_bench.bench_function(BenchmarkId::new("matches_lookup", p), |b| b.iter(|| {
        route_data_to_map(&tables, &face0, 0, "/bench/tables/A*")
      }));
    }
    tables_bench.finish();
  });
}

criterion_group!(benches, tables_bench);
criterion_main!(benches);
