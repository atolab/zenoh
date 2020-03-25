#[macro_use]
extern crate criterion;

use criterion::{Criterion};

use zenoh_protocol::core::ResKey;
use zenoh_protocol::io::ArcSlice;
use zenoh_protocol::proto::{Message, MessageKind};


use std::sync::Arc;

fn consume_message(msg: Message) {
  drop(msg);
}

fn consume_message_arc(msg: Arc<Message>) {
  drop(msg);
}

fn criterion_benchmark(c: &mut Criterion) {
  let iters = [1, 2, 4 ,8 ,16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 1, 8, 64, 128, 1024];
  let reliable = true;  
  
  for n in &iters {
    let res_key = ResKey::RIdWithSuffix(18, String::from("/com/acme/sensors/temp"));
    let info = Some(ArcSlice::new(Arc::new(vec![0;1024]), 0, 1024));
    let payload = ArcSlice::new(Arc::new(vec![0;1024]), 0, 1024);
    
    c.bench_function(format!("{} msg_creation", n).as_str(), |b| b.iter(||{
      for _ in 0..*n {
        let msg = Message::make_data(
          MessageKind::FullMessage, reliable, 0, res_key.clone(), info.clone(), payload.clone(), None, None, None);
          consume_message(msg);
      }
    }));
  }

  let res_key = ResKey::RIdWithSuffix(18, String::from("/com/acme/sensors/temp"));
  let info = Some(ArcSlice::new(Arc::new(vec![0;1024]), 0, 1024));
  let payload = ArcSlice::new(Arc::new(vec![0;1024]), 0, 1024);
  let msg = Arc::new(Message::make_data(MessageKind::FullMessage, reliable, 0, res_key.clone(), info.clone(), payload.clone(), None, None, None)); 
  
  for n in &iters {
    let amsg = msg.clone();
    c.bench_function(format!("{} arc_msg_clone", n).as_str(), |b| b.iter(||{      
      for _ in 0..*n {                   
          consume_message_arc(amsg.clone());
      } 
    }));
  }

}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
