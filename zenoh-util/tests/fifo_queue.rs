use async_std::prelude::*;
use async_std::sync::Arc;
use async_std::task;
use std::time::Instant;

use zenoh_util::collections::FifoQueue;


#[test]
fn stress_test_fifo_queue() {    
  task::block_on(async {        
      let acb = Arc::new(FifoQueue::<u64>::new(256, 16));
      let p = acb.clone();
      let c = acb.clone();
      let now = Instant::now();
      let f1 = task::spawn(async move  {
          for i in 0..1_000_000u32 {                
              p.push(i as u64).await;                
          }
      });

      let f2 = task::spawn(async move {
          for  _ in 0..1_000_000u32 {
              let _ = c.pull().await; 
           
          }
        
      });

      f1.join(f2).await;
      println!("Test run in: {}", now.elapsed().as_millis());
  });
}
