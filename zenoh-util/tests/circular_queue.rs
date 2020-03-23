use zenoh_util::CircularQueue;
use futures::*;
use async_std::task;
use std::sync::Arc;
use std::time::Instant;


#[test]
fn stress_test_circular_queue() {    
  task::block_on(async {        
      let acb = Arc::new(CircularQueue::<u64>::new(256));
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

      join!(f2, f1);
      println!("Test run in: {}", now.elapsed().as_millis());
  });
}
