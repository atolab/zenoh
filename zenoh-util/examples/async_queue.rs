use zenoh_util::collections::CircularQueue;
use futures::*;
use async_std::task;
use std::sync::Arc;
use std::time::Instant;

fn main() {    
    task::block_on(async {        
        let acb = Arc::new(CircularQueue::<u64>::new(256, 16));
        let cq1 = acb.clone();
        let cq2 = acb.clone();
        let cq3 = acb.clone();
        let cq4 = acb.clone();
        let now = Instant::now();
        let n = 10_000_000u32;

        let p1 = task::spawn(async move  {
            for i in 0..n {                
                cq1.push(i as u64).await;                
            }         
        });

        let p2 = task::spawn(async move  {
            for i in 0..n {                
                cq2.push(i as u64).await;                
            }
        });        
        
        let c1 = task::spawn(async move {
            for  _ in 0..n {
                let _ = cq3.pull().await;            
            }        
        });

        let c2 = task::spawn(async move {
            for  _ in 0..n {
                let _ = cq4.pull().await;            
            }        
        });

      join!(p1, p2, c1, c2);
      println!("Test run in: {}", now.elapsed().as_millis());
  });
}
