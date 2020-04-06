use zenoh_util::collections::CreditQueue;
use futures::*;
use async_std::task;
use async_std::sync::Arc;
use std::time::Instant;

const CREDIT: isize = 100;

fn main() {    
    task::block_on(async {        
        let acb = Arc::new(CreditQueue::<usize>::new(vec![(256, 1), (256, 1), (256, CREDIT)], 16));
        let cq1 = acb.clone();
        let cq2 = acb.clone();
        let cq3 = acb.clone();
        let cq4 = acb.clone();
        let cq5 = acb.clone();
        let now = Instant::now();
        let n = 1_000_000usize;

        let p1 = task::spawn(async move  {
            for _ in 0..n {                
                cq1.push(0, 0).await;
            }
        });

        let p2 = task::spawn(async move  {
            for _ in 0..n {                
                cq2.push(1, 1).await;
            }
        });

        let p3 = task::spawn(async move  {
            for _ in 0..n {                
                cq3.push(2, 2).await;
            }
        });

        let p4 = task::spawn(async move  {
            for _ in 0..n {                
                cq4.push(2, 2).await;
            }
        });        

        let c1 = task::spawn(async move {
            let mut count: usize = 0;
            while count < 4*n {
                // We operate on the queue asynchronously
                let j = cq5.pull().await;
                count += 1;
                if j == 2 {
                    cq5.spend(j, 1isize).await;
                    if cq5.get_credit(j).await <= 0 {
                        cq5.recharge(j, CREDIT).await;
                    }
                }

                // Let's acquire the lock and operate on the
                // queue in a syncrhonous manner
                let mut guard = cq5.lock().await;
                while let Some(j) = guard.try_pull() {
                    count += 1;
                    if j == 2 {
                        guard.spend(j, 1isize);
                        if guard.get_credit(j) <= 0 {
                            guard.recharge(j, CREDIT);
                        }
                    }
                }
                guard.unlock().await;
            }
        });


        join!(p1, p2, p3, p4, c1);
        println!("Test run in: {}", now.elapsed().as_millis());
  });
}
