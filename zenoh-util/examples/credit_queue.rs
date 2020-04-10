use zenoh_util::collections::{
    CreditBuffer,
    CreditQueue
};
use futures::*;
use async_std::task;
use async_std::sync::Arc;
use std::time::Instant;

const CREDIT: isize = 100;


fn main() {    
    task::block_on(async {
        let first = CreditBuffer::new(
            256usize, 1isize, CreditBuffer::spending_policy(|_e| 0isize)
        );
        let second = CreditBuffer::new(
            256usize, 1isize, CreditBuffer::spending_policy(|_e| 0isize)
        );
        let third = CreditBuffer::new(
            256usize, 1isize, CreditBuffer::spending_policy(|_e| 1isize)
        );
        let queues = vec![first, second, third];
        
        let acb = Arc::new(CreditQueue::<usize>::new(queues, 16));
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
            for _ in 0..(4*n) {
                let j = cq5.pull().await;
                if cq5.get_credit(j) <= 0 {
                    cq5.recharge(j, CREDIT).await;
                }
            }
        });


        join!(p1, p2, p3, p4, c1);
        println!("Test run in: {}", now.elapsed().as_millis());
  });
}
