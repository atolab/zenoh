use async_std::prelude::*;
use async_std::task;
use async_std::sync::Arc;
use std::time::Instant;

use zenoh_util::collections::{
    CreditBuffer,
    CreditQueue
};

const SIZE: usize = 1_024;
const CREDIT: isize = 100;
const BATCH: usize = 64;

fn main() {    
    task::block_on(async {  
        let first = CreditBuffer::new(
            SIZE, 1isize, CreditBuffer::spending_policy(|_e| 0isize)
        );
        let second = CreditBuffer::new(
            SIZE, 1isize, CreditBuffer::spending_policy(|_e| 0isize)
        );
        let third = CreditBuffer::new(
            SIZE, 1isize, CreditBuffer::spending_policy(|_e| 1isize)
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
            for _ in 0..n/BATCH {
                let v = vec![0; BATCH];  
                cq1.push_batch(v, 0).await;
            }
        });

        let p2 = task::spawn(async move  {
            for _ in 0..n/BATCH {  
                let v = vec![1; BATCH];  
                cq2.push_batch(v, 1).await;
            }
        });

        let p3 = task::spawn(async move  {
            for _ in 0..n/BATCH {   
                let v = vec![2; BATCH];  
                cq3.push_batch(v, 2).await;
            }
        });

        let p4 = task::spawn(async move  {
            for _ in 0..n/BATCH {   
                let v = vec![2; BATCH];  
                cq4.push_batch(v, 2).await;
            }
        });        

        let c1 = task::spawn(async move {
            let mut count: usize = 0;
            while count < 4*n {
                let mut drain = cq5.drain().await;
                for j in &mut drain {
                    count += 1;
                    if cq5.get_credit(j) <= 0 {
                        cq5.recharge(j, CREDIT);
                    }
                }
                drain.drop().await;
            }
        });


        p1.join(p2).join(p3).join(p4).join(c1).await;
        println!("Test run in: {}", now.elapsed().as_millis());
    });
}
