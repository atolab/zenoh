use async_std::task;

use zenoh_protocol::core::ZInt;
use zenoh_protocol::session::{
    OrderedQueue,
    OrderedPushError,
    PriorityQueue
};


async fn pq_run() {
    let size = 2;
    let prio = 2;
    let queue: PriorityQueue<usize> = PriorityQueue::new(size, prio);

    // High: 0, Low: 1
    queue.push(0, 0).await;
    queue.push(1, 1).await;
    let m = queue.pop().await;
    assert_eq!(m, 0);
    let m = queue.pop().await;
    assert_eq!(m, 1);

    // High: 1, Low: 0
    queue.push(0, 1).await;
    queue.push(1, 0).await;
    let m = queue.pop().await;
    assert_eq!(m, 1);
    let m = queue.pop().await;
    assert_eq!(m, 0);

    // Fill the low priority queue
    let res = queue.try_push(1, 1);
    assert_eq!(res, None);
    let res = queue.try_push(1, 1);
    assert_eq!(res, None);
    let res = queue.try_push(1, 1);
    assert_eq!(res, Some(1));

    // Fill the high priority queue
    let res = queue.try_push(0, 0);
    assert_eq!(res, None);
    let res = queue.try_push(0, 0);
    assert_eq!(res, None);
    let res = queue.try_push(0, 0);
    assert_eq!(res, Some(0));

    // Drain the queue
    let res = queue.try_pop();
    assert_eq!(res, Some(0));
    let res = queue.try_pop();
    assert_eq!(res, Some(0));
    let res = queue.try_pop();
    assert_eq!(res, Some(1));
    let res = queue.try_pop();
    assert_eq!(res, Some(1));
    let res = queue.try_pop();
    assert_eq!(res, None);
}

#[test]
fn priority_queue() {
    task::block_on(pq_run());
}

#[test]
fn ordered_queue_simple() {
    let size = 2;
    let mut queue: OrderedQueue<usize> = OrderedQueue::new(size);

    let mut sn: ZInt = 0;
    // Add the first element
    let res = queue.try_push(0, sn);
    assert!(res.is_ok());
    let res = queue.try_pop();
    assert_eq!(res, Some(0));

    // Add the second element
    sn = sn + 1;
    let res = queue.try_push(1, sn);
    assert!(res.is_ok());
    let res = queue.try_pop();
    assert_eq!(res, Some(1));

    // Verify that the queue is empty
    assert_eq!(queue.len(), 0);
}

#[test]
fn ordered_queue_order() {
    let size = 2;
    let mut queue: OrderedQueue<usize> = OrderedQueue::new(size);

    let sn: ZInt = 0;

    // Add the second element
    let res = queue.try_push(1, sn+1);
    assert!(res.is_ok());
    let res = queue.try_pop();
    assert_eq!(res, None);

    // Add the first element
    let res = queue.try_push(0, sn);
    assert!(res.is_ok());
    let res = queue.try_pop();
    assert_eq!(res, Some(0));
    let res = queue.try_pop();
    assert_eq!(res, Some(1));
    let res = queue.try_pop();
    assert_eq!(res, None);

    // Verify that the queue is empty
    assert_eq!(queue.len(), 0);
}

#[test]
fn ordered_queue_full() {
    let size = 2;
    let mut queue: OrderedQueue<usize> = OrderedQueue::new(size);

    let mut sn: ZInt = 0;

    // Fill the queue
    let res = queue.try_push(0, sn);
    assert!(res.is_ok());
    sn = sn + 1;
    let res = queue.try_push(1, sn);
    assert!(res.is_ok());
    sn = sn + 1;
    let res = queue.try_push(2, sn);
    match res {
        Ok(_) => assert!(false),
        Err(e) => match e {
            OrderedPushError::Full(msg) => assert_eq!(msg, 2),
            OrderedPushError::OutOfSync(_) => assert!(false)
        }
    }

    // Drain the queue
    let res = queue.try_pop();
    assert_eq!(res, Some(0));
    let res = queue.try_pop();
    assert_eq!(res, Some(1));

    // Verify that the queue is empty
    assert_eq!(queue.len(), 0);
}

#[test]
fn ordered_queue_out_of_sync() {
    let size = 2;
    let mut queue: OrderedQueue<usize> = OrderedQueue::new(size);

    let sn: ZInt = 3;

    // Rebase the queue
    queue.set_base(0);
    let res = queue.try_push(0, sn);
    match res {
        Ok(_) => assert!(false),
        Err(e) => match e {
            OrderedPushError::Full(_) => assert!(false),
            OrderedPushError::OutOfSync(msg) => assert_eq!(msg, 0)
        }
    }

    // Verify that the queue is empty
    assert_eq!(queue.len(), 0);
}

#[test]
fn ordered_queue_overflow() {
    // Test the overflow case
    let size = 4;
    let mut queue: OrderedQueue<usize> = OrderedQueue::new(size);

    let min = ZInt::min_value();
    let max = ZInt::max_value();

    queue.set_base(max-1);
    let res = queue.try_push(0, max-1);
    assert!(res.is_ok());
    let res = queue.try_push(1, max);
    assert!(res.is_ok());
    let res = queue.try_push(2, min);
    assert!(res.is_ok());
    let res = queue.try_push(3, min+1);
    assert!(res.is_ok());
    let res = queue.try_pop();
    assert_eq!(res, Some(0));
    let res = queue.try_pop();
    assert_eq!(res, Some(1));
    let res = queue.try_pop();
    assert_eq!(res, Some(2));
    let res = queue.try_pop();
    assert_eq!(res, Some(3));
    let res = queue.try_pop();
    assert_eq!(res, None);

    // Verify that the queue is empty
    assert_eq!(queue.len(), 0);
}


#[test]
fn ordered_queue_mask() {
    // Test the overflow case
    let size: ZInt = 64;
    let mut queue: OrderedQueue<usize> = OrderedQueue::new(size as usize);

    let mut sn: ZInt = 0;  
    while sn < size {
        let res = queue.try_push(sn as usize, sn);
        assert!(res.is_ok());
        sn = sn + 2;
    }

    // Verify that the mask is correct
    let mask = 0b0010101010101010101010101010101010101010101010101010101010101010;
    assert_eq!(queue.get_mask(), mask);

    // Insert the missing elements
    let mut sn: ZInt = 1;  
    while sn < size {
        let res = queue.try_push(sn as usize, sn);
        assert!(res.is_ok());
        sn = sn + 2;
    }

     // Verify that the mask is correct
     let mask = 0b0;
     assert_eq!(queue.get_mask(), mask);

    // Drain the queue
    while let Some(_) = queue.try_pop() {}
    // Verify that the queue is empty
    assert_eq!(queue.len(), 0);
}