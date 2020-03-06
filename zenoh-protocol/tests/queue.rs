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


async fn oq_run() {
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

    // Add the fourth element
    sn = sn + 1;
    let res = queue.try_push(3, sn+1);
    assert!(res.is_ok());
    let res = queue.try_pop();
    assert_eq!(res, None);

    // Add the third element
    let res = queue.try_push(2, sn);
    assert_eq!(res.is_ok(), true);
    let res = queue.try_pop();
    assert_eq!(res, Some(2));
    let res = queue.try_pop();
    assert_eq!(res, Some(3));

    // Fill the queue
    sn = sn + 2;
    let res = queue.try_push(4, sn);
    assert!(res.is_ok());
    sn = sn + 1;
    let res = queue.try_push(5, sn);
    assert!(res.is_ok());
    sn = sn + 1;
    let res = queue.try_push(6, sn);
    match res {
        Ok(_) => assert!(false),
        Err(e) => match e {
            OrderedPushError::Full(msg) => assert_eq!(msg, 6),
            OrderedPushError::OutOfSync(_) => assert!(false)
        }
    }

    // Drain the queue
    let res = queue.try_pop();
    assert_eq!(res, Some(4));
    let res = queue.try_pop();
    assert_eq!(res, Some(5));

    // Rebase the queue
    queue.set_base(16);
    let res = queue.try_push(6, sn);
    match res {
        Ok(_) => assert!(false),
        Err(e) => match e {
            OrderedPushError::Full(_) => assert!(false),
            OrderedPushError::OutOfSync(msg) => assert_eq!(msg, 6)
        }
    }

    // Verify that the queue is empty
    assert_eq!(queue.len(), 0);
}

#[test]
fn ordered_queue() {
    task::block_on(oq_run());
}