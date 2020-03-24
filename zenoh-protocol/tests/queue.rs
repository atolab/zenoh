// use async_std::prelude::*;
use async_std::sync::Arc;
use async_std::task;
use rand::{
    Rng,
    thread_rng
};
use std::time::Instant;

use zenoh_protocol::core::{
    ResKey,
    ZInt
};
use zenoh_protocol::io::ArcSlice;
use zenoh_protocol::proto::{
    Message,
    MessageKind
};
use zenoh_protocol::session::{
    OrderedQueue,
    QueueInner,
    MutexQueuePrio,
    MessageTx
};


#[test]
fn stress_test_queue_priority() {
    println!("GO GO GO");          
    let num_queues = 1;
    let capacity = 256;

    let inner = QueueInner::<MessageTx>::new(num_queues, capacity);
    let queue = Arc::new(MutexQueuePrio::new(inner, num_queues));

    // Build reliable data messages of 64 bytes payload
    let kind = MessageKind::FullMessage;
    let reliable = true;
    let sn = 0;
    let key = ResKey::RName("test".to_string());
    let info = None;
    let payload = ArcSlice::new(Arc::new(vec![0u8; 64]), 0, 1);
    let reply_context = None;
    let cid = None;
    let properties = None;
    let message_reliable = Message::make_data(kind, reliable, sn, key, info, payload, reply_context, cid, properties);  

    let tot_msg = 10_000_000u32;
    let num_prs = 1;
    for _ in 0..num_prs {
        let c_queue = queue.clone();
        let c_message = message_reliable.clone();
        task::spawn(async move  {
            for i in 0..tot_msg/num_prs {   
                let to_send = MessageTx {
                    inner: c_message.clone(),
                    link: None,
                    notify: None
                };
                let mut guard = c_queue.lock_push(0).await;
                if guard.push(to_send, 0).is_some() {
                    println!("PUSH ERROR! {}", i);
                }
                // Update the bitmask on the queue having messages
                c_queue.update_mask(guard.get_mask());             
            }
        });
    }

    let c_queue = queue.clone();
    let now = Instant::now();
    let f2 = task::spawn(async move {
        for i in 0..tot_msg {
            let mut guard = c_queue.lock_pop().await;
            if guard.pop().is_none() {
                println!("POP ERROR! {}", i);
            }
            c_queue.update_mask(guard.get_mask());  
        }
    });

    task::block_on(f2);
  
    println!("Test run in: {}", now.elapsed().as_millis());
}


#[test]
fn ordered_queue_simple() {
    let size = 2;
    let mut queue: OrderedQueue<usize> = OrderedQueue::new(size);

    let mut sn: ZInt = 0;
    // Add the first element
    let res = queue.try_push(0, sn);
    assert!(res.is_none());
    let res = queue.try_pop();
    assert_eq!(res, Some(0));

    // Add the second element
    sn = sn + 1;
    let res = queue.try_push(1, sn);
    assert!(res.is_none());
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
    assert!(res.is_none());
    let res = queue.try_pop();
    assert_eq!(res, None);

    // Add the first element
    let res = queue.try_push(0, sn);
    assert!(res.is_none());
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
    assert!(res.is_none());
    sn = sn + 1;
    let res = queue.try_push(1, sn);
    assert!(res.is_none());
    sn = sn + 1;
    let res = queue.try_push(2, sn);
    match res {
        Some(msg) => assert_eq!(msg, 2),
        None => assert!(false)
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
    let mut queue: OrderedQueue<ZInt> = OrderedQueue::new(size);

    let sn: ZInt = 3;

    let res = queue.try_push(sn, sn);
    match res {
        Some(msg) => assert_eq!(msg, sn),
        None => assert!(false),
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
    assert!(res.is_none());
    let res = queue.try_push(1, max);
    assert!(res.is_none());
    let res = queue.try_push(2, min);
    assert!(res.is_none());
    let res = queue.try_push(3, min+1);
    assert!(res.is_none());
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
    // Test the deterministic insertion of elements and mask
    let size = 64;
    let mut queue: OrderedQueue<ZInt> = OrderedQueue::new(size);

    let mut sn: ZInt = 0;  
    while sn < size as ZInt {
        let res = queue.try_push(sn, sn);
        assert!(res.is_none());
        sn = sn + 2;
    }

    // Verify that the mask is correct
    let mask = 0b0010101010101010101010101010101010101010101010101010101010101010;
    assert_eq!(queue.get_mask(), mask);

    // Insert the missing elements
    let mut sn: ZInt = 1;  
    while sn < size as ZInt {
        let res = queue.try_push(sn, sn);
        assert!(res.is_none());
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

#[test]
fn ordered_queue_random_mask() {
    // Test the random insertion of elements and the mask
    let size = 64;
    let mut queue: OrderedQueue<ZInt> = OrderedQueue::new(size);

    let mut sequence = Vec::<ZInt>::new();
    for i in 0..size {
        sequence.push(i as ZInt);
    }

    let head = 0;
    let mut tail = 0;
    let mut mask: ZInt = 0;
    let mut rng = thread_rng();
    while sequence.len() > 0 {
        // Get random sequence number
        let index = rng.gen_range(0, sequence.len());
        let sn = sequence.remove(index);
        // Update the tail
        if sn > tail {
            tail = sn;
        }
        // Push the element on the queue
        let res = queue.try_push(sn, sn);
        assert!(res.is_none());
        // Locally comput the mask
        mask = mask | (1 << sn);
        let shift: u32 = tail.wrapping_sub(head) as u32;
        let window = !ZInt::max_value().wrapping_shl(shift);
        // Verify that the mask is correct
        assert_eq!(queue.get_mask(), !mask & window);
    }

    // Verify that we have filled the queue
    assert_eq!(queue.len(), size);
    // Verify that no elements are marked for retransmission
    assert_eq!(queue.get_mask(), !ZInt::max_value());

    // Drain the queue
    while let Some(_) = queue.try_pop() {}
    // Verify that the queue is empty
    assert_eq!(queue.len(), 0);

    // Verify that the mask is correct
    let mask = 0b0;
    assert_eq!(queue.get_mask(), mask);
}

#[test]
fn ordered_queue_rebase() {
    let size = 8;
    let mut queue: OrderedQueue<ZInt> = OrderedQueue::new(size);

    // Fill the queue
    for i in 0..(size as ZInt) {
        // Push the element on the queue
        let res = queue.try_push(i, i);
        assert!(res.is_none());
    }

    // Verify that the queue is full
    assert_eq!(queue.len(), size);

    // Verify that the base is correct
    assert_eq!(queue.get_base(), 0);

    // Rebase the queue
    queue.set_base(4 as ZInt);
    // Verify that the correct length of the queue
    assert_eq!(queue.len(), 4);
    // Verify that the base is correct
    assert_eq!(queue.get_base(), 4);

    // Drain the queue
    let res = queue.try_pop();
    assert_eq!(res, Some(4));
    assert_eq!(queue.get_base(), 5);

    let res = queue.try_pop();
    assert_eq!(res, Some(5));
    assert_eq!(queue.get_base(), 6);

    let res = queue.try_pop();
    assert_eq!(res, Some(6));
    assert_eq!(queue.get_base(), 7);

    let res = queue.try_pop();
    assert_eq!(res, Some(7));
    assert_eq!(queue.get_base(), 8);

    let res = queue.try_pop();
    assert_eq!(res, None);
    assert_eq!(queue.get_base(), 8);

    // Verify that the correct length of the queue
    assert_eq!(queue.len(), 0);

    // Rebase the queue
    queue.set_base(0 as ZInt);
    // Verify that the base is correct
    assert_eq!(queue.get_base(), 0);
    // Fill the queue
    for i in 0..(size as ZInt) {
        // Push the element on the queue
        let res = queue.try_push(i, i);
        assert!(res.is_none());
    }

    // Verify that the correct length of the queue
    assert_eq!(queue.len(), size);

    // Rebase beyond the current boundaries triggering a reset
    let base = 2*size as ZInt;
    queue.set_base(base);
    assert_eq!(queue.get_base(), base);

    // Verify that the correct length of the queue
    assert_eq!(queue.len(), 0);

    // Verify that the mask is correct
    let mask = 0b0;
    assert_eq!(queue.get_mask(), mask);
}

#[test]
fn ordered_queue_remove() {
    let size = 8;
    let mut queue: OrderedQueue<ZInt> = OrderedQueue::new(size);

    // Fill the queue
    for i in 0..(size as ZInt) {
        // Push the element on the queue
        let res = queue.try_push(i, i);
        assert!(res.is_none());
    }

    // Verify that the correct length of the queue
    assert_eq!(queue.len(), size);

    // Drain the queue
    let res = queue.try_remove(7 as ZInt);
    assert_eq!(res, Some(7));
    assert_eq!(queue.len(), 7);

    let res = queue.try_remove(5 as ZInt);
    assert_eq!(res, Some(5));
    assert_eq!(queue.len(), 6);

    let res = queue.try_remove(3 as ZInt);
    assert_eq!(res, Some(3));
    assert_eq!(queue.len(), 5);

    let res = queue.try_remove(1 as ZInt);
    assert_eq!(res, Some(1));
    assert_eq!(queue.len(), 4);
 
    let res = queue.try_remove(0 as ZInt);
    assert_eq!(res, Some(0));
    assert_eq!(queue.len(), 3);
    
    let res = queue.try_remove(2 as ZInt);
    assert_eq!(res, Some(2));
    assert_eq!(queue.len(), 2);
        
    let res = queue.try_remove(4 as ZInt);
    assert_eq!(res, Some(4));
    assert_eq!(queue.len(), 1);
        
    let res = queue.try_remove(6 as ZInt);
    assert_eq!(res, Some(6));
    assert_eq!(queue.len(), 0);

    // Check that everything is None
    for i in 0..(size as ZInt) {
        // Push the element on the queue
        let res = queue.try_remove(i);
        assert!(res.is_none());
    }

    // Check that the base is 0
    assert_eq!(queue.get_base(), 0);
}
