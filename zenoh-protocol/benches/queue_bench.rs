#[macro_use]
extern crate criterion;

use async_std::sync::{
    Arc,
    channel,
    Barrier,
    Mutex,
    Sender
};
use async_std::task;
use criterion::{
    BenchmarkId,
    Criterion
};
use std::time::{
    Duration,
    Instant
};

use zenoh_protocol::core::ResKey;
use zenoh_protocol::io::ArcSlice;
use zenoh_protocol::proto::{
    Message,
    MessageKind
};
use zenoh_protocol::session::{
    MessageTx,
    QueuePrio,
    QueueInner
};


struct ChannelBench {
    sender_producers: Vec<Sender<u64>>,
    sender_consumer: Sender<u64>,
    barrier_start: Arc<Barrier>,
    barrier_end: Arc<Barrier>
}

impl ChannelBench {
    fn new(size: usize, producers: usize, messages: usize) -> Self {
        let (schedule, consume) = channel::<Message>(size);
        let mut sender_producers: Vec<Sender<u64>> = Vec::new();

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

        // The test barriers
        let barrier_start = Arc::new(Barrier::new(producers + 2));
        let barrier_end = Arc::new(Barrier::new(2));

        // Build the producers tasks
        for _ in 0..producers {
            let (sender, receiver) = channel::<u64>(1);
            sender_producers.push(sender);

            let c_schedule = schedule.clone();
            let c_message = message_reliable.clone();
            let c_barrier_start = barrier_start.clone();
            task::spawn(async move {
                // The total amount of messages to send is shared among all the producers
                let tot_msg = messages/producers;
                loop {        
                    // Future to wait for the benchmark signal
                    let signal = receiver.recv().await;
                    // Wait for the test signal
                    if let Some(iters) = signal {
                        // Start signal received, execute the test
                        if iters > 0 {
                            // Synchronize with the consumer task
                            c_barrier_start.wait().await;
                            // Iterate over the number of iteration as requested by Criterion
                            for _ in 0u64..iters {
                                // Send the messages for a single iteration
                                for _ in 0..tot_msg {
                                    c_schedule.send(c_message.clone()).await;
                                }
                            }
                        } else {
                            // Stop signal received, break
                            break
                        }
                    } else {
                        // Error on the channel, break
                        break
                    }
                }
            });
        }
        
        // Build the consumer task
        let (sender, receiver) = channel::<u64>(1);
        let sender_consumer = sender;

        let c_barrier_end = barrier_end.clone();
        let c_barrier_start = barrier_start.clone();
        task::spawn(async move {
            loop {
                // Future to wait for the benchmark signal
                let signal = receiver.recv().await;
                // Wait for the test signal
                if let Some(iters) = signal {
                    // Start signal received, execute the test
                    if iters > 0 {
                        // Synchronize with the producers tasks
                        c_barrier_start.wait().await;
                        // Iterate over the number of iteration as requested by Criterion
                        for _ in 0u64..iters { 
                            // Receive all the message of a single interation
                            let mut count: usize = 0;
                            while count < messages {
                                consume.recv().await.unwrap();
                                count += 1;
                            }
                        }
                        // Notify the end of a test
                        c_barrier_end.wait().await;
                    } else {
                        // Stop signal received, break
                        break
                    }
                } else {
                    // Error on the channel, break
                    break
                }             
            }
        });

        Self {
            sender_producers,
            sender_consumer,
            barrier_start,
            barrier_end
        }
    }

    // Notify all the tasks we are ready to test
    fn ready(&self, iters: u64) {
        task::block_on(async {
            for i in 0usize..self.sender_producers.len() {
                self.sender_producers[i].send(iters).await;
            }
            self.sender_consumer.send(iters).await;
        })
    }

    // Start the test on all the tasks
    fn execute(&self) {
        task::block_on(async {
            self.barrier_start.wait().await;
            self.barrier_end.wait().await;
        })
    }
}

impl Drop for ChannelBench {
    fn drop(&mut self) {
        // Stop all the tasks to not pollute the async scheduler
        task::block_on(async {
            for sender in self.sender_producers.drain(..) {
                sender.send(0).await;
            }
        });
    }
}




struct QueuePrioBench {
    sender_producers: Vec<Sender<u64>>,
    sender_consumer: Sender<u64>,
    barrier_start: Arc<Barrier>,
    barrier_end: Arc<Barrier>
}

impl QueuePrioBench {
    fn new(queues: usize, capacity: usize, producers: usize, messages: usize) -> Self {
        let inner = QueueInner::<MessageTx>::new(queues, capacity);
        let mutex = Arc::new(QueuePrio::new(inner, queues));
        let mut sender_producers: Vec<Sender<u64>> = Vec::new();

        // The test barriers
        let barrier_start = Arc::new(Barrier::new(producers + 2));
        let barrier_end = Arc::new(Barrier::new(2));

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

        // Build the producers tasks
        for p in 0..producers {
            let (sender, receiver) = channel::<u64>(1);
            sender_producers.push(sender);

            let c_mutex = mutex.clone();
            let c_message = message_reliable.clone();
            let c_barrier_start = barrier_start.clone();
            task::spawn(async move {
                // The total amount of messages to send is shared among all the producers
                let tot_msg = messages/producers;
                loop {        
                    // Future to wait for the benchmark signal
                    let signal = receiver.recv().await;
                    // Wait for the test signal
                    if let Some(iters) = signal {
                        println!("=== SIGNAL! {} ===", p);
                        // Start signal received, execute the test
                        if iters > 0 {
                            // Synchronize with the consumer task
                            c_barrier_start.wait().await;
                            // Iterate over the number of iteration as requested by Criterion
                            for i in 0u64..iters {
                                // Send the messages for a single iteration
                                for m in 0usize..tot_msg {
                                    let to_send = MessageTx {
                                        inner: c_message.clone(),
                                        link: None,
                                        notify: None
                                    };
                                    let mut guard = c_mutex.lock_push(0).await;
                                    guard.push(to_send, 0);
                                    // println!("\tPUSH! P: {}, I: {}, M: {}", p, i, m);
                                    // Update the bitmask on the queue having messages
                                    let (mask_has_messages, mask_is_full) = guard.get_mask();
                                    c_mutex.update_masks(mask_has_messages, mask_is_full);
                                }
                            }
                        } else {
                            // Stop signal received, break
                            break
                        }
                    } else {
                        // Error on the channel, break
                        break
                    }
                }
            });
        }

        // Build the consumer task
        let (sender, receiver) = channel::<u64>(1);
        let sender_consumer = sender;

        let c_mutex = mutex.clone();
        let c_barrier_end = barrier_end.clone();
        let c_barrier_start = barrier_start.clone();
        task::spawn(async move {
            loop {
                // Future to wait for the benchmark signal
                let signal = receiver.recv().await;
                // Wait for the test signal
                if let Some(iters) = signal {
                    // Start signal received, execute the test
                    if iters > 0 {
                        // Synchronize with the producers tasks
                        c_barrier_start.wait().await;
                        // Iterate over the number of iteration as requested by Criterion
                        for _ in 0u64..iters { 
                            // Receive all the message of a single interation
                            let mut count: usize = 0;
                            while count < messages {
                                let mut guard = c_mutex.lock_pop().await;
                                guard.pop();
                                let (mask_has_messages, mask_is_full) = guard.get_mask();
                                c_mutex.update_masks(mask_has_messages, mask_is_full);
                                count += 1;
                            }
                        }
                        // Notify the end of a test
                        c_barrier_end.wait().await;
                        println!("\tEND BARRIER!");
                    } else {
                        // Stop signal received, break
                        break
                    }
                } else {
                    // Error on the channel, break
                    break
                }             
            }
        });

        Self {
            sender_producers,
            sender_consumer,
            barrier_start,
            barrier_end
        }
    }

    // Notify all the tasks we are ready to test
    fn ready(&self, iters: u64) {
        task::block_on(async {
            for i in 0usize..self.sender_producers.len() {
                self.sender_producers[i].send(iters).await;
            }
            self.sender_consumer.send(iters).await;
        })
    }

    // Start the test on all the tasks
    fn execute(&self) {
        task::block_on(async {
            self.barrier_start.wait().await;
            self.barrier_end.wait().await;
        })
    }
}

impl Drop for QueuePrioBench {
    fn drop(&mut self) {
        // Stop all the tasks to not pollute the async scheduler
        task::block_on(async {
            for sender in self.sender_producers.drain(..) {
                sender.send(0).await;
            }
            self.sender_consumer.send(0).await;
        });
    }
}



struct MutexBench {
    sender_producers: Vec<Sender<u64>>,
    barrier_start: Arc<Barrier>,
    barrier_end: Arc<Barrier>
}

impl MutexBench {
    fn new(queues: usize, capacity: usize, producers: usize, messages: usize) -> Self {
        let mutex = Arc::new(Mutex::new(QueueInner::<MessageTx>::new(queues, capacity)));
        let mut sender_producers: Vec<Sender<u64>> = Vec::new();

        // The test barriers
        let barrier_start = Arc::new(Barrier::new(producers + 1));
        let barrier_end = Arc::new(Barrier::new(producers + 1));

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

        // Build the producers tasks
        for _ in 0..producers {
            let (sender, receiver) = channel::<u64>(1);
            sender_producers.push(sender);

            let c_mutex = mutex.clone();
            let _c_message = message_reliable.clone();
            let c_barrier_start = barrier_start.clone();
            let c_barrier_end = barrier_end.clone();
            task::spawn(async move {
                // The total amount of messages to send is shared among all the producers
                let tot_msg = messages/producers;
                loop {        
                    // Future to wait for the benchmark signal
                    let signal = receiver.recv().await;
                    // Wait for the test signal
                    if let Some(iters) = signal {
                        // Start signal received, execute the test
                        if iters > 0 {
                            // Synchronize with the consumer task
                            c_barrier_start.wait().await;
                            // Iterate over the number of iteration as requested by Criterion
                            for _ in 0u64..iters {
                                // Send the messages for a single iteration
                                for _ in 0usize..tot_msg {
                                    // let to_send = MessageTx {
                                    //     inner: c_message.clone(),
                                    //     link: None,
                                    //     notify: None
                                    // };
                                    // let mut guard = c_mutex.lock().await;
                                    // guard.data.push(to_send);
                                    // guard.data.pop();
                                    c_mutex.lock().await;
                                }
                            }
                            c_barrier_end.wait().await;
                        } else {
                            // Stop signal received, break
                            break
                        }
                    } else {
                        // Error on the channel, break
                        break
                    }
                }
            });
        }

        Self {
            sender_producers,
            barrier_start,
            barrier_end
        }
    }

    // Notify all the tasks we are ready to test
    fn ready(&self, iters: u64) {
        task::block_on(async {
            for i in 0usize..self.sender_producers.len() {
                self.sender_producers[i].send(iters).await;
            }
        })
    }

    // Start the test on all the tasks
    fn execute(&self) {
        task::block_on(async {
            self.barrier_start.wait().await;
            self.barrier_end.wait().await;
        })
    }
}

impl Drop for MutexBench {
    fn drop(&mut self) {
        // Stop all the tasks to not pollute the async scheduler
        task::block_on(async {
            for sender in self.sender_producers.drain(..) {
                sender.send(0).await;
            }
        });
    }
}




// struct QueueBench {
//     sender_producers: Vec<Sender<u64>>,
//     sender_consumer: Sender<u64>,
//     barrier_start: Arc<Barrier>,
//     barrier_end: Arc<Barrier>
// }

// impl QueueBench {
//     fn new(size: usize, producers: usize, messages: usize) -> Self {
//         let queue = Arc::new(QueueTx::new(size));
//         let mut sender_producers: Vec<Sender<u64>> = Vec::new();

//         // Build reliable data messages of 64 bytes payload
//         let kind = MessageKind::FullMessage;
//         let reliable = true;
//         let sn = 0;
//         let key = ResKey::RName("test".to_string());
//         let info = None;
//         let payload = ArcSlice::new(Arc::new(vec![0u8; 64]), 0, 1);
//         let reply_context = None;
//         let cid = None;
//         let properties = None;
//         let message_reliable = Message::make_data(kind, reliable, sn, key, info, payload, reply_context, cid, properties);  

//         // The test barriers
//         let barrier_start = Arc::new(Barrier::new(producers + 2));
//         let barrier_end = Arc::new(Barrier::new(2));

//         // Build the producers tasks
//         for _ in 0..producers {
//             let (sender, receiver) = channel::<u64>(1);
//             sender_producers.push(sender);

//             let c_queue = queue.clone();
//             let c_message = message_reliable.clone();
//             let c_barrier_start = barrier_start.clone();
//             task::spawn(async move {
//                 // The total amount of messages to send is shared among all the producers
//                 let tot_msg = messages/producers;
//                 loop {        
//                     // Future to wait for the benchmark signal
//                     let signal = receiver.recv().await;
//                     // Wait for the test signal
//                     if let Some(iters) = signal {
//                         // Start signal received, execute the test
//                         if iters > 0 {
//                             // Synchronize with the consumer task
//                             c_barrier_start.wait().await;
//                             // Iterate over the number of iteration as requested by Criterion
//                             for _ in 0u64..iters {
//                                 // Send the messages for a single iteration
//                                 for _ in 0..tot_msg {
//                                     let to_send = MessageTxPush {
//                                         inner: c_message.clone(),
//                                         link: None,
//                                         notify: None
//                                     };
//                                     c_queue.push(to_send).await;
//                                 }
//                             }
//                         } else {
//                             // Stop signal received, break
//                             break
//                         }
//                     } else {
//                         // Error on the channel, break
//                         break
//                     }
//                 }
//             });
//         }
        
//         // Build the consumer task
//         let (sender, receiver) = channel::<u64>(1);
//         let sender_consumer = sender;

//         let c_barrier_end = barrier_end.clone();
//         let c_barrier_start = barrier_start.clone();
//         task::spawn(async move {
//             loop {
//                 // Future to wait for the benchmark signal
//                 let signal = receiver.recv().await;
//                 // Wait for the test signal
//                 if let Some(iters) = signal {
//                     // Start signal received, execute the test
//                     if iters > 0 {
//                         // Synchronize with the producers tasks
//                         c_barrier_start.wait().await;
//                         // Iterate over the number of iteration as requested by Criterion
//                         for _ in 0u64..iters { 
//                             // Receive all the message of a single interation
//                             let mut count: usize = 0;
//                             while count < messages {
//                                 queue.pop().await;
//                                 count += 1;
//                             }
//                         }
//                         // Notify the end of a test
//                         c_barrier_end.wait().await;
//                     } else {
//                         // Stop signal received, break
//                         break
//                     }
//                 } else {
//                     // Error on the channel, break
//                     break
//                 }             
//             }
//         });

//         Self {
//             sender_producers,
//             sender_consumer,
//             barrier_start,
//             barrier_end
//         }
//     }

//     // Notify all the tasks we are ready to test
//     fn ready(&self, iters: u64) {
//         task::block_on(async {
//             for i in 0usize..self.sender_producers.len() {
//                 self.sender_producers[i].send(iters).await;
//             }
//             self.sender_consumer.send(iters).await;
//         })
//     }

//     // Start the test on all the tasks
//     fn execute(&self) {
//         task::block_on(async {
//             self.barrier_start.wait().await;
//             self.barrier_end.wait().await;
//         })
//     }
// }

// impl Drop for QueueBench {
//     fn drop(&mut self) {
//         // Stop all the tasks to not pollute the async scheduler
//         task::block_on(async {
//             for sender in self.sender_producers.drain(..) {
//                 sender.send(0).await;
//             }
//             self.sender_consumer.send(0).await;
//         });
//     }
// }


fn queue_prio(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue_prio");

    // Number of messages to send for each Criterion iteration
    let messages = 10_000;

    // Test the mutex with different number of concurrent producers
    for producers in [4, 8].iter() {
        for queues in [1].iter() {
            for capacity in [16, 1024].iter() {
                // Build the mutex bench
                let test = QueuePrioBench::new(*queues, *capacity, *producers, messages);
                // Assign an ID to this scenario
                let scenario = format!("queues: {}, capacity: {}, producers: {}, msg/iter: {}", queues, capacity, producers, messages);
                let id = BenchmarkId::new("reliable", scenario);
                // Tell Criterion the test to execute the test
                group.bench_function(id, move |b| b.iter_custom(|iters| {
                    // Notify the tasks to get ready for a test
                    test.ready(iters);
                    // Mark the starting time of a test
                    let start = Instant::now();
                    // Execute the test
                    test.execute();
                    // Mesure the elapsed time
                    start.elapsed()
                }));
            }
        }
    }

    // Clean this benchmark group on Criterion
    group.finish();
}


fn queue_mutex(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue_mutex");

    // Number of messages to send for each Criterion iteration
    let messages = 1_000;

    // Test the mutex with different number of concurrent producers
    for producers in [1, 2, 4, 8, 16].iter() {
        // Build the mutex bench
        let test = MutexBench::new(1, 16, *producers, messages);
        // Assign an ID to this scenario
        let scenario = format!("producers: {}, msg/iter: {}", producers, messages);
        let id = BenchmarkId::new("reliable", scenario);
        // Tell Criterion the test to execute the test
        group.bench_function(id, move |b| b.iter_custom(|iters| {
            // Notify the tasks to get ready for a test
            test.ready(iters);
            // Mark the starting time of a test
            let start = Instant::now();
            // Execute the test
            test.execute();
            // Mesure the elapsed time
            start.elapsed()
        }));
    }

    // Clean this benchmark group on Criterion
    group.finish();
}

fn queue_channel(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue_channel");
    // Set the warm up time to 10s
    group.warm_up_time(Duration::from_secs(10));
    // Set the measurement time to 10s
    group.measurement_time(Duration::from_secs(10));

    // Number of messages to send for each Criterion iteration
    let messages = 1_000;

    // ************************ //
    //     Reliable messages    //
    // ************************ //
    // Test the queue for different buffer sizes
    for size in [64, 1024].iter() {
        // Test the queue with different number of concurrent producers
        for producers in [1, 4, 8].iter() {
            // Build the queue bench
            let test = ChannelBench::new(*size, *producers, messages);
            // Assign an ID to this scenario
            let scenario = format!("ch size: {}, producers: {}, msg/iter: {}", size, producers, messages);
            let id = BenchmarkId::new("reliable", scenario);
            // Tell Criterion the test to execute the test
            group.bench_function(id, move |b| b.iter_custom(|iters| {
                // Notify the tasks to get ready for a test
                test.ready(iters);
                // Mark the starting time of a test
                let start = Instant::now();
                // Execute the test
                test.execute();
                // Mesure the elapsed time
                start.elapsed()
            }));
        }
    }

    // Clean this benchmark group on Criterion
    group.finish();
}


// fn queue_throughput(c: &mut Criterion) {
//     let mut group = c.benchmark_group("queue_throughput");
//     // Set the warm up time to 10s
//     group.warm_up_time(Duration::from_secs(10));
//     // Set the measurement time to 10s
//     group.measurement_time(Duration::from_secs(10));

//     // Number of messages to send for each Criterion iteration
//     let messages = 1_000;

//     // ************************ //
//     //     Reliable messages    //
//     // ************************ //
//     // Test the queue for different buffer sizes
//     for size in [64, 128, 256, 512, 1024].iter() {
//         // Test the queue with different number of concurrent producers
//         for producers in [1, 2, 4].iter() {
//             // Build the queue bench
//             let test = QueueBench::new(*size, *producers, messages);
//             // Assign an ID to this scenario
//             let scenario = format!("queue size: {}, producers: {}, msg/iter: {}", size, producers, messages);
//             let id = BenchmarkId::new("reliable", scenario);
//             // Tell Criterion the test to execute the test
//             group.bench_function(id, move |b| b.iter_custom(|iters| {
//                 // Notify the tasks to get ready for a test
//                 test.ready(iters);
//                 // Mark the starting time of a test
//                 let start = Instant::now();
//                 // Execute the test
//                 test.execute();
//                 // Mesure the elapsed time
//                 start.elapsed()
//             }));
//         }
//     }

//     // Clean this benchmark group on Criterion
//     group.finish();
// }

criterion_group!(benches, queue_mutex, queue_prio, queue_channel);
criterion_main!(benches);
