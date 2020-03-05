use async_std::prelude::*;
use async_std::sync::{
    Arc,
    Barrier,
    channel,
    Mutex,
    MutexGuard,
    Receiver,
    RwLock,
    Sender,
};
use async_std::task;
use async_std::task::{
    Context, 
    Poll,
    Waker
};
use crossbeam::atomic::AtomicCell;
use crossbeam::queue::SegQueue;
use std::fmt;
use std::sync::atomic::{
    AtomicU64,
    Ordering
};
use std::pin::Pin;

use crate::{
    zerror,
    zrwopt
};
use crate::core::{
    ZError,
    ZErrorKind,
    ZInt,
    ZResult
};
use crate::proto::{
    Body,
    Message,
    MessageKind
};
use crate::session::{
    MsgHandler,
    DummyHandler, 
    Session
};
use crate::session::queue::{
    Queue,
    QueueMessage
};
use crate::link::{
    Link,
    Locator
};


// Constants
const QUEUE_SIZE: usize = 16;


async fn consume_loop(transport: Arc<Transport>) {
    async fn consume(transport: &Arc<Transport>) -> Option<bool> {
        let message = transport.consume().await;
        match &message.link {
            // Send the message to the indicated link
            Some((src, dst)) => {
                let guard = transport.link.lock().await;
                if let Some(index) = transport.find_link(&guard, &src, &dst) {
                    let _ = guard[index].send(&message.message).await;
                }
            },
            None => {
                // Send the message only on the first link
                for l in transport.link.lock().await.iter() {
                    let _ = l.send(&message.message).await;
                    break
                }
            }
        }
        if let Some(barrier) = &message.barrier {
            barrier.wait().await;
        }
        Some(true)
    }
    
    loop {
        let stop = transport.ch_recv.recv();
        let consume = consume(&transport);
        match consume.race(stop).await {
            Some(true) => continue,
            Some(false) => break,
            None => break
        }
    }
}


pub struct Transport {
    // The reference to the session
    pub(crate) session: RwLock<Option<Arc<Session>>>,
    // The callback for Data messages
    callback: RwLock<Arc<dyn MsgHandler + Send + Sync>>,
    // The timeout after which the session is closed if no messages are received
    lease: AtomicU64,
    // The list of transport links associated to this session
    link: Mutex<Vec<Link>>,
    // The queue of messages to be transmitted
    queue: Queue,
    // The schedule and consume wakers for the schedule future
    s_waker: SegQueue<Waker>,
    c_waker: AtomicCell<Option<Waker>>,
    // The channel endpoints for terminating the consume_loop task
    ch_send: Sender<bool>,
    ch_recv: Receiver<bool>
}

impl Transport {
    pub(crate) fn new(lease: ZInt) -> Self {
        let (sender, receiver) = channel::<bool>(1);
        Self {
            callback: RwLock::new(Arc::new(DummyHandler::new())), 
            lease: AtomicU64::new(lease),
            link: Mutex::new(Vec::new()),
            session: RwLock::new(None), 
            queue: Queue::new(QUEUE_SIZE),
            s_waker: SegQueue::new(),
            c_waker: AtomicCell::new(None),
            ch_send: sender,
            ch_recv: receiver
        }
    }

    pub(crate) fn initialize(&self, session: Arc<Session>, callback: Arc<dyn MsgHandler + Send + Sync>) {
        *self.session.try_write().unwrap() = Some(session);
        *self.callback.try_write().unwrap() = callback;
    }

    pub(crate) fn get_lease(&self) -> ZInt {
        self.lease.load(Ordering::Acquire)
    }

    pub(crate) fn set_lease(&self, lease: ZInt) {
        self.lease.store(lease, Ordering::Release)
    }

    pub(crate) async fn get_links(&self) -> Vec<Link> {
        let mut vec = Vec::new();
        for l in self.link.lock().await.iter() {
            vec.push(l.clone());
        }
        vec
    }

    pub(crate) fn start(transport: Arc<Transport>) {
        task::spawn(async move {
            consume_loop(transport).await;
        });
    }

    pub(crate) async fn stop(&self) {
        self.ch_send.send(false).await;
    }


    /*************************************/
    /*               LINK                */
    /*************************************/
    fn find_link(&self, guard: &MutexGuard<'_, Vec<Link>>, src: &Locator, dst: &Locator) -> Option<usize> {
        let mut found = false;
        let mut index: usize = 0;

        for i in 0..guard.len() {
            if guard[i].get_src() == *src &&
                    guard[i].get_dst() == *dst {
                found = true;
                index = i;
                break
            }
        }

        match found {
            true => Some(index),
            false => None
        }
    }

    pub(crate) async fn add_link(&self, link: Link) -> ZResult<()> {
        // println!("Adding link {} => {} to {:?}", link.get_src(), link.get_dst(), self.get_peer());
        let mut guard = self.link.lock().await;
        match self.find_link(&guard, &link.get_src(), &link.get_dst()) {
            Some(_) => Err(zerror!(ZErrorKind::Other{
                descr: format!("Trying to delete a link that does not exist!")
            })),
            None => Ok(guard.push(link))
        }
    }

    pub(crate) async fn del_link(&self, src: &Locator, dst: &Locator, _reason: Option<ZError>) -> ZResult<Link> {    
        // println!("Deleting link {} => {} from {:?}", src, dst, self.get_peer());
        let mut guard = self.link.lock().await;
        match self.find_link(&guard, src, dst) {
            Some(index) => Ok(guard.remove(index)),
            None => Err(zerror!(ZErrorKind::Other{
                descr: format!("Trying to delete a link that does not exist!")
            }))
        }
    }

    // async fn link_error(&self, src: &Locator, dst: &Locator, reason: Option<ZError>) -> Result<(), ZError> {
    //     if self.link.lock().await.len() == 0 {
    //         let err = match reason {
    //             Some(err) => err,
    //             None => zerror!(ZErrorKind::Other{
    //                 msg: format!("No links left in session {}", self.id)
    //             })
    //         };
    //         // Stop the task
    //         self.ch_sender.send(false).await;
    //         self.manager.del_session(self.id, Some(err)).await?;
    //     }
    //     return Ok(())
    // }


    /*************************************/
    /*             SCHEDULE              */
    /*************************************/

    // Schedule the message to be sent asynchronsly
    pub(crate) async fn schedule(&self, message: Arc<Message>, 
        priority: Option<usize>, link: Option<(Locator, Locator)>
    ) -> usize {
        self.inner_schedule(message, priority, link, None).await
    }

    // Send the message in a synchronous way. 
    // 1) Schedule the message 
    // 2) Be notified when the message is actually sent
    pub(crate) async fn send(&self, message: Arc<Message>, 
        priority: Option<usize>, link: Option<(Locator, Locator)>
    ) -> usize {
        let barrier = Arc::new(Barrier::new(2));
        let sn = self.inner_schedule(message, priority, link, Some(barrier.clone())).await;
        barrier.wait().await;
        sn
    }

    async fn inner_schedule(&self, message: Arc<Message>, 
        priority: Option<usize>, link: Option<(Locator, Locator)>,
        barrier: Option<Arc<Barrier>>
    ) -> usize {
        struct FutureSchedule<'a> {
            transport: &'a Transport,
            message: Arc<QueueMessage>
        }
        
        impl<'a> Future for FutureSchedule<'a> {
            type Output = usize;
        
            fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
                self.transport.s_waker.push(ctx.waker().clone());
                match self.transport.queue.push(self.message.clone()) {
                    true => Poll::Ready(0),
                    false => Poll::Pending
                }
            }
        }

        impl Drop for FutureSchedule<'_> {
            fn drop(&mut self) {
                if let Some(waker) = self.transport.c_waker.take() {
                    waker.wake();
                }
            }
        }

        FutureSchedule {
            transport: self,
            message: Arc::new(QueueMessage {
                message, priority, link, barrier
            })
        }.await;
        
        // println!("{:?} SCHEDULE", self.session.inner.whatami);
        // TO FIX
        // We need to return the message sequence number
        0
    }

    pub async fn consume(&self) -> Arc<QueueMessage> {
        struct FutureConsume<'a> {
            transport: &'a Transport
        }
        
        impl<'a> Future for FutureConsume<'a> {
            type Output = Arc<QueueMessage>;
        
            fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
                self.transport.c_waker.store(Some(ctx.waker().clone()));
                match self.transport.queue.pop() {
                    Some(msg) => Poll::Ready(msg),
                    None => Poll::Pending
                }
            }
        }

        impl Drop for FutureConsume<'_> {
            fn drop(&mut self) {
                while let Ok(waker) = self.transport.s_waker.pop() {
                    waker.wake();
                }
            }
        }

        FutureConsume {
            transport: self
        }.await
    }

    /*************************************/
    /*   MESSAGE RECEIVE FROM THE LINK   */
    /*************************************/
    async fn receive_full_message(&self, src: &Locator, dst: &Locator, message: Message) -> ZResult<()> {
        match &message.body {
            Body::Accept{opid, apid, lease} => {
                zrwopt!(self.session).process_accept(src, dst, opid, apid, lease).await?;
            },
            // Body::AckNack{sn, mask} => {},
            Body::AckNack{..} => {},
            Body::Close{pid, reason} => {
                zrwopt!(self.session).process_close(src, dst, pid, reason).await?;
            },
            // Body::Data{reliable, sn, key, info, payload} => {
            Body::Data{..} => {
                self.callback.read().await.handle_message(message).await?;
            },
            // Body::Declare{sn, declarations} => {},
            Body::Declare{..} => {},
            // Body::Hello{whatami, locators} => {},
            Body::Hello{..} => {},
            // Body::KeepAlive{pid} => {},
            Body::KeepAlive{..} => {},
            Body::Open{version, whatami, pid, lease, locators} => {
                zrwopt!(self.session).process_open(src, dst, version, whatami, pid, lease, locators).await?;
            },
            // Body::Ping{hash} => {},
            Body::Ping{..} => {},
            // Body::Pong{hash} => {},
            Body::Pong{..} => {},
            // Body::Pull{sn, key, pull_id, max_samples} => {},
            Body::Pull{..} => {},
            // Body::Query{sn, key, predicate, qid, target, consolidation} => {},
            Body::Query{..} => {},
            // Body::Scout{what} => {},
            Body::Scout{..} => {},
            // Body::Sync{sn, count} => {}
            Body::Sync{..} => {}
        }
        Ok(())
    }

    async fn receive_first_fragement(&self, _src: &Locator, _dst: &Locator, _message: Message, _number: Option<ZInt>) -> ZResult<()> {
        unimplemented!("Defragementation not implemented yet!");
    }

    async fn receive_middle_fragement(&self, _src: &Locator, _dst: &Locator, _message: Message) -> ZResult<()> {
        unimplemented!("Defragementation not implemented yet!");
    }

    async fn receive_last_fragement(&self, _src: &Locator, _dst: &Locator, _message: Message) -> ZResult<()> {
        unimplemented!("Defragementation not implemented yet!");
    }

    pub async fn receive_message(&self, src: &Locator, dst: &Locator, message: Message) -> ZResult<()> {
        match message.kind {
            MessageKind::FullMessage =>
                self.receive_full_message(src, dst, message).await?,
            MessageKind::FirstFragment{n} =>
                self.receive_first_fragement(src, dst, message, n).await?,
            MessageKind::InbetweenFragment => 
                self.receive_middle_fragement(src, dst, message).await?,
            MessageKind::LastFragment => 
                self.receive_last_fragement(src, dst, message).await?
        }
        Ok(())
    }

    /*************************************/
    /*         CLOSE THE SESSION         */
    /*************************************/
    pub async fn close(&self, _reason: Option<ZError>) -> ZResult<()> {
        // Stop the task
        self.stop().await;
        // Remove and close all the links
        for l in self.link.lock().await.drain(..) {
            l.close(None).await?;
        }
        // Remove the reference to the session
        *self.session.write().await = None;

        Ok(())
    }
}

// impl Drop for Transport {
//     fn drop(&mut self) {
//         println!("> Dropping Transport ({:?})", self);
//     }
// }

impl fmt::Debug for Transport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let links = task::block_on(async {
            let mut s = String::new();
            for l in self.link.lock().await.iter() {
                s.push_str(&format!("\n\t[({:?}) => ({:?})]", l.get_src(), l.get_dst()));
            }
            s
        });
        write!(f, "Links:{}", links)
    }
}
