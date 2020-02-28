use async_std::prelude::*;
use async_std::sync::{
    Arc,
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
    Locator,
    Message,
    MessageKind
};
use crate::session::{
    SessionCallback,
    Session
};
use crate::session::queue::{
    Queue,
    QueueError,
    QueueMessage
};
use crate::session::link::Link;


// Constants
const QUEUE_LIM: usize = 16;


async fn consume_loop(transport: Arc<Transport>) {
    async fn consume(transport: &Arc<Transport>) -> Option<bool> {
        let (message, _priority, link) = transport.consume().await;
        match link {
            // Send the message to the indicated link
            Some((src, dst)) => {
                let guard = transport.link.lock().await;
                if let Some(index) = transport.find_link(&guard, &src, &dst) {
                    let _ = guard[index].send(&message).await;
                }
                Some(true)
            },
            None => {
                // Send the message only on the first link
                for l in transport.link.lock().await.iter() {
                    let _ = l.send(&message).await;
                    break
                }
                Some(true)
            }
        }
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
    session: RwLock<Option<Arc<Session>>>,
    // The callback for Data messages
    callback: Arc<dyn SessionCallback + Send + Sync>,
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
    pub fn new(lease: ZInt, callback: Arc<dyn SessionCallback + Send + Sync>) -> Self {
        let (sender, receiver) = channel::<bool>(1);
        Self {
            callback,
            lease: AtomicU64::new(lease),
            link: Mutex::new(Vec::new()),
            session: RwLock::new(None), 
            queue: Queue::new(QUEUE_LIM),
            s_waker: SegQueue::new(),
            c_waker: AtomicCell::new(None),
            ch_send: sender,
            ch_recv: receiver
        }
    }

    pub fn get_lease(&self) -> ZInt {
        self.lease.load(Ordering::Acquire)
    }

    pub fn set_lease(&self, lease: ZInt) {
        self.lease.store(lease, Ordering::Release)
    }

    pub fn get_session(&self) -> Arc<Session> {
        zrwopt!(self.session).clone()
    }

    // This function is expected to be called during the construction
    // of a session object, never at runtime
    pub fn set_session(&self, session: &Arc<Session>) {
        *self.session.try_write().unwrap() = Some(session.clone())
    }

    pub fn start(transport: Arc<Transport>) {
        task::spawn(async move {
            consume_loop(transport).await;
        });
    }

    pub async fn stop(&self) {
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

    pub async fn add_link(&self, link: Link) -> ZResult<()> {
        // println!("Adding link {} => {} to {:?}", link.get_src(), link.get_dst(), self.get_peer());
        let mut guard = self.link.lock().await;
        match self.find_link(&guard, &link.get_src(), &link.get_dst()) {
            Some(_) => Err(zerror!(ZErrorKind::Other{
                descr: format!("Trying to delete a link that does not exist!")
            })),
            None => Ok(guard.push(link))
        }
    }

    pub async fn del_link(&self, src: &Locator, dst: &Locator, _reason: Option<ZError>) -> ZResult<Link> {    
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
    /*           SCHEDULE OUT            */
    /*************************************/
    pub async fn schedule<'a>(&self, message: Arc<Message>, priority: Option<usize>, link: Option<(Locator, Locator)>) -> usize {
        struct FutureSchedule<'a> {
            session: &'a Transport,
            message: QueueMessage
        }
        
        impl<'a> Future for FutureSchedule<'a> {
            type Output = usize;
        
            fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
                self.session.s_waker.push(ctx.waker().clone());
                match self.session.queue.push(self.message.clone()) {
                    Ok(()) => Poll::Ready(0),
                    Err(QueueError::IsEmpty) => panic!("This should not happen!!!"),
                    Err(QueueError::IsFull) => Poll::Pending
                }
            }
        }

        impl Drop for FutureSchedule<'_> {
            fn drop(&mut self) {
                if let Some(waker) = self.session.c_waker.take() {
                    waker.wake();
                }
            }
        }

        FutureSchedule {
            session: self,
            message: (message, priority, link)
        }.await;
        
        // println!("{:?} SCHEDULE", self.session.inner.whatami);
        // TO FIX
        // We need to return the message sequence number
        0
    }

    pub async fn consume(&self) -> QueueMessage {
        struct FutureConsume<'a> {
            session: &'a Transport
        }
        
        impl<'a> Future for FutureConsume<'a> {
            type Output = QueueMessage;
        
            fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
                self.session.c_waker.store(Some(ctx.waker().clone()));
                match self.session.queue.pop() {
                    Ok(msg) => Poll::Ready(msg),
                    Err(QueueError::IsEmpty) => Poll::Pending,
                    Err(QueueError::IsFull) => panic!("This should not happen!!!")
                }
            }
        }

        impl Drop for FutureConsume<'_> {
            fn drop(&mut self) {
                while let Ok(waker) = self.session.s_waker.pop() {
                    waker.wake();
                }
            }
        }

        FutureConsume {
            session: self
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
            // Body::Close{pid, reason} => {},
            Body::Close{..} => {},
            // Body::Data{reliable, sn, key, info, payload} => {
            Body::Data{..} => {
                self.callback.receive_message(message).await?;
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

    async fn receive_first_fragement(&self, _src: &Locator, _dst: &Locator, _message: Message, _number: Option<ZInt>) -> Result<(), ZError> {
        unimplemented!("Defragementation not implemented yet!");
    }

    async fn receive_middle_fragement(&self, _src: &Locator, _dst: &Locator, _message: Message) -> Result<(), ZError> {
        unimplemented!("Defragementation not implemented yet!");
    }

    async fn receive_last_fragement(&self, _src: &Locator, _dst: &Locator, _message: Message) -> Result<(), ZError> {
        unimplemented!("Defragementation not implemented yet!");
    }

    pub async fn receive_message(&self, src: &Locator, dst: &Locator, message: Message) -> Result<(), ZError> {
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
