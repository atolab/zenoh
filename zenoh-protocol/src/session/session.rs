use async_std::prelude::*;
use async_std::sync::{
    Arc,
    channel,
    Mutex,
    Receiver,
    Sender
};
use async_std::task;
use async_std::task::{
    Context, 
    Poll,
    Waker
};
use crossbeam::atomic::AtomicCell;
use crossbeam::queue::SegQueue;
use std::sync::atomic::{
    AtomicBool,
    AtomicU64,
    Ordering
};
use std::pin::Pin;

use crate::zerror;
use crate::core::{
    PeerId,
    ZError,
    ZErrorKind,
    ZInt
};
use crate::proto::{
    Body,
    Locator,
    Message,
    MessageKind
};
use crate::session::{
    ArcSelf,
    // EmptyCallback,
    Queue,
    QueueError,
    SessionManager,
    Link,
    SessionCallback
};


// Constants
const QUEUE_LIM: usize = 16;


async fn consume_loop(session: Arc<Session>, receiver: Receiver<bool>) {
    async fn consume(session: &Arc<Session>) -> Option<bool> {
        let message = session.consume().await;
        // Send the message only on the first link
        for l in session.link.lock().await.iter() {
            let _ = l.send(message.clone()).await;
            break
        }
        Some(true)
    }
    
    loop {
        let stop = receiver.recv();
        let consume = consume(&session);
        match consume.race(stop).await {
            Some(true) => continue,
            Some(false) => break,
            None => break
        }
    }
}


pub struct Session {
    // An Arc reference to Self
    arc: ArcSelf<Self>,
    // Session ID
    id: usize,
    // If session is active it can forward messages to the callback
    active: AtomicBool,
    // The callback for Data messages
    callback: Arc<dyn SessionCallback + Send + Sync>,
    // The peer this session is associated to
    peer: Mutex<Option<PeerId>>,
    // The timeout after which the session is closed if no messages are received
    lease: AtomicU64,
    // The list of transport links associated to this session
    link: Mutex<Vec<Arc<dyn Link + Send + Sync>>>,
    // The reference to the session manager
    manager: Arc<SessionManager>,
    // The queue of messages to be transmitted
    queue: Queue,
    // The schedule and consume wakers for the schedule future
    s_waker: SegQueue<Waker>,
    c_waker: AtomicCell<Option<Waker>>,
    // The channel endpoints for terminating the consume_loop task
    ch_sender: Sender<bool>,
    ch_receiver: Receiver<bool>
}

impl Session {
    pub fn new(id: usize, peer: Option<PeerId>, lease: ZInt, manager: Arc<SessionManager>,
        callback: Arc<dyn SessionCallback + Send + Sync>
    ) -> Arc<Self> {
        let (sender, receiver) = channel::<bool>(1);
        let s = Arc::new(Self {
            arc: ArcSelf::new(),
            id: id,
            active: AtomicBool::new(false),
            callback: callback,
            peer: Mutex::new(peer),
            lease: AtomicU64::new(lease),
            link: Mutex::new(Vec::new()),
            manager: manager, 
            queue: Queue::new(QUEUE_LIM),
            s_waker: SegQueue::new(),
            c_waker: AtomicCell::new(None),
            ch_sender: sender,
            ch_receiver: receiver
        });
        s.arc.set(&s);
        let a_self = s.arc.get();
        let c_recv = s.ch_receiver.clone();
        task::spawn(async move {
            consume_loop(a_self, c_recv).await;
        });
        return s
    }

    #[inline(always)]
    pub fn get_id(&self) -> usize {
        self.id
    }

    #[inline(always)]
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub fn activate(&self) {
        self.active.store(true, Ordering::Release)
    }

    #[inline(always)]
    pub fn deactivate(&self) {
        self.active.store(false, Ordering::Release)
    }

    #[inline(always)]
    pub fn get_lease(&self) -> ZInt {
        self.lease.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub fn set_lease(&self, lease: ZInt) {
        self.lease.store(lease, Ordering::Release)
    }

    #[inline(always)]
    pub async fn get_peer(&self) -> Option<PeerId> {
        self.peer.lock().await.clone()
    }

    #[inline(always)]
    pub async fn set_peer(&self, peer: Option<PeerId>) {
        *self.peer.lock().await = peer
    }

    // #[inline(always)]
    // pub async fn set_callback(&self, callback: Arc<dyn SessionCallback + Send + Sync>) {
    //     *self.callback.lock().await = callback;
    // }


    /*************************************/
    /*               LINK                */
    /*************************************/
    pub async fn add_link(&self, link: Arc<dyn Link + Send + Sync>) -> Result<(), ZError> {
        self.link.lock().await.push(link);
        Ok(())
    }

    pub async fn del_link(&self, src: &Locator, dst: &Locator, reason: Option<ZError>) -> Result<Arc<dyn Link + Send + Sync>, ZError> {
        let mut found = false;
        let mut index: usize = 0;

        let mut guard = self.link.lock().await;
        for i in 0..guard.len() {
            if guard[i].get_src() == *src &&
                    guard[i].get_dst() == *dst {
                found = true;
                index = i;
                break
            }
        }
    
        if !found {
            return Err(zerror!(ZErrorKind::Other{
                msg: format!("Trying to delete a link that does not exist!")
            }))
        }

        let link = guard.remove(index);
        if guard.len() == 0 {
            let err = match reason {
                Some(err) => err,
                None => zerror!(ZErrorKind::Other{
                    msg: format!("No links left in session {}", self.id)
                })
            };
            // Stop the task
            self.ch_sender.send(false).await;
            self.manager.del_session(self.id, Some(err)).await?;
        }

       return Ok(link)
    }


    /*************************************/
    /*           SCHEDULE OUT            */
    /*************************************/
    pub async fn schedule(&self, message: Arc<Message>) -> usize {
        struct FutureSchedule<'a> {
            session: &'a Session,
            message: Arc<Message>
        }
        
        impl<'a> Future for FutureSchedule<'a> {
            type Output = usize;
        
            fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
                self.session.s_waker.push(ctx.waker().clone());
                match self.session.queue.push(self.message.clone()) {
                    Ok(()) => return Poll::Ready(0),
                    Err(QueueError::IsEmpty) => panic!("This should not happen!!!"),
                    Err(QueueError::IsFull) => return Poll::Pending
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
            message: message
        }.await;
        
        // TO FIX
        // We need to return the message sequence number
        return 0
    }

    pub async fn consume(&self) -> Arc<Message> {
        struct FutureConsume<'a> {
            session: &'a Session
        }
        
        impl<'a> Future for FutureConsume<'a> {
            type Output = Arc<Message>;
        
            fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
                self.session.c_waker.store(Some(ctx.waker().clone()));
                match self.session.queue.pop() {
                    Ok(msg) => return Poll::Ready(msg),
                    Err(QueueError::IsEmpty) => return Poll::Pending,
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
    /*           SCHEDULE IN             */
    /*************************************/
    pub async fn schedule_in(&self, message: Arc<Message>) {
        struct FutureSchedule<'a> {
            session: &'a Session,
            message: Arc<Message>
        }
        
        impl<'a> Future for FutureSchedule<'a> {
            type Output = usize;
        
            fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
                self.session.s_waker.push(ctx.waker().clone());
                match self.session.queue.push(self.message.clone()) {
                    Ok(()) => return Poll::Ready(0),
                    Err(QueueError::IsEmpty) => panic!("This should not happen!!!"),
                    Err(QueueError::IsFull) => return Poll::Pending
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
            message: message
        }.await;
    }

    pub async fn consume_in(&self) -> Arc<Message> {
        struct FutureConsume<'a> {
            session: &'a Session
        }
        
        impl<'a> Future for FutureConsume<'a> {
            type Output = Arc<Message>;
        
            fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
                self.session.c_waker.store(Some(ctx.waker().clone()));
                match self.session.queue.pop() {
                    Ok(msg) => return Poll::Ready(msg),
                    Err(QueueError::IsEmpty) => return Poll::Pending,
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
    async fn receive_full_message(&self, src: &Locator, dst: &Locator, message: Message) -> Result<(), ZError> {
        match &message.body {
            // Body::Accept{opid, apid, lease} => {
            Body::Accept{..} => {
                self.manager.process_message(self.arc.get(), src, dst, message).await?;
            },
            // Body::AckNack{sn, mask} => {},
            Body::AckNack{..} => {},
            // Body::Close{pid, reason} => {},
            Body::Close{..} => {},
            // Body::Data{reliable, sn, key, info, payload} => {
            Body::Data{..} => {
                match self.is_active() {
                    true => self.callback.receive_message(self.arc.get(), message).await?,
                    false => return Err(zerror!(ZErrorKind::Other{
                        msg: format!("Data message received but session is not active")
                    }))
                }
            },
            // Body::Declare{sn, declarations} => {},
            Body::Declare{..} => {},
            // Body::Hello{whatami, locators} => {},
            Body::Hello{..} => {},
            // Body::KeepAlive{pid} => {},
            Body::KeepAlive{..} => {},
            // Body::Open{version, whatami, pid, lease, locators} => {
            Body::Open{..} => {
                self.manager.process_message(self.arc.get(), src, dst, message).await?;
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
        return Ok(())
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
        return Ok(())
    }

    /*************************************/
    /*         CLOSE THE SESSION         */
    /*************************************/
    pub async fn close(&self, _reason: Option<ZError>) -> Result<(), ZError> {
        // Stop the task
        self.ch_sender.send(false).await;
        for l in self.link.lock().await.iter() {
            l.close(None).await?;
        }
        Ok(())
    }
}