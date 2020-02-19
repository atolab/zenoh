use async_std::prelude::*;
use async_std::sync::{
    Arc,
    channel,
    Mutex,
    Receiver,
    RwLock,
    Sender,
    Weak
};
use async_std::task;
use async_std::task::{
    Context, 
    Poll,
    Waker
};
use crossbeam::atomic::AtomicCell;
use crossbeam::queue::SegQueue;
use std::pin::Pin;
use uuid::Uuid;

use crate::{
    ArcSelf,
    zarcself,
    zerror
};
use crate::core::{
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
    EmptyCallback,
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
        // Send a copy of the message over each link
        for l in session.link.lock().await.iter() {
            match l.send(message.clone()).await {
                Ok(_) => (),
                Err(_) => ()
            }
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
    weak_self: RwLock<Weak<Self>>,
    id: Uuid,
    link: Mutex<Vec<Arc<dyn Link + Send + Sync>>>,
    manager: Arc<SessionManager>,
    callback: Mutex<Arc<dyn SessionCallback + Send + Sync>>,
    queue: Queue,
    s_waker: SegQueue<Waker>,
    c_waker: AtomicCell<Option<Waker>>,
    ch_sender: Sender<bool>,
    ch_receiver: Receiver<bool>
}

zarcself!(Session);
impl Session {
    pub fn new(id: Uuid, manager: Arc<SessionManager>) -> Self {
        let (sender, receiver) = channel::<bool>(1);
        Self {
            weak_self: RwLock::new(Weak::new()),
            id: id,
            manager: manager, 
            callback: Mutex::new(Arc::new(EmptyCallback::new())),
            link: Mutex::new(Vec::new()),
            queue: Queue::new(QUEUE_LIM),
            s_waker: SegQueue::new(),
            c_waker: AtomicCell::new(None),
            ch_sender: sender,
            ch_receiver: receiver
        }
    }

    pub async fn initialize(&self, arc_self: &Arc<Self>) {
        self.set_arc_self(arc_self);
        let a_self = self.get_arc_self();
        let c_recv = self.ch_receiver.clone();
        task::spawn(async move {
            consume_loop(a_self, c_recv).await;
        });
    }

    pub fn get_id(&self) -> Uuid {
        self.id
    }

    pub async fn set_callback(&self, callback: Arc<dyn SessionCallback + Send + Sync>) {
        *self.callback.lock().await = callback;
    }

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
            let err = if let Some(err) = reason {
                err
            } else {
                zerror!(ZErrorKind::Other{
                    msg: format!("No links left in session {}", self.id)
                })
            };
            // Stop the task
            self.ch_sender.send(false).await;
            self.manager.del_session(self.id, Some(err)).await;
        }

       return Ok(link)
    }

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

    async fn receive_full_message(&self, src: &Locator, dst: &Locator, message: Message) {
        match &message.body {
            Body::Accept{opid, apid, lease} => {
                self.manager.process_message(self.get_arc_self(), src, dst, message).await;
            },
            Body::AckNack{sn, mask} => {},
            Body::Close{pid, reason} => {},
            Body::Data{reliable, sn, key, info, payload} => {
                self.callback.lock().await.receive_message(message);
            },
            Body::Declare{sn, declarations} => {},
            Body::Hello{whatami, locators} => {},
            Body::KeepAlive{pid} => {},
            Body::Open{version, whatami, pid, lease, locators} => {
                self.manager.process_message(self.get_arc_self(), src, dst, message).await;
            },
            Body::Ping{hash} => {},
            Body::Pong{hash} => {},
            Body::Pull{sn, key, pull_id, max_samples} => {},
            Body::Query{sn, key, predicate, qid, target, consolidation} => {},
            Body::Scout{what} => {},
            Body::Sync{sn, count} => {}
        }
    }

    async fn receive_first_fragement(&self, src: &Locator, dst: &Locator, message: Message, number: Option<ZInt>) {
        unimplemented!("Defragementation not implemented yet!");
    }

    async fn receive_middle_fragement(&self, src: &Locator, dst: &Locator, message: Message) {
        unimplemented!("Defragementation not implemented yet!");
    }

    async fn receive_last_fragement(&self, src: &Locator, dst: &Locator, message: Message) {
        unimplemented!("Defragementation not implemented yet!");
    }

    pub async fn receive_message(&self, src: &Locator, dst: &Locator, message: Message) {
        match message.kind {
            MessageKind::FullMessage =>
                self.receive_full_message(src, dst, message).await,
            MessageKind::FirstFragment{n} =>
                self.receive_first_fragement(src, dst, message, n).await,
            MessageKind::InbetweenFragment => 
                self.receive_middle_fragement(src, dst, message).await,
            MessageKind::LastFragment => 
                self.receive_last_fragement(src, dst, message).await
        }
    }

    pub async fn close(&self, reason: Option<ZError>) -> Result<(), ZError> {
        // Stop the task
        self.ch_sender.send(false).await;
        for l in self.link.lock().await.iter() {
            l.close(None).await;
        }
        Ok(())
    }
}

// impl Drop for Session {
//     fn drop(&mut self) {
//         println!("Dropping Session {:?}", self.id);
//     }
// }