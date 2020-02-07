use async_std::prelude::*;
use async_std::sync::{
    Arc,
    Mutex,
    RwLock,
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

use crate::{
    ArcSelf,
    zarcself,
    zerror
};
use crate::core::ZInt;
use crate::proto::{
    Body,
    Locator,
    Message,
    MessageKind
};
use crate::session::{
    Queue,
    QueueError,
    SessionManager,
    Link,
    SessionCallback
};


// Constants
const QUEUE_LIM: usize = 16;


async fn consume_loop(session: Arc<Session>) {
    loop {
        let message = session.consume().await;
        // Send a copy of the message over each link
        for l in session.link.lock().await.iter() {
            match l.send(message.clone()).await {
                Ok(_) => (),
                Err(_) => ()
            }
        }
    }
}


pub struct Session {
    weak_self: RwLock<Weak<Self>>,
    id: usize,
    link: Mutex<Vec<Arc<dyn Link + Send + Sync>>>,
    manager: Arc<SessionManager>,
    callback: Arc<dyn SessionCallback + Send + Sync>,
    queue: Queue,
    s_waker: SegQueue<Waker>,
    c_waker: AtomicCell<Option<Waker>>
}

zarcself!(Session);
impl Session {
    pub fn new(id: usize, manager: Arc<SessionManager>, callback: Arc<dyn SessionCallback + Send + Sync>) -> Self {
        Self {
            weak_self: RwLock::new(Weak::new()),
            id: id,
            manager: manager, 
            callback: callback,
            link: Mutex::new(Vec::new()),
            queue: Queue::new(QUEUE_LIM),
            s_waker: SegQueue::new(),
            c_waker: AtomicCell::new(None)
        }
    }

    pub async fn initialize(&self) {
        let a_self = self.get_arc_self();
        task::spawn(async move {
            consume_loop(a_self).await;
        });
    }

    pub fn get_id(&self) -> usize {
        self.id
    }

    pub async fn add_link(&self, link: Arc<dyn Link + Send + Sync>) -> Option<Arc<dyn Link + Send + Sync>> {
        let res = self.del_link(&link.get_locator()).await;
        self.link.lock().await.push(link);
        return res
    }

    pub async fn del_link(&self, locator: &Locator) -> Option<Arc<dyn Link + Send + Sync>> {
        let mut found = false;
        let mut index: usize = 0;

        let mut guard = self.link.lock().await;
        for i in 0..guard.len() {
            if guard[i].get_locator() == *locator {
                found = true;
                index = i;
                break
            }
        }
    
        if found {
            return Some(guard.remove(index))
        } 
        return None
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
        0
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

    async fn receive_full_message(&self, locator: &Locator, message: Message) {
        match &message.body {
            Body::Accept{opid, apid, lease} => {
                self.manager.process_message(self.id, locator, message).await;
            },
            Body::AckNack{sn, mask} => {},
            Body::Close{pid, reason} => {},
            Body::Data{reliable, sn, key, info, payload} => {
                self.callback.receive_message(message);
            },
            Body::Declare{sn, declarations} => {},
            Body::Hello{whatami, locators} => {},
            Body::KeepAlive{pid} => {},
            Body::Open{version, whatami, pid, lease, locators} => {
                self.manager.process_message(self.id, locator, message).await;
            },
            Body::Ping{hash} => {},
            Body::Pong{hash} => {},
            Body::Pull{sn, key, pull_id, max_samples} => {},
            Body::Query{sn, key, predicate, qid, target, consolidation} => {},
            Body::Scout{what} => {},
            Body::Sync{sn, count} => {}
        }
    }

    async fn receive_first_fragement(&self, locator: &Locator, message: Message, number: Option<ZInt>) {
        unimplemented!("Defragementation not implemented yet!");
    }

    async fn receive_middle_fragement(&self, locator: &Locator, message: Message) {
        unimplemented!("Defragementation not implemented yet!");
    }

    async fn receive_last_fragement(&self, locator: &Locator, message: Message) {
        unimplemented!("Defragementation not implemented yet!");
    }

    pub async fn receive_message(&self, locator: &Locator, message: Message) {
        match message.kind {
            MessageKind::FullMessage =>
                self.receive_full_message(locator, message).await,
            MessageKind::FirstFragment{n} =>
                self.receive_first_fragement(locator, message, n).await,
            MessageKind::InbetweenFragment => 
                self.receive_middle_fragement(locator, message).await,
            MessageKind::LastFragment => 
                self.receive_last_fragement(locator, message).await
        }
    }

    pub async fn close(&self) {
        for l in self.link.lock().await.iter() {
            l.close();
        }
    }
}
