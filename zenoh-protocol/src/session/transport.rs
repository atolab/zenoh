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
use std::fmt;

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
    SessionCallback,
    Session
};
use crate::session::queue::{
    PriorityQueue
};
use crate::link::{
    Link,
    Locator
};


// Constants
const QUEUE_SIZE: usize = 16;
const QUEUE_PRIO: usize = 2;


async fn consume_loop(transport: Arc<Transport>) {
    async fn consume(transport: &Arc<Transport>) -> Option<bool> {
        let message = transport.queue.pop().await;
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



struct QueueMessage {
    message: Arc<Message>, 
    link: Option<(Locator, Locator)>,
    barrier: Option<Arc<Barrier>>
}


pub struct Transport {
    // The reference to the session
    pub(crate) session: RwLock<Option<Arc<Session>>>,
    // The callback for Data messages
    callback: Arc<dyn SessionCallback + Send + Sync>,
    // The timeout after which the session is closed if no messages are received
    lease: RwLock<ZInt>,
    // The list of transport links associated to this session
    link: Mutex<Vec<Link>>,
    // The queue of messages to be transmitted
    queue: PriorityQueue<QueueMessage>,
    // The channel endpoints for terminating the consume_loop task
    ch_send: Sender<bool>,
    ch_recv: Receiver<bool>
}

impl Transport {
    pub(crate) fn new(lease: ZInt, callback: Arc<dyn SessionCallback + Send + Sync>) -> Self {
        let (sender, receiver) = channel::<bool>(1);
        Self {
            callback,
            lease: RwLock::new(lease),
            link: Mutex::new(Vec::new()),
            session: RwLock::new(None), 
            queue: PriorityQueue::new(QUEUE_SIZE, QUEUE_PRIO),
            ch_send: sender,
            ch_recv: receiver
        }
    }

    pub(crate) async fn get_lease(&self) -> ZInt {
        *self.lease.read().await
    }

    pub(crate) async fn set_lease(&self, lease: ZInt) {
        *self.lease.write().await = lease
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


    /*************************************/
    /*             SCHEDULE              */
    /*************************************/

    // Schedule the message to be sent asynchronsly
    pub(crate) async fn schedule(&self, message: Arc<Message>, 
        priority: Option<usize>, link: Option<(Locator, Locator)>
    ) {
        let msg = QueueMessage {
            message,
            link,
            barrier: None
        };
        self.queue.push(msg, priority).await
    }

    // Send the message in a synchronous way. 
    // 1) Schedule the message 
    // 2) Be notified when the message is actually sent
    pub(crate) async fn send(&self, message: Arc<Message>, 
        priority: Option<usize>, link: Option<(Locator, Locator)>
    ) {
        let barrier = Arc::new(Barrier::new(2));
        let msg = QueueMessage {
            message,
            link,
            barrier: Some(barrier.clone())
        };
        self.queue.push(msg, priority).await;
        barrier.wait().await;
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
