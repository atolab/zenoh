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
    MsgHandler,
    Session
};
use crate::session::queue::{
    HIGH_PRIO,
    LOW_PRIO,
    OrderedQueue,
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
        let message = transport.queue_tx.pop().await;
        match &message.link {
            // Send the message to the indicated link
            Some((src, dst)) => {
                let guard = transport.links.lock().await;
                if let Some(index) = transport.find_link(&guard, &src, &dst) {
                    let _ = guard[index].send(&message.message).await;
                }
            },
            None => {
                // Send the message only on the first link
                for l in transport.links.lock().await.iter() {
                    let _ = l.send(&message.message).await;
                    break
                }
            }
        }
        // Notify the barrier for the synchronous send 
        if let Some(barrier) = &message.barrier {
            barrier.wait().await;
        }
        Some(true)
    }
    
    loop {
        let stop = transport.signal_recv.recv();
        let consume = consume(&transport);
        match consume.race(stop).await {
            Some(true) => continue,
            Some(false) => break,
            None => break
        }
    }
}



struct TxMessage {
    message: Arc<Message>, 
    link: Option<(Locator, Locator)>,
    barrier: Option<Arc<Barrier>>
}


pub struct Transport {
    // The reference to the session
    pub(crate) session: RwLock<Option<Arc<Session>>>,
    // The callback for Data messages
    callback: RwLock<Option<Arc<dyn MsgHandler + Send + Sync>>>,
    // callback: Arc<dyn MsgHandler + Send + Sync>,
    // The timeout after which the session is closed if no messages are received
    lease: RwLock<ZInt>,
    // The list of transport links associated to this session
    links: Mutex<Vec<Link>>,
    // The queue of messages to be transmitted
    queue_tx: PriorityQueue<TxMessage>,
    queue_rx: Mutex<OrderedQueue<Message>>,
    queue_reliability: RwLock<Vec<Message>>,
    // Keeping track of the last unreliable sequence number
    last_sn_unreliable: Mutex<ZInt>,
    // The channel endpoints for terminating the consume_loop task
    signal_send: Sender<bool>,
    signal_recv: Receiver<bool>
}

impl Transport {
    pub(crate) fn new(lease: ZInt) -> Self {
        let (sender, receiver) = channel::<bool>(1);
        Self {
            session: RwLock::new(None), 
            callback: RwLock::new(None), 
            lease: RwLock::new(lease),
            links: Mutex::new(Vec::new()),
            queue_tx: PriorityQueue::new(QUEUE_SIZE, QUEUE_PRIO),
            queue_rx: Mutex::new(OrderedQueue::new(QUEUE_SIZE)),
            queue_reliability: RwLock::new(Vec::with_capacity(QUEUE_SIZE)),
            last_sn_unreliable: Mutex::new(0),
            signal_send: sender,
            signal_recv: receiver
        }
    }

    pub(crate) fn initialize(&self, session: Arc<Session>, callback: Arc<dyn MsgHandler + Send + Sync>) {
        *self.session.try_write().unwrap() = Some(session);
        *self.callback.try_write().unwrap() = Some(callback);
    }

    pub(crate) async fn get_lease(&self) -> ZInt {
        *self.lease.read().await
    }

    pub(crate) async fn set_lease(&self, lease: ZInt) {
        *self.lease.write().await = lease
    }

    pub(crate) async fn get_links(&self) -> Vec<Link> {
        let mut vec = Vec::new();
        for l in self.links.lock().await.iter() {
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
        self.signal_send.send(false).await;
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
        let mut guard = self.links.lock().await;
        match self.find_link(&guard, &link.get_src(), &link.get_dst()) {
            Some(_) => Err(zerror!(ZErrorKind::Other{
                descr: format!("Trying to delete a link that does not exist!")
            })),
            None => Ok(guard.push(link))
        }
    }

    pub(crate) async fn del_link(&self, src: &Locator, dst: &Locator, _reason: Option<ZError>) -> ZResult<Link> {    
        // println!("Deleting link {} => {} from {:?}", src, dst, self.get_peer());
        let mut guard = self.links.lock().await;
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
        let msg = TxMessage {
            message,
            link,
            barrier: None
        };
        let priority = match priority {
            Some(prio) => prio,
            None => LOW_PRIO 
        };
        self.queue_tx.push(msg, priority).await
    }

    // Send the message in a synchronous way. 
    // 1) Schedule the message 
    // 2) Be notified when the message is actually sent
    pub(crate) async fn send(&self, message: Arc<Message>, 
        priority: Option<usize>, link: Option<(Locator, Locator)>
    ) {
        let barrier = Arc::new(Barrier::new(2));
        let msg = TxMessage {
            message,
            link,
            barrier: Some(barrier.clone())
        };
        let priority = match priority {
            Some(prio) => prio,
            None => LOW_PRIO 
        };
        self.queue_tx.push(msg, priority).await;
        barrier.wait().await;
    }

    /*************************************/
    /*   MESSAGE RECEIVE FROM THE LINK   */
    /*************************************/
    async fn process_reliable_message(&self, message: Message, _sn: ZInt) -> ZResult<()> {
        zrwopt!(self.callback).handle_message(message).await?;
        Ok(())
    }

    async fn process_unreliable_message(&self, message: Message, _sn: ZInt) -> ZResult<()> {
        zrwopt!(self.callback).handle_message(message).await?;
        Ok(())
    }

    async fn receive_full_message(&self, src: &Locator, dst: &Locator, message: Message) -> ZResult<()> {
        match &message.body {
            Body::Accept{whatami, opid, apid, lease} => {
                zrwopt!(self.session).process_accept(src, dst, whatami, opid, apid, lease).await?;
            },
            // Body::AckNack{sn, mask} => {},
            Body::AckNack{..} => {},
            Body::Close{pid, reason} => {
                zrwopt!(self.session).process_close(src, dst, pid, reason).await?;
            },
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
            // Body::Scout{what} => {},
            Body::Scout{..} => {},
            // Body::Sync{sn, count} => {}
            Body::Sync{..} => {},
            Body::Data{reliable, sn, key: _, info: _, payload: _} => {
                let c_sn = *sn;
                match reliable {
                    true => self.process_reliable_message(message, c_sn).await?,
                    false => self.process_unreliable_message(message, c_sn).await?,
                }
            },
            Body::Declare{sn, declarations: _} |
            Body::Pull{sn, key: _, pull_id: _, max_samples: _} |
            Body::Query{sn, key: _, predicate: _, qid: _, target: _, consolidation: _} => {
                let c_sn = *sn;
                self.process_reliable_message(message, c_sn).await?;
            },
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
        zrwopt!(self.callback).close().await;
        // Stop the task
        self.stop().await;
        // Remove and close all the links
        for l in self.links.lock().await.drain(..) {
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
            for l in self.links.lock().await.iter() {
                s.push_str(&format!("\n\t[({:?}) => ({:?})]", l.get_src(), l.get_dst()));
            }
            s
        });
        write!(f, "Links:{}", links)
    }
}
