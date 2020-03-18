use async_std::prelude::*;
use async_std::sync::{
    Arc,
    channel,
    Mutex,
    RwLock,
    Sender,
    Receiver
};
use async_std::task;
use rand::{
    Rng,
    thread_rng
};

use crate::zerror;
use crate::core::{
    ZError,
    ZErrorKind,
    ZInt,
    ZResult
};
use crate::proto::{
    Body,
    Message
};
use crate::session::{
    SessionManagerInner,
    Transport
};
use crate::link::{
    Link,
    Locator
};


#[macro_export]
macro_rules! get_dummy_addr {
    ($locator:expr) => (match $locator {
        Locator::Dummy(addr) => addr,
        _ => return Err(zerror!(ZErrorKind::InvalidLocator {
            descr: format!("Not a Dummy locator: {}", $locator)
        }))
    });
}

// Command
enum Command {
    Err(ZError),
    Ok,
    Signal
}

/*************************************/
/*              LINK                 */
/*************************************/
pub struct LinkDummy {
    rx_port: Receiver<Message>,
    tx_port: Sender<Message>,
    src_locator: Locator,
    dst_locator: Locator,
    transport: Mutex<Arc<Transport>>,
    ch_send: Sender<Command>,
    ch_recv: Receiver<Command>,
    dropping_probability: RwLock<f32>,
    reordering_probability: RwLock<f32>,
}

impl LinkDummy {
    pub fn new(src_addr: String, dst_addr: String, rx_port: Receiver<Message>, 
        tx_port: Sender<Message>, transport: Arc<Transport>
    ) -> Self {
        let (sender, receiver) = channel::<Command>(1);
        Self {
            rx_port,
            tx_port,
            src_locator: Locator::Dummy(src_addr),
            dst_locator: Locator::Dummy(dst_addr),
            transport: Mutex::new(transport),
            ch_send: sender,
            ch_recv: receiver,
            dropping_probability: RwLock::new(0.0),
            reordering_probability: RwLock::new(0.0)
        }
    }

    pub async fn set_dropping_probability(&self, dropping_probability: f32) {
        *self.dropping_probability.write().await = dropping_probability;
    }

    pub async fn set_reordering_probability(&self, reordering_probability: f32) {
        *self.reordering_probability.write().await = reordering_probability;
    }

    pub async fn close(&self) -> ZResult<()> {
        self.ch_send.send(Command::Signal).await;
        Ok(())
    }
    
    pub async fn send(&self, message: &Message) -> ZResult<()> {
        let msg = message.clone();
        self.tx_port.send(msg).await;
        Ok(())
    }

    pub fn start(link: Arc<LinkDummy>) {
        task::spawn(receive_loop(link));
    }

    pub async fn stop(&self) -> ZResult<()> {
        self.ch_send.send(Command::Signal).await;
        Ok(())
    }

    pub fn get_src(&self) -> Locator {
        self.src_locator.clone()
    }

    pub fn get_dst(&self) -> Locator {
        self.dst_locator.clone()
    }

    pub fn get_mtu(&self) -> usize {
        65_536
    }

    pub fn is_ordered(&self) -> bool {
        true
    }

    pub fn is_reliable(&self) -> bool {
        true
    }
}

async fn receive_loop(link: Arc<LinkDummy>) {
    async fn read(link: &Arc<LinkDummy>, src: &Locator, dst: &Locator) -> Option<Command> {
        match link.rx_port.recv().await {
            Some(mut message) => {
                let dropping_probability = *link.dropping_probability.read().await;
                let drop = {
                    let mut rng = thread_rng();
                    rng.gen_range(0f32, 1f32) < dropping_probability
                };
                let reordering_probability = *link.reordering_probability.read().await;
                {
                    let mut rng = thread_rng();
                    if rng.gen_range(0f32, 1f32) < reordering_probability {
                        match message.body {
                            Body::Data{reliable: _, ref mut sn, key: _, info: _, payload: _} |
                            Body::Declare{ref mut sn, declarations: _} |
                            Body::Pull{ref mut sn, key: _, pull_id: _, max_samples: _} |
                            Body::Query{ref mut sn, key: _, predicate: _, qid: _, target: _, consolidation: _} => {
                                // Update the sequence number
                                *sn = rng.gen_range(ZInt::min_value(), ZInt::max_value());
                            },
                            _ => {}
                        }
                    }
                }
                if !drop {
                    link.transport.lock().await.receive_message(src, dst, message).await;
                } else {
                    // println!("DROPPED {:?}", message);
                }
                Some(Command::Ok)
            },
            None => Some(Command::Err(zerror!(ZErrorKind::Other {
                descr: "Link channel has failed!".to_string()
            })))
        }
    }

    let src = link.get_src();
    let dst = link.get_dst();
    loop {
        let stop = link.ch_recv.recv();
        let read = read(&link, &src, &dst);
        match read.race(stop).await {
            Some(command) => match command {
                Command::Ok => continue,
                Command::Err(_) => break,
                Command::Signal => break
            },
            None => break
        }
    }
}


pub struct ManagerDummy {}

impl ManagerDummy {
    pub fn new(_manager: Arc<SessionManagerInner>) -> Self {
        unimplemented!("ManagerDummy is not supposed to be implemented!");
    }

    pub async fn new_link(&self, _dst: &Locator, _transport: Arc<Transport>) -> ZResult<Link> {
        unimplemented!("ManagerDummy is not supposed to be implemented!");
    }

    pub async fn del_link(&self, _src: &Locator, _dst: &Locator) -> ZResult<Link> {
        unimplemented!("ManagerDummy is not supposed to be implemented!");
    }

    pub async fn get_link(&self, _src: &Locator, _dst: &Locator) -> ZResult<Link> {
        unimplemented!("ManagerDummy is not supposed to be implemented!");
    }

    pub async fn new_listener(&self, _locator: &Locator) -> ZResult<()> {
        unimplemented!("ManagerDummy is not supposed to be implemented!"); 
    }

    pub async fn del_listener(&self, _locator: &Locator) -> ZResult<()> {
        unimplemented!("ManagerDummy is not supposed to be implemented!"); 
    }
    pub async fn get_listeners(&self) -> Vec<Locator> {
        unimplemented!("ManagerDummy is not supposed to be implemented!");
    }
}