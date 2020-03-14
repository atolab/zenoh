use async_std::prelude::*;
use async_std::sync::{
    Arc,
    channel,
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
    ZResult
};
use crate::proto::Message;
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
    transport: Arc<Transport>,
    ch_send: Sender<Command>,
    ch_recv: Receiver<Command>,
    dropping_probability: f32,
}

impl LinkDummy {
    pub fn new(src_addr: String, dst_addr: String, rx_port: Receiver<Message>, tx_port: Sender<Message>, 
        transport: Arc<Transport>, dropping_probability: f32
    ) -> Self {
        let (sender, receiver) = channel::<Command>(1);
        Self {
            rx_port,
            tx_port,
            src_locator: Locator::Dummy(src_addr),
            dst_locator: Locator::Dummy(dst_addr),
            transport,
            ch_send: sender,
            ch_recv: receiver,
            dropping_probability
        }
    }

    pub async fn close(&self, _reason: Option<ZError>) -> ZResult<()> {
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
            Some(message) => {
                let drop = {
                    let mut rng = thread_rng();
                    rng.gen_range(0f32, 1f32) < link.dropping_probability
                };
                if !drop {
                    link.transport.receive_message(src, dst, message).await;
                } else {
                    // println!("DROPPED {:?}", message);
                }
                Some(Command::Ok)
            },
            None => Some(Command::Err(zerror!(ZErrorKind::Other {
                descr: format!("Link channel has failed!")
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

// impl Drop for LinkDummy {
//     fn drop(&mut self) {
//         println!("> Dropping Link ({:?}) => ({:?})", self.get_src(), self.get_dst());
//     }
// }

pub struct ManagerDummy {}

impl ManagerDummy {
    pub fn new(_manager: Arc<SessionManagerInner>) -> Self {
        unimplemented!("ManagerDummy is not supposed to be implemented!");
    }

    pub async fn new_link(&self, _dst: &Locator, _transport: Arc<Transport>) -> ZResult<Link> {
        unimplemented!("ManagerDummy is not supposed to be implemented!");
    }

    pub async fn del_link(&self, _src: &Locator, _dst: &Locator, _reason: Option<ZError>) -> ZResult<Link> {
        unimplemented!("ManagerDummy is not supposed to be implemented!");
    }

    pub async fn move_link(&self, _src: &Locator, _dst: &Locator, _transport: Arc<Transport>) -> ZResult<()> {
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