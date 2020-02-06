use async_std::net::{
    SocketAddr,
    TcpListener,
    TcpStream
};
use async_std::prelude::*;
use async_std::sync::{
    Arc,
    Mutex,
    RwLock,
    Weak
};
use async_std::task;
use async_trait::async_trait;
use std::collections::{
    HashMap,
    HashSet
};
use std::net::Shutdown;

use crate::{
    ArcSelf,
    impl_arc_self
};
use crate::io::rwbuf::RWBuf;
use crate::proto::{
    Locator,
    Message
};
use crate::session::{
    Session,
    Link,
    LinkManager
};

/*************************************/
/*              LINK                 */
/*************************************/
pub struct LinkTcp {
    locator: Locator,
    socket: TcpStream,
    remote: SocketAddr,
    buff_size: usize,
    session: Mutex<Arc<Session>>,
    next_session: Mutex<Option<Arc<Session>>>,
    manager: Arc<ManagerTcp>
}

impl LinkTcp {
    fn new(socket: TcpStream, remote: SocketAddr, session: Arc<Session>, manager: Arc<ManagerTcp>) -> Self {
        Self {
            locator: Locator::Tcp(remote),
            socket: socket,
            remote: remote,
            buff_size: 8_192,
            session: Mutex::new(session),
            next_session: Mutex::new(None),
            manager: manager
        }
    }
}

#[async_trait]
impl Link for LinkTcp {
    async fn close(&self) -> async_std::io::Result<()> {
        self.socket.shutdown(Shutdown::Both)?;
        self.manager.del_link(&self.get_locator()).await;
        Ok(())
    }
    
    #[inline(always)]
    async fn send(&self, message: Arc<Message>) -> async_std::io::Result<()> {
        let mut buff = RWBuf::new(self.buff_size);
        match buff.write_message(&message) {
            Ok(_) => {
                return (&self.socket).write_all(&buff.slice()).await
            },
            Err(_) => {}
        };
        Ok(())
    }

    #[inline(always)]
    async fn set_session(&self, session: Arc<Session>) {
        *self.next_session.lock().await = Some(session);
    }

    #[inline(always)]
    fn get_locator(&self) -> Locator {
        self.locator.clone()
    }

    #[inline]
    fn get_mtu(&self) -> usize {
        65_536
    }

    #[inline]
    fn is_ordered(&self) -> bool {
        true
    }

    #[inline]
    fn is_reliable(&self) -> bool {
        true
    }
}

async fn receive_loop(link: Arc<LinkTcp>) -> async_std::io::Result<()> {
    let locator = link.get_locator();
    let mut buff = RWBuf::new(link.buff_size);
    loop {
        match (&link.socket).read(&mut buff.writable_slice()).await {
            Ok(n) => { 
                buff.set_write_pos(buff.write_pos() + n).unwrap();
            },
            Err(_) => {
                link.close().await?;
                break
            }
        };
        // Check if we need to change the session to send the message to
        let mut session = link.session.lock().await;
        loop {
            if let Some(next) = link.next_session.lock().await.take() {
                println!("NEXT: {}", next.id);
                *session = next;
            }
            match buff.read_message() {
                Ok(message) => {
                    session.receive_message(&locator, message).await;
                },
                Err(_) => {}
            }
        }
    }
    Ok(())
}

/*************************************/
/*          LISTENER                 */
/*************************************/
pub struct ManagerTcp {
    weak_self: RwLock<Weak<Self>>,
    addr: SocketAddr,
    session: Arc<Session>,
    listener: RwLock<HashSet<Locator>>,
    link: RwLock<HashMap<Locator, Arc<LinkTcp>>>,
    link_limit: Option<usize>
}

impl_arc_self!(ManagerTcp);
impl ManagerTcp {
    pub fn new(addr: SocketAddr, session: Arc<Session>, limit: Option<usize>) -> Self {  
        Self {
            weak_self: RwLock::new(Weak::new()),
            addr: addr,
            session: session,
            listener: RwLock::new(HashSet::new()),
            link: RwLock::new(HashMap::new()),
            link_limit: limit
        }
    }

    #[inline]
    async fn add_link(&self, link: Arc<LinkTcp>) -> Result<Option<Arc<LinkTcp>>, ()> {
        if let Some(limit) = self.link_limit {
            if self.link.read().await.len() >= limit {
                return Err(())
            }
        }
        Ok(self.link.write().await.insert(link.get_locator(), link))
    }

    #[inline]
    async fn del_link(&self, locator: &Locator) -> Option<Arc<LinkTcp>> {
        self.link.write().await.remove(locator)
    }

    #[inline]
    async fn add_listener(&self, locator: &Locator) -> Result<bool, ()> {
        Ok(self.listener.write().await.insert(locator.clone()))
    }

    #[inline]
    async fn del_listener(&self, locator: &Locator) -> bool {
        self.listener.write().await.remove(locator)
    }
}

#[async_trait]
impl LinkManager for ManagerTcp  {  
    async fn new_link(&self, locator: &Locator) -> Result<Arc<LinkTcp>, ()> {
        let addr = match locator {
            Locator::Tcp(addr) => addr,
            _ => return Err(())
        };
        let stream = match TcpStream::connect(addr).await {
            Ok(stream) => stream,
            Err(_) => return Err(())
        };
        let link = Arc::new(LinkTcp::new(stream, addr.clone(), self.session.clone(), self.get_arc_self()));
        match self.add_link(link.clone()).await {
            Ok(_) => {
                self.session.add_link(link.clone()).await;
            },
            Err(_) => return Err(())
        }
        Ok(link)
    }

    async fn new_listener(&self, locator: &Locator) -> async_std::io::Result<()> {
        let a_self = self.get_arc_self();
        task::spawn(async move {
            accept_loop(a_self).await
        });
        Ok(())
    }
  
}

async fn accept_loop(manager: Arc<ManagerTcp>) -> async_std::io::Result<()> {
    let socket = TcpListener::bind(manager.addr).await?; 
    println!("Listening on: tcp://{}", socket.local_addr()?);
    loop {
        let (stream, src) = socket.accept().await?;
        let link = Arc::new(LinkTcp::new(stream, src, manager.session.clone(), manager.get_arc_self()));
        // Spawn the receiving loop for the task if 
        match manager.add_link(link.clone()).await {
            Ok(_) => {
                task::spawn(async move {
                    match receive_loop(link).await {
                        Ok(_) => (),
                        Err(_) => ()
                    }
                });
            },
            Err(_) => {
                link.clone();
            }
        }
    }
}