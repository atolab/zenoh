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
    zarcself,
    zerror
};
use crate::core::{
    ZError,
    ZErrorKind
};
use crate::io::RWBuf;
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
    addr: SocketAddr,
    buff_size: usize,
    session: Mutex<Arc<Session>>,
    next_session: Mutex<Option<Arc<Session>>>,
    manager: Arc<ManagerTcp>
}

impl LinkTcp {
    fn new(socket: TcpStream, addr: SocketAddr, session: Arc<Session>, manager: Arc<ManagerTcp>) -> Self {
        Self {
            locator: Locator::Tcp{ addr: addr },
            socket: socket,
            addr: addr,
            buff_size: 8_192,
            session: Mutex::new(session),
            next_session: Mutex::new(None),
            manager: manager
        }
    }
}

#[async_trait]
impl Link for LinkTcp {
    async fn close(&self) -> Result<(), ZError> {
        match self.socket.shutdown(Shutdown::Both) {
            Ok(_) => (),
            Err(e) => return Err(zerror!(ZErrorKind::Other{
                msg: format!("{}", e)
            }))
        };
        self.manager.del_link(&self.addr).await;
        Ok(())
    }
    
    #[inline(always)]
    async fn send(&self, message: Arc<Message>) -> Result<(), ZError> {
        // let mut buff = RWBuf::new(self.buff_size);
        // match buff.write_message(&message) {
        //     Ok(_) => {
        //         return (&self.socket).write_all(&buff.slice()).await
        //     },
        //     Err(_) => {}
        // };
        Ok(())
    }

    #[inline(always)]
    async fn set_session(&self, session: Arc<Session>) -> Result<(), ZError> {
        *self.next_session.lock().await = Some(session);
        Ok(())
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

async fn receive_loop(link: Arc<LinkTcp>) -> Result<(), ZError> {
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
            // match buff.read_message() {
            //     Ok(message) => {
            //         session.receive_message(&locator, message).await;
            //     },
            //     Err(_) => {}
            // }
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
    listener: RwLock<HashSet<SocketAddr>>,
    link: RwLock<HashMap<SocketAddr, Arc<LinkTcp>>>,
    link_limit: Option<usize>
}

zarcself!(ManagerTcp);
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
    async fn add_link(&self, link: Arc<LinkTcp>) -> Result<Option<Arc<LinkTcp>>, ZError> {
        if let Some(limit) = self.link_limit {
            if self.link.read().await.len() >= limit {
                return Err(zerror!(ZErrorKind::Other{
                    msg: format!("Reached maximum number of TCP connections: {}", limit)
                }))
            }
        }
        Ok(self.link.write().await.insert(link.addr, link))
    }

    #[inline]
    async fn del_link(&self, addr: &SocketAddr) -> Option<Arc<LinkTcp>> {
        self.link.write().await.remove(addr)
    }

    #[inline]
    async fn add_listener(&self, addr: &SocketAddr) -> Result<bool, ()> {
        Ok(self.listener.write().await.insert(addr.clone()))
    }

    #[inline]
    async fn del_listener(&self, addr: &SocketAddr) -> bool {
        self.listener.write().await.remove(addr)
    }
}

#[async_trait]
impl LinkManager for ManagerTcp  {  
    async fn new_link(&self, locator: &Locator) -> Result<Arc<dyn Link + Send + Sync>, ZError> {
        let addr = match locator {
            Locator::Tcp{ addr } => addr,
            _ => return Err(zerror!(ZErrorKind::InvalidLocator{
                reason: format!("Not a TCP locator: {}", locator)
            }))
        };
        let stream = match TcpStream::connect(addr).await {
            Ok(stream) => stream,
            Err(e) => return Err(zerror!(ZErrorKind::Other{
                msg: format!("{}", e)
            }))
        };
        let link = Arc::new(LinkTcp::new(stream, addr.clone(), self.session.clone(), self.get_arc_self()));
        match self.add_link(link.clone()).await {
            Ok(_) => {
                self.session.add_link(link.clone()).await;
            },
            Err(e) => return Err(e)
        }
        Ok(link)
    }

    async fn del_link(&self, locator: &Locator) -> Result<Arc<dyn Link + Send + Sync>, ZError> {
        let addr = match locator {
            Locator::Tcp{ addr } => addr,
            _ => return Err(zerror!(ZErrorKind::InvalidLocator{
                reason: format!("Not a TCP locator: {}", locator)
            }))
        };
        match self.del_link(addr).await {
            Some(link) => return Ok(link),
            None => return Err(zerror!(ZErrorKind::Other{
                msg: format!("No active TCP link with: {}", addr)
            }))
        }
    }

    async fn new_listener(&self, locator: &Locator) -> Result<(), ZError> {
        let a_self = self.get_arc_self();
        task::spawn(async move {
            accept_loop(a_self).await
        });
        Ok(())
    }

    async fn del_listener(&self, locator: &Locator) -> Result<(), ZError> {
        Ok(())
    }
  
}

async fn accept_loop(manager: Arc<ManagerTcp>) -> Result<(), ZError> {
    let socket = match TcpListener::bind(manager.addr).await {
        Ok(socket) => socket,
        Err(e) => return Err(zerror!(ZErrorKind::Other{
            msg: format!("{}", e)
        }))
    }; 
    println!("Listening on: tcp://{}", manager.addr);
    loop {
        let (stream, src) = match socket.accept().await {
            Ok((stream, src)) => (stream, src),
            Err(e) => return Err(zerror!(ZErrorKind::Other{
                msg: format!("{}", e)
            }))
        };
        let link = Arc::new(LinkTcp::new(stream, src, manager.session.clone(), manager.get_arc_self()));
        // Spawn the receiving loop for the task if 
        let l_clone = link.clone();
        match manager.add_link(link.clone()).await {
            Ok(_) => {
                task::spawn(async move {
                    match receive_loop(l_clone).await {
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