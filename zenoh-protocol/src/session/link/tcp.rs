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
use std::collections::HashMap;
use std::net::Shutdown;
use std::sync::atomic::{
    AtomicUsize, 
    Ordering
};

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
    EmptyCallback,
    Session,
    SessionManager,
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
        match self.manager.del_link(&self.locator).await {
            Ok(_) => return Ok(()),
            Err(e) => return Err(zerror!(ZErrorKind::Other{
                msg: format!("{}", e)
            }))
        }
    }
    
    async fn send(&self, message: Arc<Message>) -> Result<(), ZError> {
        // println!("SEND {} {:?}", self.addr, message);
        let mut buff = RWBuf::new(self.buff_size);
        match buff.write_message(&message) {
            Ok(_) => match (&self.socket).write_all(&buff.readable_slice()).await {
                Ok(_) => Ok(()),
                Err(e) => return Err(zerror!(ZErrorKind::Other{
                    msg: format!("{}", e)
                }))
            },
            Err(e) => return Err(e)
        }
    }

    async fn set_session(&self, session: Arc<Session>) -> Result<(), ZError> {
        *self.next_session.lock().await = Some(session);
        Ok(())
    }

    #[inline(always)]
    fn get_locator(&self) -> Locator {
        self.locator.clone()
    }

    #[inline(always)]
    fn get_mtu(&self) -> usize {
        65_536
    }

    #[inline(always)]
    fn is_ordered(&self) -> bool {
        true
    }

    #[inline(always)]
    fn is_reliable(&self) -> bool {
        true
    }
}

async fn receive_loop(link: Arc<LinkTcp>) -> Result<(), ZError> {
    // println!("REC LOOP {}", link.addr);
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
                session.del_link(&link.get_locator()).await;
                *session = next;
                session.add_link(link.clone()).await;
            }
            match buff.read_message() {
                Ok(message) => {
                    session.receive_message(&locator, message).await;
                },
                Err(_) => break
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
    manager: Arc<SessionManager>,
    listener: RwLock<HashMap<SocketAddr, Arc<TcpListener>>>,
    link: RwLock<HashMap<SocketAddr, Arc<LinkTcp>>>,
}

zarcself!(ManagerTcp);
impl ManagerTcp {
    pub fn new(manager: Arc<SessionManager>) -> Self {  
        Self {
            weak_self: RwLock::new(Weak::new()),
            manager: manager,
            listener: RwLock::new(HashMap::new()),
            link: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl LinkManager for ManagerTcp  {
    async fn new_link(&self, locator: &Locator, session: Arc<Session>) -> Result<Arc<dyn Link + Send + Sync>, ZError> {
        // Check if the locator is a TCP locator
        let addr = match locator {
            Locator::Tcp{ addr } => addr,
            _ => return Err(zerror!(ZErrorKind::InvalidLocator{
                reason: format!("Not a TCP locator: {}", locator)
            }))
        };
        
        // Create the TCP connection
        let stream = match TcpStream::connect(addr).await {
            Ok(stream) => stream,
            Err(e) => return Err(zerror!(ZErrorKind::Other{
                msg: format!("{}", e)
            }))
        };
        
        // Create a new link object
        let link = Arc::new(LinkTcp::new(stream, addr.clone(), session.clone(), self.get_arc_self()));
        self.link.write().await.insert(link.addr, link.clone());
        session.add_link(link.clone()).await;
        
        // Spawn the receive loop for the new link
        let a_link = link.clone();
        task::spawn(async move {
            match receive_loop(a_link).await {
                Ok(_) => (),
                Err(e) => println!("{:?}", e)
            }
            
        });
        // WORKAROUND !!! This task does nothing !!!
        // There is a scheduling problem: if I remove this task,
        // the previous task with the receive loop task is not scheduled
        task::spawn(async move {});

        Ok(link)
    }

    async fn del_link(&self, locator: &Locator) -> Result<Arc<dyn Link + Send + Sync>, ZError> {
        // Check if the locator is a TCP locator
        let addr = match locator {
            Locator::Tcp{ addr } => addr,
            _ => return Err(zerror!(ZErrorKind::InvalidLocator{
                reason: format!("Not a TCP locator: {}", locator)
            }))
        };
        match self.link.write().await.remove(addr) {
            Some(link) => return Ok(link),
            None => return Err(zerror!(ZErrorKind::Other{
                msg: format!("No active TCP link with: {}", addr)
            }))
        }
    }

    async fn new_listener(&self, locator: &Locator, limit: Option<usize>) -> Result<(), ZError> {
        // Check if the locator is a TCP locator
        let addr = match locator {
            Locator::Tcp{ addr } => addr,
            _ => return Err(zerror!(ZErrorKind::InvalidLocator{
                reason: format!("Not a TCP locator: {}", locator)
            }))
        };
        let a_self = self.get_arc_self();
        let a_addr = addr.clone();
        task::spawn(async move {
            match accept_loop(a_self, a_addr, limit).await {
                Ok(_) => (),
                Err(e) => println!("{:?}", e)
            }
        });
        Ok(())
    }

    async fn del_listener(&self, locator: &Locator) -> Result<(), ZError> {
        // Check if the locator is a TCP locator
        let addr = match locator {
            Locator::Tcp{ addr } => addr,
            _ => return Err(zerror!(ZErrorKind::InvalidLocator{
                reason: format!("Not a TCP locator: {}", locator)
            }))
        };
        match self.listener.read().await.get(&addr) {
            Some(_socket) => {
                unimplemented!("Stopping an existing TCP listener is not yet implemented");
            },
            None => return Err(zerror!(ZErrorKind::Other{
                msg: format!("")
            }))
        }
    }
  
    async fn get_listeners(&self) -> Vec<Locator> {
        self.listener.read().await.keys().map(|x| Locator::Tcp{addr: *x}).collect()
    }
}

async fn accept_loop(manager: Arc<ManagerTcp>, addr: SocketAddr, limit: Option<usize>) -> Result<(), ZError> {
    // Bind the TCP socket
    let socket = match TcpListener::bind(addr).await {
        Ok(socket) => Arc::new(socket),
        Err(e) => return Err(zerror!(ZErrorKind::Other{
            msg: format!("{}", e)
        }))
    };

    // Update the list of active listeners on the manager
    manager.listener.write().await.insert(addr.clone(), socket.clone());

    println!("Listening on: tcp://{}", addr);
    let count = Arc::new(AtomicUsize::new(0));
    loop {
        // Wait for incoming connections
        let (stream, src) = match socket.accept().await {
            Ok((stream, src)) => (stream, src),
            Err(_) => break
        };

        // Check if the connection limit for this listener is not exceeded
        if let Some(limit) = limit {
            if count.load(Ordering::Acquire) < limit {
                count.fetch_add(1, Ordering::Release);
            } else {
                match stream.shutdown(Shutdown::Both) {
                    Ok(_) => continue,
                    Err(_) => continue
                }
            }
        }

        // Create a temporary Session 
        let callback = Arc::new(EmptyCallback::new());
        let session = Arc::new(Session::new(0, manager.manager.clone(), callback));
        session.initialize(&session).await;
        // Create the new link object
        let link = Arc::new(LinkTcp::new(stream, src, session.clone(), manager.get_arc_self()));

        // Store a reference to the link into the manger
        manager.link.write().await.insert(src.clone(), link.clone());

        // Store a reference to the link into the session
        session.add_link(link.clone()).await;

        // Spawn the receive loop for the new link
        let a_link = link.clone();
        let a_count = count.clone();
        task::spawn(async move {
            match receive_loop(a_link).await {
                Ok(_) => (),
                Err(e) => println!("{:?}", e)
            }
            a_count.fetch_sub(1, Ordering::Release);
        });
    }

    // Delete the listener from the manager
    manager.listener.write().await.remove(&addr);

    Ok(())
}