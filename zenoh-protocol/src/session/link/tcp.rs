use async_std::net::{
    SocketAddr,
    TcpListener,
    TcpStream
};
use async_std::prelude::*;
use async_std::sync::{
    Arc,
    channel,
    Mutex,
    Sender,
    RwLock,
    Receiver,
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
    Session,
    SessionManager,
    Link,
    LinkManager
};

/*************************************/
/*              LINK                 */
/*************************************/
pub struct LinkTcp {
    socket: TcpStream,
    local_addr: SocketAddr,
    peer_addr: SocketAddr,
    local_locator: Locator,
    peer_locator: Locator,
    buff_size: usize,
    session: Mutex<Arc<Session>>,
    next_session: Mutex<Option<Arc<Session>>>,
    manager: Arc<ManagerTcp>
}

impl LinkTcp {
    fn new(socket: TcpStream, session: Arc<Session>, manager: Arc<ManagerTcp>) -> Self {
        let local_addr = socket.local_addr().unwrap();
        let peer_addr = socket.peer_addr().unwrap();
        Self {
            socket: socket,
            local_addr: local_addr,
            peer_addr: peer_addr,
            local_locator: Locator::Tcp{ addr: local_addr },
            peer_locator: Locator::Tcp{ addr: peer_addr },
            buff_size: 8_192,
            session: Mutex::new(session),
            next_session: Mutex::new(None),
            manager: manager
        }
    }
}

#[async_trait]
impl Link for LinkTcp {
    async fn close(&self, reason: Option<ZError>) -> Result<(), ZError> {
        let _ = self.socket.shutdown(Shutdown::Both);
        match self.manager.del_link(&self.get_src(), &self.get_dst(), reason).await {
            Ok(_) => return Ok(()),
            Err(e) => return Err(zerror!(ZErrorKind::Other{
                msg: format!("{}", e)
            }))
        }
    }
    
    async fn send(&self, message: Arc<Message>) -> Result<(), ZError> {
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
    fn get_src(&self) -> Locator {
        self.local_locator.clone()
    }

    #[inline(always)]
    fn get_dst(&self) -> Locator {
        self.peer_locator.clone()
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
    let src = link.get_src();
    let dst = link.get_dst();
    let mut buff = RWBuf::new(link.buff_size);
    let err = loop {
        match (&link.socket).read(&mut buff.writable_slice()).await {
            Ok(n) => { 
                // Reading zero bytes means error
                if n == 0 {
                    break zerror!(ZErrorKind::IOError{
                        reason: format!("Failed to read from the TCP socket")
                    })
                }
                buff.set_write_pos(buff.write_pos() + n).unwrap();
            },
            Err(e) => break zerror!(ZErrorKind::IOError{
                reason: format!("{}", e)
            })
        }
        // Check if we need to change the session to send the message to
        let mut session = link.session.lock().await;
        loop {
            if let Some(next) = link.next_session.lock().await.take() {
                let err = zerror!(ZErrorKind::Other{
                    msg: format!("Moving the link to a new session")
                });
                let _ = session.del_link(&link.get_src(), &link.get_dst(), Some(err)).await;
                *session = next;
                let _ = session.add_link(link.clone()).await;
            }
            match buff.read_message() {
                Ok(message) => {
                    session.receive_message(&dst, &src, message).await;
                },
                Err(_) => break
            }
        }
    };

    // Close the link and clean the session
    let _ = link.close(None).await;
    let _ = link.session.lock().await.del_link(&link.get_src(), &link.get_dst(), Some(err)).await;
    return Ok(())
}

// impl Drop for LinkTcp {
//     fn drop(&mut self) {
//         println!("Dropping Link ({} => {})", self.local_addr, self.peer_addr);
//     }
// }

/*************************************/
/*          LISTENER                 */
/*************************************/
pub struct ManagerTcp {
    weak_self: RwLock<Weak<Self>>,
    manager: Arc<SessionManager>,
    listener: RwLock<HashMap<SocketAddr, (Arc<TcpListener>, Sender<bool>)>>,
    link: RwLock<HashMap<(SocketAddr, SocketAddr), Arc<LinkTcp>>>,
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

    async fn new_session(&self) -> Arc<Session> {
        self.manager.new_session().await
    }
}

#[async_trait]
impl LinkManager for ManagerTcp  {
    async fn new_link( &self, dst: &Locator, session: Arc<Session>) -> Result<Arc<dyn Link + Send + Sync>, ZError> {
        // Check if the locator is a TCP locator
        let dst = match dst {
            Locator::Tcp{ addr } => addr,
            _ => return Err(zerror!(ZErrorKind::InvalidLocator{
                reason: format!("Not a TCP locator: {}", dst)
            }))
        };
        
        // Create the TCP connection
        let stream = match TcpStream::connect(dst).await {
            Ok(stream) => stream,
            Err(e) => return Err(zerror!(ZErrorKind::Other{
                msg: format!("{}", e)
            }))
        };
        
        // Create a new link object
        let link = Arc::new(LinkTcp::new(stream, session.clone(), self.get_arc_self()));
        self.link.write().await.insert((link.local_addr, link.peer_addr), link.clone());
        match session.add_link(link.clone()).await {
            Ok(_) => {},
            Err(e) => return Err(e)
        }
        
        
        // Spawn the receive loop for the new link
        let a_link = link.clone();
        task::spawn(async move {
            let _ = receive_loop(a_link).await;
        });

        return Ok(link)
    }

    async fn del_link(&self, src: &Locator, dst: &Locator, reason: Option<ZError>) -> Result<Arc<dyn Link + Send + Sync>, ZError> {
        // Check if the src locator is a TCP locator
        let src = match src {
            Locator::Tcp{ addr } => addr,
            _ => return Err(zerror!(ZErrorKind::InvalidLocator{
                reason: format!("Not a TCP locator: {}", src)
            }))
        };

        // Check if the dst locator is a TCP locator
        let dst = match dst {
            Locator::Tcp{ addr } => addr,
            _ => return Err(zerror!(ZErrorKind::InvalidLocator{
                reason: format!("Not a TCP locator: {}", dst)
            }))
        };

        // Remove the link from the manager list
        match self.link.write().await.remove(&(*src, *dst)) {
            Some(link) => return Ok(link),
            None => return Err(zerror!(ZErrorKind::Other{
                msg: format!("No active TCP link ({} => {})", src, dst)
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
  
        // Bind the TCP socket
        let socket = match TcpListener::bind(addr).await {
            Ok(socket) => Arc::new(socket),
            Err(e) => return Err(zerror!(ZErrorKind::Other{
                msg: format!("{}", e)
            }))
        };

        // Create the channel necessary to break the accept loop
        let (sender, receiver) = channel::<bool>(1);
        // Update the list of active listeners on the manager
        self.listener.write().await.insert(addr.clone(), (socket.clone(), sender));

        // Spawn the accept loop for the listener
        let a_self = self.get_arc_self();
        let a_addr = addr.clone();
        task::spawn(async move {
            // Wait for the accept loop to terminate
            let res = match accept_loop(&a_self, &socket, limit, receiver).await {
                Ok(_) => Ok(()),
                Err(e) => Err(zerror!(ZErrorKind::Other{
                    msg: format!("{}", e)
                }))
            }; 

            // Delete the listener from the manager
            a_self.listener.write().await.remove(&a_addr);
 
            return res
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

        // Stop the listener
        match self.listener.read().await.get(&addr) {
            Some((_socket, sender)) => {
                sender.send(false).await;
                return Ok(())
            },
            None => return Err(zerror!(ZErrorKind::Other{
                msg: format!("No TCP listener on locator: {}", locator)
            }))
        }
    }
  
    async fn get_listeners(&self) -> Vec<Locator> {
        self.listener.read().await.keys().map(|x| Locator::Tcp{addr: *x}).collect()
    }
}

async fn accept_loop(manager: &Arc<ManagerTcp>, socket: &Arc<TcpListener>, 
    limit: Option<usize>, receiver: Receiver<bool>
) -> Result<(), ZError> {
    // The accept future
    async fn accept(manager: &Arc<ManagerTcp>, socket: &TcpListener, 
        limit: &Option<usize>, count: &Arc<AtomicUsize>) -> Option<bool> {

        // Wait for incoming connections
        let stream = match socket.accept().await {
            Ok((stream, _)) => stream,
            Err(_) => return Some(true)
        };

        // Check if the connection limit for this listener is not exceeded
        if let Some(limit) = limit {
            if count.load(Ordering::Acquire) < *limit {
                count.fetch_add(1, Ordering::Release);
            } else {
                let _ = stream.shutdown(Shutdown::Both);
                return Some(true)
            }
        }

        // Create a new temporary session Session 
        let session = manager.new_session().await;
        // Create the new link object
        let link = Arc::new(LinkTcp::new(stream, session.clone(), manager.get_arc_self()));

        // Store a reference to the link into the manger
        manager.link.write().await.insert((link.local_addr, link.peer_addr), link.clone());

        // Store a reference to the link into the session
        let _ = session.add_link(link.clone()).await;

        // Spawn the receive loop for the new link
        let a_link = link.clone();
        let a_count = count.clone();
        task::spawn(async move {
            let _ = receive_loop(a_link).await;
            a_count.fetch_sub(1, Ordering::Release);
        });

        Some(true)
    }

    let count = Arc::new(AtomicUsize::new(0));
    loop {
        let stop = receiver.recv();
        let accept = accept(&manager, &socket, &limit, &count);
        match accept.race(stop).await {
            Some(true) => continue,
            Some(false) => break,
            None => break
        }
    }

    Ok(())
}