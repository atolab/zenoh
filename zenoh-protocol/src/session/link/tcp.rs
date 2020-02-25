use async_std::net::{
    SocketAddr,
    TcpListener,
    TcpStream
};
use async_std::prelude::*;
use async_std::sync::{
    Arc,
    channel,
    Sender,
    RwLock,
    Receiver};
use async_std::task;
use async_trait::async_trait;
use std::collections::HashMap;
use std::net::Shutdown;

use crate::zerror;
use crate::core::{
    ZError,
    ZErrorKind,
    ZResult
};
use crate::io::RWBuf;
use crate::proto::{
    Locator,
    Message
};
use crate::session::{
    ArcSelf,
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
    arc: ArcSelf<Self>,
    socket: TcpStream,
    local_addr: SocketAddr,
    peer_addr: SocketAddr,
    local_locator: Locator,
    peer_locator: Locator,
    buff_size: usize,
    session: Arc<Session>,
    manager: Arc<ManagerTcp>,
    // The channel endpoints for terminating the receive_loop task
    ch_sender: Sender<ZResult<()>>,
    ch_receiver: Receiver<ZResult<()>>
}

impl LinkTcp {
    fn new(socket: TcpStream, session: Arc<Session>, manager: Arc<ManagerTcp>) -> Arc<Self> {
        let local_addr = socket.local_addr().unwrap();
        let peer_addr = socket.peer_addr().unwrap();
        let (sender, receiver) = channel::<ZResult<()>>(1);
        let l = Arc::new(Self {
            arc: ArcSelf::new(),
            socket: socket,
            local_addr: local_addr,
            peer_addr: peer_addr,
            local_locator: Locator::Tcp{ addr: local_addr },
            peer_locator: Locator::Tcp{ addr: peer_addr },
            buff_size: 8_192,
            session: session,
            manager: manager,
            ch_sender: sender,
            ch_receiver: receiver
        });
        l.arc.set(&l);
        // println!("> New Link ({:?}) => ({:?})", l.get_src(), l.get_dst());
        return l
    }
}

#[async_trait]
impl Link for LinkTcp {
    async fn close(&self, reason: Option<ZError>) -> ZResult<()> {
        let _ = self.socket.shutdown(Shutdown::Both);
        match self.manager.del_link(&self.get_src(), &self.get_dst(), reason).await {
            Ok(_) => return Ok(()),
            Err(e) => return Err(zerror!(ZErrorKind::Other{
                msg: format!("{}", e)
            }))
        }
    }
    
    async fn send(&self, message: Arc<Message>) -> ZResult<()> {
        let mut buff = RWBuf::new(self.buff_size);
        match buff.write_message(&message) {
            Ok(_) => match (&self.socket).write_all(&buff.readable_slice()).await {
                Ok(_) => return Ok(()),
                Err(e) => return Err(zerror!(ZErrorKind::Other{
                    msg: format!("{}", e)
                }))
            },
            Err(e) => return Err(e)
        }
    }

    async fn start(&self) -> ZResult<()> {
        let a_link = self.arc.get();
        task::spawn(async move {
            let _ = receive_loop(a_link).await;
        });
        return Ok(())
    }

    async fn stop(&self) -> ZResult<()> {
        let reason = Err(zerror!(ZErrorKind::Other{
            msg: format!("Received command to stop the link")
        }));
        self.ch_sender.send(reason).await;
        return Ok(())
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

async fn receive_loop(link: Arc<LinkTcp>) -> ZResult<()> {
    async fn read(link: &Arc<LinkTcp>, buff: &mut RWBuf, src: &Locator, dst: &Locator) -> Option<ZResult<()>> {
        match (&link.socket).read(&mut buff.writable_slice()).await {
            Ok(n) => { 
                // Reading zero bytes means error
                if n == 0 {
                    return Some(Err(zerror!(ZErrorKind::IOError{
                        reason: format!("Failed to read from the TCP socket")
                    })))
                }
                buff.set_write_pos(buff.write_pos() + n).unwrap();
            },
            Err(e) => return Some(Err(zerror!(ZErrorKind::IOError{
                reason: format!("{}", e)
            })))
        }
        loop {
            match buff.read_message() {
                Ok(message) => match link.session.receive_message(&dst, &src, message).await {
                    Ok(_) => continue,
                    Err(_) => continue
                },
                Err(_) => break
            }
        }
        Some(Ok(()))
    }

    let src = link.get_src();
    let dst = link.get_dst();
    let mut buff = RWBuf::new(link.buff_size);
    let err = loop {
        let stop = link.ch_receiver.recv();
        let read = read(&link, &mut buff, &src, &dst);
        match read.race(stop).await {
            Some(res) => match res {
                Ok(_) => continue,
                Err(e) => break e
            }
            None => break zerror!(ZErrorKind::Other{
                msg: format!("Stopped")
            })
        }
    };

    // Close the link and clean the session
    // link.close(Some(err)).await?;
    // link.session.lock().await.del_link(&link.get_src(), &link.get_dst(), Some(err)).await?;

    return Err(err)
}

impl Drop for LinkTcp {
    fn drop(&mut self) {
        // println!("> Dropping Link ({:?}) => ({:?})", self.get_src(), self.get_dst());
    }
}

/*************************************/
/*          LISTENER                 */
/*************************************/
pub struct ManagerTcp {
    arc: ArcSelf<Self>,
    manager: Arc<SessionManager>,
    listener: RwLock<HashMap<SocketAddr, (Arc<TcpListener>, Sender<bool>)>>,
    link: RwLock<HashMap<(SocketAddr, SocketAddr), Arc<LinkTcp>>>,
}

impl ManagerTcp {
    pub fn new(manager: Arc<SessionManager>) -> Arc<Self> {  
        let m = Arc::new(Self {
            arc: ArcSelf::new(),
            manager: manager,
            listener: RwLock::new(HashMap::new()),
            link: RwLock::new(HashMap::new()),
        });
        m.arc.set(&m);
        return m
    }

    async fn new_session(&self) -> Arc<Session> {
        self.manager.new_session(None, Arc::new(EmptyCallback::new())).await
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
        let link = LinkTcp::new(stream, session.clone(), self.arc.get());
        self.link.write().await.insert((link.local_addr, link.peer_addr), link.clone());
        session.add_link(link.clone()).await?;
        
        // Spawn the receive loop for the new link
        link.start().await?;

        return Ok(link)
    }

    async fn del_link(&self, src: &Locator, dst: &Locator, _reason: Option<ZError>) -> Result<Arc<dyn Link + Send + Sync>, ZError> {
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

    async fn move_link(&self, src: &Locator, dst: &Locator, session: Arc<Session>) -> Result<(), ZError> {
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

        // Remove the link from the session manager
        let old_link = match self.link.write().await.remove(&(*src, *dst)) {
            Some(link) => link,
            None => return Err(zerror!(ZErrorKind::Other{
                msg: format!("No active TCP link ({} => {})", src, dst)
            }))
        };

        // Remove the link from the session
        let reason = Some(zerror!(ZErrorKind::Other{
            msg: format!("Migrating the link to a new session")
        }));
        old_link.session.del_link(&old_link.get_src(), &old_link.get_dst(), reason).await?;

        // Stop the link
        old_link.stop();

        // Create a new link object
        let new_link = LinkTcp::new(old_link.socket.clone(), session.clone(), self.arc.get());
        self.link.write().await.insert((new_link.local_addr, new_link.peer_addr), new_link.clone());

        // Add the link to the new session
        session.add_link(new_link.clone()).await?;

        // Start the link
        new_link.start().await?;

        // println!("> Count Link ({:?}) => ({:?}) ({})", old_link.get_src(), old_link.get_dst(), Arc::strong_count(&old_link));
        return Ok(())
    }

    async fn new_listener(&self, locator: &Locator) -> Result<(), ZError> {
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
        let a_self = self.arc.get();
        let a_addr = addr.clone();
        task::spawn(async move {
            // Wait for the accept loop to terminate
            let res = match accept_loop(&a_self, &socket, receiver).await {
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

async fn accept_loop(manager: &Arc<ManagerTcp>, socket: &Arc<TcpListener>, receiver: Receiver<bool>
) -> Result<(), ZError> {
    // The accept future
    async fn accept(manager: &Arc<ManagerTcp>, socket: &TcpListener) -> Option<bool> {
        // Wait for incoming connections
        let stream = match socket.accept().await {
            Ok((stream, _)) => stream,
            Err(_) => return Some(true)
        };

        // Create a new temporary session Session 
        let session = manager.new_session().await;
        // Create the new link object
        let link = LinkTcp::new(stream, session.clone(), manager.arc.get());

        // Store a reference to the link into the manger
        manager.link.write().await.insert((link.local_addr, link.peer_addr), link.clone());

        // Store a reference to the link into the session
        let _ = session.add_link(link.clone()).await;

        // Spawn the receive loop for the new link
        let _ = link.start().await;

        Some(true)
    }

    loop {
        let stop = receiver.recv();
        let accept = accept(&manager, &socket);
        match accept.race(stop).await {
            Some(true) => continue,
            Some(false) => break,
            None => break
        }
    }

    return Ok(())
}