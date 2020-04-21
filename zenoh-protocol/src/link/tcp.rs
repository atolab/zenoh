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
    Receiver};
use async_std::task;
use std::collections::HashMap;
use std::net::Shutdown;

#[cfg(write_vectored)]
use std::io::IoSlice;

use crate::{
    zerror,
    to_zerror,
};
use crate::core::{
    ZError,
    ZErrorKind,
    ZResult
};
use crate::io::{
    ArcSlice,
    RBuf
};
use crate::session::{
    SessionManagerInner,
    Transport
};
use crate::link::{
    Link,
    Locator
};
use zenoh_util::zasynclock;


// Size of buffer used to read from socket
const READ_BUFFER_SIZE: usize = 128 * 1_024;
// Default MTU
const DEFAULT_MTU: usize = 65_535;


#[macro_export]
macro_rules! get_tcp_addr {
    ($locator:expr) => (match $locator {
        Locator::Tcp(addr) => addr,
        // @TODO: uncomment the following when more links are added
        // _ => return Err(zerror!(ZErrorKind::InvalidLocator {
        //     descr: format!("Not a TCP locator: {}", $locator)
        // }))
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
pub struct LinkTcp {
    // The underlying socket as returned from the std library
    socket: TcpStream,
    // The source socket address of this link (address used on the local host)
    src_addr: SocketAddr,
    // The destination socket address of this link (address used on the remote host)
    dst_addr: SocketAddr,
    // The source Zenoh locator of this link (locator used on the local host)
    src_locator: Locator,
    // The destination Zenoh locator of this link (locator used on the local host)
    dst_locator: Locator,
    // The buffer size to use in a read operation
    buff_size: usize,
    // The reference to the associated transport
    transport: Mutex<Arc<Transport>>,
    // The reference to the associated link manager
    manager: Arc<ManagerTcpInner>,
    // Channel for stopping the read task
    ch_send: Sender<Command>,
    ch_recv: Receiver<Command>
}

impl LinkTcp {
    fn new(socket: TcpStream, transport: Arc<Transport>, manager: Arc<ManagerTcpInner>) -> LinkTcp {
        // Retrieve the source and destination socket addresses
        let src_addr = socket.local_addr().unwrap();
        let dst_addr = socket.peer_addr().unwrap();
        // The channel for stopping the read task
        let (sender, receiver) = channel::<Command>(1);
        // Build the LinkTcp
        LinkTcp {
            socket,
            src_addr,
            dst_addr,
            src_locator: Locator::Tcp(src_addr),
            dst_locator: Locator::Tcp(dst_addr),
            buff_size: READ_BUFFER_SIZE,
            transport: Mutex::new(transport),
            manager,
            ch_send: sender,
            ch_recv: receiver
        }
    }

    pub async fn close(&self) -> ZResult<()> {
        let _ = self.socket.shutdown(Shutdown::Both);
        self.manager.del_link(&self.get_src(), &self.get_dst()).await?;
        Ok(())
    }
    
    pub async fn send(&self, buffer: RBuf) -> ZResult<()> {
        #[cfg(write_vectored)]
        {
            let mut ioslices = &mut buffer.as_ioslices()[..];
            while ! ioslices.is_empty() {
                match (&self.socket).write_vectored(ioslices).await {
                    Ok(size) => {
                        ioslices = IoSlice::advance(ioslices, size);
                    },
                    err => {err.map_err(to_zerror!(IOError, "on write_vectored".to_string()))?;}
                }
            }
        }
        
        #[cfg(not(write_vectored))]
        {
            let mut sendbuff = Vec::with_capacity(buffer.readable());
            for s in buffer.get_slices() {
                std::io::Write::write_all(&mut sendbuff, s.as_slice())
                    .map_err(to_zerror!(IOError, "on buff.write_all".to_string()))?;
            }
            (&self.socket).write_all(&sendbuff).await
                .map_err(to_zerror!(IOError, "on socket.write_all".to_string()))?;
        }

        Ok(())
    }

    pub fn start(link: Arc<LinkTcp>) {
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
        DEFAULT_MTU
    }

    pub fn is_ordered(&self) -> bool {
        true
    }

    pub fn is_reliable(&self) -> bool {
        true
    }
}

async fn receive_loop(link: Arc<LinkTcp>) {
    async fn read(link: &Arc<LinkTcp>, buff: &mut RBuf, link_obj: &Link) -> Option<Command> {
        let mut rbuf = vec![0u8; link.buff_size];
        match (&link.socket).read(&mut rbuf).await {
            Ok(n) => { 
                // Reading zero bytes means error
                if n == 0 {
                    return Some(Command::Err(zerror!(ZErrorKind::IOError {
                        descr: "Failed to read from the TCP socket".to_string()
                    })))
                }
                buff.add_slice(ArcSlice::new(Arc::new(rbuf), 0, n));
            },
            Err(e) => return Some(Command::Err(zerror!(ZErrorKind::IOError {
                descr: format!("{}", e)
            })))
        }
        // Read all the messages
        loop {
            let pos = buff.get_pos();
            match buff.read_message() {
                Ok(message) => {
                    let mut guard = zasynclock!(link.transport);
                    if let Some(transport) = guard.receive_message(link_obj, message).await {
                        *guard = transport;
                    }
                    buff.clean_read_slices();
                    continue
                },
                Err(_) => {
                    if buff.set_pos(pos).is_err() {
                        panic!("Unrecoverable error in TCP read loop!")
                    }
                    break
                }
            }
        }

        Some(Command::Ok)
    }

    let mut signal = false;
    let link_obj = Link::Tcp(link.clone());
    let mut buff = RBuf::new();
    let _err = loop {
        let stop = link.ch_recv.recv();
        let read = read(&link, &mut buff, &link_obj);
        match read.race(stop).await {
            Some(command) => match command {
                Command::Ok => continue,
                Command::Err(e) => break e,
                Command::Signal => {
                    signal = true;
                    break zerror!(ZErrorKind::Other {
                        descr: "Stopped by a signal!".to_string()
                    })
                }
            },
            None => {
                signal = true;
                break zerror!(ZErrorKind::Other {
                    descr: "Error in the signal channel!".to_string()
                })
            }
        }
    };

    // Remove the link in case of IO error
    if !signal {
        let _ = link.manager.del_link(&link.get_src(), &link.get_dst()).await;
        let _ = link.transport.lock().await.del_link(&link_obj).await;
    }
}

// impl Drop for LinkTcp {
//     fn drop(&mut self) {
//         println!("> Dropping Link ({:?}) => ({:?})", self.get_src(), self.get_dst());
//     }
// }

/*************************************/
/*          LISTENER                 */
/*************************************/
pub struct ManagerTcp(Arc<ManagerTcpInner>);

impl ManagerTcp {
    pub fn new(manager: Arc<SessionManagerInner>) -> Self {  
        Self(Arc::new(ManagerTcpInner::new(manager)))
    }

    pub async fn new_link(&self, dst: &Locator, transport: Arc<Transport>) -> ZResult<Link> {
        let link = self.0.new_link(&self.0, dst, transport).await?;
        Ok(Link::Tcp(link))
    }

    pub async fn del_link(&self, src: &Locator, dst: &Locator) -> ZResult<Link> {
        let link = self.0.del_link(src, dst).await?;
        Ok(Link::Tcp(link))
    }

    pub async fn get_link(&self, src: &Locator, dst: &Locator) -> ZResult<Link> {
        let link = self.0.get_link(src, dst).await?;
        Ok(Link::Tcp(link))
    }

    pub  async fn new_listener(&self, locator: &Locator) -> ZResult<()> {
        self.0.new_listener(&self.0, locator).await
    }

    pub async fn del_listener(&self, locator: &Locator) -> ZResult<()> {
        self.0.del_listener(&self.0, locator).await
    }
  
    pub async fn get_listeners(&self) -> Vec<Locator> {
        self.0.get_listeners().await
    }
}

#[allow(clippy::type_complexity)]
struct ManagerTcpInner {
    inner: Arc<SessionManagerInner>,
    listener: RwLock<HashMap<SocketAddr, (Arc<TcpListener>, Sender<bool>)>>,
    link: RwLock<HashMap<(SocketAddr, SocketAddr), Arc<LinkTcp>>>,
}

impl ManagerTcpInner {
    pub fn new(inner: Arc<SessionManagerInner>) -> Self {  
        Self {
            inner,
            listener: RwLock::new(HashMap::new()),
            link: RwLock::new(HashMap::new()),
        }
    }

    // @TODO: remove the allow #[allow(clippy::infallible_destructuring_match)] when adding more transport links
    #[allow(clippy::infallible_destructuring_match)]
    async fn new_link(&self, a_self: &Arc<Self>, dst: &Locator, transport: Arc<Transport>) -> ZResult<Arc<LinkTcp>> {
        let dst = get_tcp_addr!(dst);
        
        // Create the TCP connection
        let stream = match TcpStream::connect(dst).await {
            Ok(stream) => stream,
            Err(e) => return Err(zerror!(ZErrorKind::Other{
                descr: format!("{}", e)
            }))
        };
        
        // Create a new link object
        let link = Arc::new(LinkTcp::new(stream, transport.clone(), a_self.clone()));
        let key = (link.src_addr, link.dst_addr);
        self.link.write().await.insert(key, link.clone());
        
        // Spawn the receive loop for the new link
        LinkTcp::start(link.clone());

        Ok(link)
    }

    // @TODO: remove the allow #[allow(clippy::infallible_destructuring_match)] when adding more transport links
    #[allow(clippy::infallible_destructuring_match)]
    async fn del_link(&self, src: &Locator, dst: &Locator) -> ZResult<Arc<LinkTcp>> {
        let src = get_tcp_addr!(src);
        let dst = get_tcp_addr!(dst);

        // Remove the link from the manager list
        match self.link.write().await.remove(&(*src, *dst)) {
            Some(link) => Ok(link),
            None => Err(zerror!(ZErrorKind::Other{
                descr: format!("No active TCP link ({} => {})", src, dst)
            }))
        }
    }

    // @TODO: remove the allow #[allow(clippy::infallible_destructuring_match)] when adding more transport links
    #[allow(clippy::infallible_destructuring_match)]
    async fn get_link(&self, src: &Locator, dst: &Locator) -> ZResult<Arc<LinkTcp>> {
        let src = get_tcp_addr!(src);
        let dst = get_tcp_addr!(dst);

        // Remove the link from the manager list
        match self.link.write().await.get(&(*src, *dst)) {
            Some(link) => Ok(link.clone()),
            None => Err(zerror!(ZErrorKind::Other{
                descr: format!("No active TCP link ({} => {})", src, dst)
            }))
        }
    }

    // @TODO: remove the allow #[allow(clippy::infallible_destructuring_match)] when adding more transport links
    #[allow(clippy::infallible_destructuring_match)]
    async fn new_listener(&self, a_self: &Arc<Self>, locator: &Locator) -> ZResult<()> {
        let addr = get_tcp_addr!(locator);
  
        // Bind the TCP socket
        let socket = match TcpListener::bind(addr).await {
            Ok(socket) => Arc::new(socket),
            Err(e) => return Err(zerror!(ZErrorKind::Other{
                descr: format!("{}", e)
            }))
        };

        // Create the channel necessary to break the accept loop
        let (sender, receiver) = channel::<bool>(1);
        // Update the list of active listeners on the manager
        self.listener.write().await.insert(*addr, (socket.clone(), sender));

        // Spawn the accept loop for the listener
        let c_self = a_self.clone();
        let c_addr = *addr;
        task::spawn(async move {
            // Wait for the accept loop to terminate
            accept_loop(&c_self, &socket, receiver).await; 
            // Delete the listener from the manager
            c_self.listener.write().await.remove(&c_addr);
        });
        Ok(())
    }

    // @TODO: remove the allow #[allow(clippy::infallible_destructuring_match)] when adding more transport links
    #[allow(clippy::infallible_destructuring_match)]
    async fn del_listener(&self, _a_self: &Arc<Self>, locator: &Locator) -> ZResult<()> {
        let addr = get_tcp_addr!(locator);

        // Stop the listener
        match self.listener.write().await.remove(&addr) {
            Some((_socket, sender)) => {
                sender.send(false).await;
                Ok(())
            },
            None => Err(zerror!(ZErrorKind::Other{
                descr: format!("No TCP listener on locator: {}", locator)
            }))
        }
    }
  
    async fn get_listeners(&self) -> Vec<Locator> {
        self.listener.read().await.keys().map(|x| Locator::Tcp(*x)).collect()
    }
}

async fn accept_loop(a_self: &Arc<ManagerTcpInner>, socket: &Arc<TcpListener>, receiver: Receiver<bool>) {
    // The accept future
    async fn accept(a_self: &Arc<ManagerTcpInner>, socket: &TcpListener) -> Option<bool> {
        // Wait for incoming connections
        let stream = match socket.accept().await {
            Ok((stream, _)) => stream,
            Err(_) => return Some(true)
        };

        // Retrieve the initial temporary session 
        let transport = a_self.inner.get_initial_session().await.transport.clone();
        // Create the new link object
        let link = Arc::new(LinkTcp::new(stream, transport.clone(), a_self.clone()));

        // Store a reference to the link into the manger
        a_self.link.write().await.insert((link.src_addr, link.dst_addr), link.clone());

        // Store a reference to the link into the session
        if transport.add_link(Link::Tcp(link.clone())).await.is_err() {
            return Some(false)
        }

        // Spawn the receive loop for the new link
        LinkTcp::start(link.clone());

        Some(true)
    }

    loop {
        let stop = receiver.recv();
        let accept = accept(&a_self, &socket);
        match accept.race(stop).await {
            Some(true) => continue,
            Some(false) => break,
            None => break
        }
    }
}