use async_std::net::{SocketAddr, TcpListener, TcpStream};
use async_std::prelude::*;
use async_std::sync::{Arc, channel, Mutex, Sender, RwLock, Receiver};
use async_std::task;
use std::collections::HashMap;
use std::net::Shutdown;

#[cfg(write_vectored)]
use std::io::IoSlice;

use crate::{zerror, to_zerror};
use crate::core::{ZError, ZErrorKind, ZResult};
use crate::io::{ArcSlice, RBuf};
use crate::proto::Message;
use crate::session::{SessionManagerInner, TransportRx};
use crate::link::{Link, Locator};
use zenoh_util::{zasynclock, zasyncread, zasyncwrite};


// Default MTU
const DEFAULT_MTU: usize = 65_535;

configurable!{
    // Size of buffer used to read from socket
    static ref READ_BUFFER_SIZE: usize = 128 * 1_024;
    // Size of buffer used to read from socket
    static ref MESSAGES_TO_READ: usize = 1_024;
}


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
    transport: Mutex<Arc<TransportRx>>,
    // The reference to the associated link manager
    manager: Arc<ManagerTcpInner>,
    // Channel for stopping the read task
    ch_send: Sender<bool>,
    ch_recv: Receiver<bool>
}

impl LinkTcp {
    fn new(socket: TcpStream, transport: Arc<TransportRx>, manager: Arc<ManagerTcpInner>) -> LinkTcp {
        // Retrieve the source and destination socket addresses
        let src_addr = socket.local_addr().unwrap();
        let dst_addr = socket.peer_addr().unwrap();
        // The channel for stopping the read task
        let (sender, receiver) = channel::<bool>(1);
        // Build the LinkTcp
        LinkTcp {
            socket,
            src_addr,
            dst_addr,
            src_locator: Locator::Tcp(src_addr),
            dst_locator: Locator::Tcp(dst_addr),
            buff_size: *READ_BUFFER_SIZE,
            transport: Mutex::new(transport),
            manager,
            ch_send: sender,
            ch_recv: receiver
        }
    }

    pub async fn close(&self) -> ZResult<()> {
        let _ = self.socket.shutdown(Shutdown::Both);
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
        task::spawn(async move {
            let link_obj = Link::Tcp(link.clone());
            let err = read_task(&link, &link_obj).await;
            // Remove the link in case of IO error
            let _ = link.manager.del_link(&link.get_src(), &link.get_dst()).await;
            if err {
                let _ = zasynclock!(link.transport).link_err(&link_obj).await;
            }
        });
    }

    pub async fn stop(&self) -> ZResult<()> {
        self.ch_send.send(false).await;
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

async fn read_task(link: &Arc<LinkTcp>, link_obj: &Link) -> bool {
    async fn read_loop(link: &Arc<LinkTcp>, link_obj: &Link) -> Option<bool> {
        let mut buff = RBuf::new();
        let mut messages: Vec<Message> = Vec::with_capacity(*MESSAGES_TO_READ);

        let mut guard = zasynclock!(link.transport);
        loop {
            let mut rbuf = vec![0u8; link.buff_size];
            match (&link.socket).read(&mut rbuf).await {
                Ok(n) => { 
                    // Reading zero bytes means error
                    if n == 0 {
                        // Failed to read from the TCP socket
                        return Some(true)
                    }
                    buff.add_slice(ArcSlice::new(Arc::new(rbuf), 0, n));
                },
                Err(_) => {
                    // Error reading from the TCP socket
                    return Some(true)
                }
            }
            // Read all the messages
            loop {
                let pos = buff.get_pos();
                match buff.read_message() {
                    Ok(message) => {
                        messages.push(message);
                        buff.clean_read_slices();
                    },
                    Err(_) => {
                        if buff.set_pos(pos).is_err() {
                            panic!("Unrecoverable error in TCP read loop!")
                        }
                        break
                    }
                }
            }

            // Propagate the messages to the upper logic
            for msg in messages.drain(..) {
                if let Some(transport) = guard.receive_message(&link_obj, msg).await {
                    *guard = transport;
                }
            }
        }
    }

    let stop = link.ch_recv.recv();
    let read_loop = read_loop(link, link_obj);
    match read_loop.race(stop).await {
        Some(value) => value,
        None => true
    }
}


/*************************************/
/*          LISTENER                 */
/*************************************/
pub struct ManagerTcp(Arc<ManagerTcpInner>);

impl ManagerTcp {
    pub fn new(manager: Arc<SessionManagerInner>) -> Self {  
        Self(Arc::new(ManagerTcpInner::new(manager)))
    }

    pub async fn new_link(&self, dst: &Locator, transport: Arc<TransportRx>) -> ZResult<Link> {
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


struct ListenerTcpInner {
    socket: Arc<TcpListener>,
    sender: Sender<bool>,
    receiver: Receiver<bool>
}

impl ListenerTcpInner {
    fn new(socket: Arc<TcpListener>) -> ListenerTcpInner {
        // Create the channel necessary to break the accept loop
        let (sender, receiver) = channel::<bool>(1);
        // Update the list of active listeners on the manager
        ListenerTcpInner {
            socket,
            sender,
            receiver
        }
    }
}

struct ManagerTcpInner {
    inner: Arc<SessionManagerInner>,
    listener: RwLock<HashMap<SocketAddr, Arc<ListenerTcpInner>>>,
    link: RwLock<HashMap<(SocketAddr, SocketAddr), Arc<LinkTcp>>>
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
    async fn new_link(&self, a_self: &Arc<Self>, dst: &Locator, transport: Arc<TransportRx>) -> ZResult<Arc<LinkTcp>> {
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
        match zasyncwrite!(self.link).remove(&(*src, *dst)) {
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
        match zasyncwrite!(self.link).get(&(*src, *dst)) {
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

        // Update the list of active listeners on the manager
        let listener = Arc::new(ListenerTcpInner::new(socket.clone()));
        zasyncwrite!(self.listener).insert(*addr, listener.clone());

        // Spawn the accept loop for the listener
        let c_self = a_self.clone();
        let c_addr = *addr;
        task::spawn(async move {
            // Wait for the accept loop to terminate
            accept_task(&c_self, listener).await; 
            // Delete the listener from the manager
            zasyncwrite!(c_self.listener).remove(&c_addr);
        });
        Ok(())
    }

    // @TODO: remove the allow #[allow(clippy::infallible_destructuring_match)] when adding more transport links
    #[allow(clippy::infallible_destructuring_match)]
    async fn del_listener(&self, _a_self: &Arc<Self>, locator: &Locator) -> ZResult<()> {
        let addr = get_tcp_addr!(locator);

        // Stop the listener
        match zasyncwrite!(self.listener).remove(&addr) {
            Some(listener) => {
                listener.sender.send(false).await;
                Ok(())
            },
            None => Err(zerror!(ZErrorKind::Other{
                descr: format!("No TCP listener on locator: {}", locator)
            }))
        }
    }
  
    async fn get_listeners(&self) -> Vec<Locator> {
        zasyncread!(self.listener).keys().map(|x| Locator::Tcp(*x)).collect()
    }
}

async fn accept_task(a_self: &Arc<ManagerTcpInner>, listener: Arc<ListenerTcpInner>) {
    // The accept future
    async fn accept_loop(a_self: &Arc<ManagerTcpInner>, listener: &Arc<ListenerTcpInner>) -> Option<bool> {
        loop {
            // Wait for incoming connections
            let stream = match listener.socket.accept().await {
                Ok((stream, _)) => stream,
                Err(_) => return Some(true)
            };

            // Retrieve the initial temporary session 
            let initial = a_self.inner.get_initial_session().await;
            // Create the new link object
            let link = Arc::new(LinkTcp::new(stream, initial.rx.clone(), a_self.clone()));

            // Store a reference to the link into the manger
            zasyncwrite!(a_self.link).insert((link.src_addr, link.dst_addr), link.clone());

            // Store a reference to the link into the session
            if initial.add_link(Link::Tcp(link.clone())).await.is_err() {
                continue
            }

            // Spawn the receive loop for the new link
            LinkTcp::start(link.clone());
        }
    }

    let stop = listener.receiver.recv();
    let accept_loop = accept_loop(&a_self, &listener);
    let _ = accept_loop.race(stop).await;
}