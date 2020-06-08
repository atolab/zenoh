use async_std::net::{SocketAddr, TcpListener, TcpStream};
use async_std::prelude::*;
use async_std::sync::{Arc, channel, Mutex, Sender, RwLock, Receiver, Weak};
use async_std::task;
use async_trait::async_trait;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt;
use std::net::Shutdown;
use std::time::Duration;

use crate::io::{ArcSlice, RBuf};
use crate::proto::SessionMessage;
use crate::session::{SessionManagerInner, Action, Transport};
use super::{Link, LinkTrait, Locator, ManagerTrait};
use zenoh_util::{zasynclock, zasyncread, zasyncwrite, zerror};
use zenoh_util::core::{ZResult, ZError, ZErrorKind};


// Default MTU (TCP PDU) in bytes.
const DEFAULT_MTU: usize = 65_536;

zconfigurable! {
    // Size of buffer used to read from socket.
    static ref TCP_READ_BUFFER_SIZE: usize = 2*DEFAULT_MTU;
    // Size of the vector used to deserialize the messages.
    static ref TCP_READ_MESSAGES_VEC_SIZE: usize = 32;
    // Amount of time in microseconds to throttle the accept loop upon an error. 
    // Default set to 100 ms.
    static ref TCP_ACCEPT_THROTTLE_TIME: u64 = 100_000;
}


#[macro_export]
macro_rules! get_tcp_addr {
    ($locator:expr) => (match $locator {
        Locator::Tcp(addr) => addr,
        // @TODO: uncomment the following when more links are added
        // _ => {
        //    let e = format!("Not a TCP locator: {}", $locator);
        //    log::debug!("{}", e);    
        //    return zerror!(ZErrorKind::InvalidLocator {
        //        descr: e
        //    })
        // }
    });
}

/*************************************/
/*              LINK                 */
/*************************************/
pub struct Tcp {
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
    // The reference to the associated transport
    transport: Mutex<Transport>,
    // The reference to the associated link manager
    manager: Arc<ManagerTcpInner>,
    // Channel for stopping the read task
    ch_send: Sender<()>,
    ch_recv: Receiver<()>,
    // Weak reference to self
    w_self: RwLock<Option<Weak<Self>>>
}

impl Tcp {
    fn new(socket: TcpStream, transport: Transport, manager: Arc<ManagerTcpInner>) -> Tcp {
        // Retrieve the source and destination socket addresses
        let src_addr = socket.local_addr().unwrap();
        let dst_addr = socket.peer_addr().unwrap();
        // The channel for stopping the read task
        let (sender, receiver) = channel::<()>(1);
        // Build the Tcp
        Tcp {
            socket,
            src_addr,
            dst_addr,
            src_locator: Locator::Tcp(src_addr),
            dst_locator: Locator::Tcp(dst_addr),
            transport: Mutex::new(transport),
            manager,
            ch_send: sender,
            ch_recv: receiver,
            w_self: RwLock::new(None)
        }
    }

    fn initizalize(&self, w_self: Weak<Self>) {
        *self.w_self.try_write().unwrap() = Some(w_self);
    }
}

#[async_trait]
impl LinkTrait for Tcp {
    async fn close(&self) -> ZResult<()> {
        log::trace!("Closing TCP link: {}", self);

        // Stop the read loop
        self.stop().await?;
        // Close the underlying TCP socket
        let _ = self.socket.shutdown(Shutdown::Both);
        // Delete the link from the manager
        let _ = self.manager.del_link(&self.src_addr, &self.dst_addr).await;
        Ok(())
    }
    
    async fn send(&self, buffer: &[u8]) -> ZResult<()> {
        log::trace!("Sending {} bytes on TCP link: {}", buffer.len(), self);

        let res = (&self.socket).write_all(buffer).await;
        if let Err(e) = res {
            log::debug!("Transmission error on TCP link {}: {}", self, e);
            return zerror!(ZErrorKind::IOError {
                descr: format!("{}", e)
            })
        }            

        Ok(())
    }

    async fn start(&self) -> ZResult<()> {
        log::trace!("Starting read loop on TCP link: {}", self);
        let link = if let Some(link) = zasyncread!(self.w_self).as_ref() {
            if let Some(link) = link.upgrade() {
                link
            } else {
                let e = format!("TCP link does not longer exist: {}", self);
                log::error!("{}", e);
                return zerror!(ZErrorKind::Other {
                    descr: e
                })
            }
        } else {
            let e = format!("TCP link is unitialized: {}", self);
            log::error!("{}", e);
            return zerror!(ZErrorKind::Other {
                descr: e
            })
        };

        // Spawn the read task
        task::spawn(read_task(link));

        Ok(())
    }

    async fn stop(&self) -> ZResult<()> {
        log::trace!("Stopping read loop on TCP link: {}", self);
        self.ch_send.send(()).await;
        Ok(())
    }

    fn get_src(&self) -> Locator {
        self.src_locator.clone()
    }

    fn get_dst(&self) -> Locator {
        self.dst_locator.clone()
    }

    fn get_mtu(&self) -> usize {
        DEFAULT_MTU
    }

    fn is_ordered(&self) -> bool {
        true
    }

    fn is_reliable(&self) -> bool {
        true
    }

    fn is_streamed(&self) -> bool {
        true
    }
}

async fn read_task(link: Arc<Tcp>) {
    let read_loop = async {
        // The link object to be passed to the transport
        let link_obj: Link = link.clone();
        // Acquire the lock on the transport
        let mut guard = zasynclock!(link.transport);

        // // The RBuf to read a message batch onto
        let mut rbuf = RBuf::new();

        // The buffer allocated to read from a single syscall
        let mut buffer = vec![0u8; *TCP_READ_BUFFER_SIZE];

        // The vector for storing the deserialized messages
        let mut messages: Vec<SessionMessage> = Vec::with_capacity(*TCP_READ_MESSAGES_VEC_SIZE);

        // An example of the received buffer and the correspoding indexes is: 
        //
        //  0 1 2 3 4 5 6 7  ..  n 0 1 2 3 4 5 6 7      k 0 1 2 3 4 5 6 7      x
        // +-+-+-+-+-+-+-+-+ .. +-+-+-+-+-+-+-+-+-+ .. +-+-+-+-+-+-+-+-+-+ .. +-+
        // | L | First batch      | L | Second batch     | L | Incomplete batch |
        // +-+-+-+-+-+-+-+-+ .. +-+-+-+-+-+-+-+-+-+ .. +-+-+-+-+-+-+-+-+-+ .. +-+
        //
        // - Decoding Iteration 0:
        //      r_l_pos = 0; r_s_pos = 2; r_e_pos = n;
        // 
        // - Decoding Iteration 1:
        //      r_l_pos = n; r_s_pos = n+2; r_e_pos = n+k;
        //
        // - Decoding Iteration 2:
        //      r_l_pos = n+k; r_s_pos = n+k+2; r_e_pos = n+k+x;
        //  
        // In this example, Iteration 2 will fail since the batch is incomplete and
        // fewer bytes than the ones indicated in the length are read. The incomplete
        // batch is hence stored in a RBuf in order to read more bytes from the socket 
        // and deserialize a complete batch. In case it is not possible to read at once 
        // the 2 bytes indicating the message batch length (i.e., only the first byte is
        // available), the first byte is copied at the beginning of the buffer and more
        // bytes are read in the next iteration.

        // The read position of the length bytes in the buffer
        let mut r_l_pos: usize;
        // The start read position of the message bytes in the buffer
        let mut r_s_pos: usize;
        // The end read position of the messages bytes in the buffer
        let mut r_e_pos: usize;
        // The write position in the buffer
        let mut w_pos: usize = 0;        

        // Keep track of the number of bytes still to read for incomplete message batches
        let mut left_to_read: usize = 0;

        // Macro to handle a link error
        macro_rules! zlinkerror {
            () => {
                // Close the underlying TCP socket
                let _ = link.socket.shutdown(Shutdown::Both);
                // Delete the link from the manager
                let _ = link.manager.del_link(&link.src_addr, &link.dst_addr).await;
                // Notify the transport
                guard.link_err(&link_obj).await;
                // Exit
                return Ok(())
            };
        }

        // Macro to add a slice to the RBuf
        macro_rules! zaddslice {
            ($start:expr, $end:expr) => {
                let tot = $end - $start;
                let mut slice = Vec::with_capacity(tot);
                slice.extend_from_slice(&buffer[$start..$end]);
                rbuf.add_slice(ArcSlice::new(Arc::new(slice), 0, tot));
            };
        }

        // Macro for deserializing the messages
        macro_rules! zdeserialize {
            () => {
                // Deserialize all the messages from the current RBuf
                loop {
                    match rbuf.read_session_message() {
                        Ok(msg) => messages.push(msg),
                        Err(e) => match e.get_kind() {
                            ZErrorKind::InvalidMessage { descr } => {
                                log::warn!("Closing TCP link {}: {}", link, descr);
                                zlinkerror!();
                            },
                            _ => break
                        }
                    }                 
                }

                for msg in messages.drain(..) {
                    let action = guard.receive_message(&link_obj, msg).await;
                    // Enforce the action as instructed by the upper logic
                    match action {
                        Action::Read => {},
                        Action::ChangeTransport(transport) => {
                            log::debug!("Change transport on TCP link: {}", link);
                            *guard = transport
                        },
                        Action::Close => {
                            log::debug!("Closing TCP link: {}", link);
                            // Close the underlying TCP socket
                            let _ = link.socket.shutdown(Shutdown::Both);
                            // Delete the link from the manager
                            let _ = link.manager.del_link(&link.src_addr, &link.dst_addr).await;
                            // Exit
                            return Ok(())
                        }
                    }  
                }
            };
        }
        
        log::trace!("Ready to read from TCP link: {}", link);
        loop {            
            // Async read from the TCP socket
            match (&link.socket).read(&mut buffer[w_pos..]).await {
                Ok(mut n) => {
                    if n == 0 {  
                        // Reading 0 bytes means error
                        log::warn!("Zero bytes reading on TCP link: {}", link);
                        zlinkerror!();
                    }

                    // If we had a w_pos different from 0, it means we add an incomplete length reading
                    // in the previous iteration: we have only read 1 byte instead of 2.
                    if w_pos != 0 {
                        // Update the number of read bytes by adding the bytes we have already read in
                        // the previous iteration. "n" now is the index pointing to the last valid
                        // position in the buffer.
                        n += w_pos;
                        // Reset the write index
                        w_pos = 0;
                    }

                    // Reset the read length index
                    r_l_pos = 0;                    

                    // Check if we had an incomplete message batch
                    if left_to_read > 0 {
                        // Check if still we haven't read enough bytes
                        if n < left_to_read {
                            // Update the number of bytes still to read;
                            left_to_read -= n;
                            // Copy the relevant buffer slice in the RBuf
                            zaddslice!(0, n);
                            // Keep reading from the socket
                            continue
                        }
                        
                        // We are ready to decode a complete message batch
                        // Copy the relevant buffer slice in the RBuf
                        zaddslice!(0, left_to_read);
                        // Read the batch
                        zdeserialize!();
                        
                        // Update the read length index
                        r_l_pos = left_to_read;
                        // Reset the remaining bytes to read
                        left_to_read = 0;  

                        // Check if we have completely read the batch
                        if buffer[r_l_pos..n].is_empty() {  
                            // Reset the RBuf
                            rbuf.clear();                         
                            // Keep reading from the socket
                            continue
                        }                                                       
                    }

                    // Loop over all the buffer which may contain multiple message batches
                    loop {
                        // Compute the total number of bytes we have read
                        let read = buffer[r_l_pos..n].len();
                        // Check if we have read the 2 bytes necessary to decode the message length                
                        if read < 2 {    
                            // Copy the bytes at the beginning of the buffer
                            buffer.copy_within(r_l_pos..n, 0); 
                            // Update the write index                  
                            w_pos = read;
                            // Keep reading from the socket
                            break
                        }
                        
                        // We have read at least two bytes in the buffer, update the read start index
                        r_s_pos = r_l_pos + 2;
                        // Read the lenght as litlle endian from the buffer (array of 2 bytes)
                        let length: [u8; 2] = buffer[r_l_pos..r_s_pos].try_into().unwrap();
                        // Decode the total amount of bytes that we are expected to read
                        let to_read = u16::from_le_bytes(length) as usize;

                        // Check if we have really something to read
                        if to_read == 0 {                            
                            // Keep reading from the socket
                            break
                        }
                        
                        // Compute the number of useful bytes we have actually read
                        let read = buffer[r_s_pos..n].len();

                        if read == 0 {
                            // The buffer might be empty in case of having read only the two bytes 
                            // of the length and no additional bytes are left in the reading buffer
                            left_to_read = to_read;
                            // Keep reading from the socket
                            break 
                        } else if read < to_read {
                            // We haven't read enough bytes for a complete batch, so
                            // we need to store the bytes read so far and keep reading

                            // Update the number of bytes we still have to read to 
                            // obtain a complete message batch for decoding
                            left_to_read = to_read - read;

                            // Copy the buffer in the RBuf if not empty                            
                            zaddslice!(r_s_pos, n);

                            // Keep reading from the socket
                            break                            
                        }

                        // We have at least one complete message batch we can deserialize
                        // Compute the read end index of the message batch in the buffer
                        r_e_pos = r_s_pos + to_read;

                        // Copy the relevant buffer slice in the RBuf
                        zaddslice!(r_s_pos, r_e_pos);
                        // Deserialize the batch
                        zdeserialize!();

                        // Reset the current RBuf
                        rbuf.clear();
                        // Reset the remaining bytes to read
                        left_to_read = 0;

                        // Check if we are done with the current reading buffer
                        if buffer[r_e_pos..n].is_empty() {
                            // Keep reading from the socket
                            break
                        }

                        // Update the read length index to read the next message batch
                        r_l_pos = r_e_pos;
                    }
                },
                Err(e) => {
                    log::warn!("Reading error on TCP link {}: {}", link, e);
                    zlinkerror!();
                }
            }
        }
    };

    // Execute the read loop 
    let stop = link.ch_recv.recv();
    let _ = read_loop.race(stop).await;
}

impl Drop for Tcp {
    fn drop(&mut self) {
        // Close the underlying TCP socket
        let _ = self.socket.shutdown(Shutdown::Both);
    }
}

impl fmt::Display for Tcp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} => {}", self.src_addr, self.dst_addr)?;
        Ok(())
    }
}

impl fmt::Debug for Tcp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tcp")
            .field("src", &self.src_addr)
            .field("dst", &self.dst_addr)
            .finish()
    }
} 

/*************************************/
/*          LISTENER                 */
/*************************************/
pub struct ManagerTcp(Arc<ManagerTcpInner>);

impl ManagerTcp {
    pub(crate) fn new(manager: Arc<SessionManagerInner>) -> Self {  
        Self(Arc::new(ManagerTcpInner::new(manager)))
    }
}

#[async_trait]
impl ManagerTrait for ManagerTcp {
    // @TODO: remove the allow #[allow(clippy::infallible_destructuring_match)] when adding more transport links
    #[allow(clippy::infallible_destructuring_match)]
    async fn new_link(&self, dst: &Locator, transport: &Transport) -> ZResult<Link> {
        let dst = get_tcp_addr!(dst);
        let link: Link = self.0.new_link(&self.0, dst, transport).await?;
        Ok(link)
    }

    // @TODO: remove the allow #[allow(clippy::infallible_destructuring_match)] when adding more transport links
    #[allow(clippy::infallible_destructuring_match)]
    async fn del_link(&self, src: &Locator, dst: &Locator) -> ZResult<Link> {
        let src = get_tcp_addr!(src);
        let dst = get_tcp_addr!(dst);
        let link: Link = self.0.del_link(src, dst).await?;
        Ok(link)
    }

    // @TODO: remove the allow #[allow(clippy::infallible_destructuring_match)] when adding more transport links
    #[allow(clippy::infallible_destructuring_match)]
    async fn get_link(&self, src: &Locator, dst: &Locator) -> ZResult<Link> {
        let src = get_tcp_addr!(src);
        let dst = get_tcp_addr!(dst);
        let link: Link = self.0.get_link(src, dst).await?;
        Ok(link)
    }

    // @TODO: remove the allow #[allow(clippy::infallible_destructuring_match)] when adding more transport links
    #[allow(clippy::infallible_destructuring_match)]
    async fn new_listener(&self, locator: &Locator) -> ZResult<()> {
        let addr = get_tcp_addr!(locator);
        self.0.new_listener(&self.0, addr).await
    }

    // @TODO: remove the allow #[allow(clippy::infallible_destructuring_match)] when adding more transport links
    #[allow(clippy::infallible_destructuring_match)]
    async fn del_listener(&self, locator: &Locator) -> ZResult<()> {
        let addr = get_tcp_addr!(locator);
        self.0.del_listener(&self.0, addr).await
    }
  
    async fn get_listeners(&self) -> Vec<Locator> {
        self.0.get_listeners().await
    }
}


struct ListenerTcpInner {
    socket: Arc<TcpListener>,
    sender: Sender<()>,
    receiver: Receiver<()>
}

impl ListenerTcpInner {
    fn new(socket: Arc<TcpListener>) -> ListenerTcpInner {
        // Create the channel necessary to break the accept loop
        let (sender, receiver) = channel::<()>(1);
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
    link: RwLock<HashMap<(SocketAddr, SocketAddr), Arc<Tcp>>>
}

impl ManagerTcpInner {
    pub fn new(inner: Arc<SessionManagerInner>) -> Self {  
        Self {
            inner,
            listener: RwLock::new(HashMap::new()),
            link: RwLock::new(HashMap::new()),
        }
    }

    async fn new_link(&self, a_self: &Arc<Self>, dst: &SocketAddr, transport: &Transport) -> ZResult<Arc<Tcp>> {
        // Create the TCP connection
        let stream = match TcpStream::connect(dst).await {
            Ok(stream) => stream,
            Err(e) => {
                let e = format!("Can not create a new TCP link bound to {}: {}", dst, e);
                log::warn!("{}", e);
                return zerror!(ZErrorKind::Other {
                    descr: e
                })
            }
        };
        
        // Create a new link object
        let link = Arc::new(Tcp::new(stream, transport.clone(), a_self.clone()));
        link.initizalize(Arc::downgrade(&link));

        // Store the ink object
        let key = (link.src_addr, link.dst_addr);
        self.link.write().await.insert(key, link.clone());
        
        // Spawn the receive loop for the new link
        let _ = link.start().await;

        Ok(link)
    }

    async fn del_link(&self, src: &SocketAddr, dst: &SocketAddr) -> ZResult<Arc<Tcp>> {
        // Remove the link from the manager list
        match zasyncwrite!(self.link).remove(&(*src, *dst)) {
            Some(link) => Ok(link),
            None => {
                let e = format!("Can not delete TCP link because it has not been found: {} => {}", src, dst);
                log::trace!("{}", e);
                zerror!(ZErrorKind::Other {
                    descr: e
                })
            }
        }
    }

    async fn get_link(&self, src: &SocketAddr, dst: &SocketAddr) -> ZResult<Arc<Tcp>> {
        // Remove the link from the manager list
        match zasyncwrite!(self.link).get(&(*src, *dst)) {
            Some(link) => Ok(link.clone()),
            None => {
                let e = format!("Can not get TCP link because it has not been found: {} => {}", src, dst);
                log::trace!("{}", e);
                zerror!(ZErrorKind::Other {
                    descr: e
                })
            }
        }
    }

    async fn new_listener(&self, a_self: &Arc<Self>, addr: &SocketAddr) -> ZResult<()> {
        // Bind the TCP socket
        let socket = match TcpListener::bind(addr).await {
            Ok(socket) => Arc::new(socket),
            Err(e) => {
                let e = format!("Can not create a new TCP listener on {}: {}", addr, e);
                log::warn!("{}", e);
                return zerror!(ZErrorKind::Other {
                    descr: e
                })
            }
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

    async fn del_listener(&self, _a_self: &Arc<Self>, addr: &SocketAddr) -> ZResult<()> {
        // Stop the listener
        match zasyncwrite!(self.listener).remove(&addr) {
            Some(listener) => {
                listener.sender.send(()).await;
                Ok(())
            },
            None => {
                let e = format!("Can not delete the TCP listener because it has not been found: {}", addr);
                log::trace!("{}", e);
                zerror!(ZErrorKind::Other {
                    descr: e
                })
            }
        }
    }
  
    async fn get_listeners(&self) -> Vec<Locator> {
        zasyncread!(self.listener).keys().map(|x| Locator::Tcp(*x)).collect()
    }
}

async fn accept_task(a_self: &Arc<ManagerTcpInner>, listener: Arc<ListenerTcpInner>) {
    // The accept future
    let accept_loop = async {
        log::trace!("Ready to accept TCP connections on: {:?}", listener.socket.local_addr());
        loop {
            // Wait for incoming connections
            let stream = match listener.socket.accept().await {
                Ok((stream, _)) => stream,
                Err(e) => {
                    log::warn!("{}. Hint: you might want to increase the system open file limit", e);
                    // Throttle the accept loop upon an error
                    // NOTE: This might be due to various factors. However, the most common case is that
                    //       the process has reached the maximum number of open files in the system. On
                    //       Linux systems this limit can be changed by using the "ulimit" command line 
                    //       tool. In case of systemd-based systems, this can be changed by using the 
                    //       "sysctl" command line tool.
                    task::sleep(Duration::from_micros(*TCP_ACCEPT_THROTTLE_TIME)).await;
                    continue
                }
            };

            log::debug!("Accepted TCP connection on {:?}: {:?}", stream.local_addr(), stream.peer_addr());

            // Retrieve the initial temporary session 
            let initial = a_self.inner.get_initial_transport().await;
            // Create the new link object
            let link = Arc::new(Tcp::new(stream, initial, a_self.clone()));
            link.initizalize(Arc::downgrade(&link));

            // Store a reference to the link into the manager
            zasyncwrite!(a_self.link).insert((link.src_addr, link.dst_addr), link.clone());

            // Spawn the receive loop for the new link
            let _ = link.start().await;
        }
    };

    let stop = listener.receiver.recv();
    let _ = accept_loop.race(stop).await;
}