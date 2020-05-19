use async_std::net::{SocketAddr, TcpListener, TcpStream};
use async_std::prelude::*;
use async_std::sync::{Arc, channel, Mutex, Sender, RwLock, Receiver, Weak};
use async_std::task;
use async_trait::async_trait;
use std::collections::HashMap;
use std::convert::TryInto;
use std::net::Shutdown;

#[cfg(write_vectored)]
use std::io::IoSlice;

use crate::{zerror, to_zerror};
use crate::core::{ZError, ZErrorKind, ZResult};
use crate::io::{ArcSlice, RBuf};
use crate::proto::SessionMessage;
use crate::session::{SessionManagerInner, Action, Transport};
use super::{Link, LinkTrait, Locator, ManagerTrait};
use zenoh_util::{zasynclock, zasyncread, zasyncwrite};


// Default MTU
const DEFAULT_MTU: usize = 65_535;

configurable!{
    // Size of buffer used to read from socket
    static ref READ_BUFFER_SIZE: usize = 64 * 1_024;
    // Size of buffer used to read from socket
    static ref MESSAGES_TO_READ: usize = 64;
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
    // The buffer size to use in the read operation
    buff_size: usize,
    // The reference to the associated transport
    transport: Mutex<Transport>,
    // The reference to the associated link manager
    manager: Arc<ManagerTcpInner>,
    // Channel for stopping the read task
    ch_send: Sender<bool>,
    ch_recv: Receiver<bool>,
    // Weak reference to self
    w_self: RwLock<Option<Weak<Self>>>
}

impl Tcp {
    fn new(socket: TcpStream, transport: Transport, manager: Arc<ManagerTcpInner>) -> Tcp {
        // Retrieve the source and destination socket addresses
        let src_addr = socket.local_addr().unwrap();
        let dst_addr = socket.peer_addr().unwrap();
        // The channel for stopping the read task
        let (sender, receiver) = channel::<bool>(1);
        // Build the Tcp
        Tcp {
            socket,
            src_addr,
            dst_addr,
            src_locator: Locator::Tcp(src_addr),
            dst_locator: Locator::Tcp(dst_addr),
            buff_size: *READ_BUFFER_SIZE,
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
        // Stop the read loop
        self.stop().await?;
        // Close the underlying TCP socket
        let _ = self.socket.shutdown(Shutdown::Both);
        // Delete the link from the manager
        let _ = self.manager.del_link(&self.src_addr, &self.dst_addr).await;
        Ok(())
    }
    
    async fn send(&self, buffer: &[u8]) -> ZResult<()> {
        // #[cfg(write_vectored)]
        // {
        //     let mut ioslices = &mut buffer.as_ioslices()[..];
        //     while ! ioslices.is_empty() {
        //         match (&self.socket).write_vectored(ioslices).await {
        //             Ok(size) => {
        //                 ioslices = IoSlice::advance(ioslices, size);
        //             },
        //             err => {err.map_err(to_zerror!(IOError, "on write_vectored".to_string()))?;}
        //         }
        //     }
        // }
        
        // #[cfg(not(write_vectored))]
        // {
        //     let mut sendbuff = Vec::with_capacity(buffer.readable());
        //     for s in buffer.get_slices() {
        //         std::io::Write::write_all(&mut sendbuff, s.as_slice())
        //             .map_err(to_zerror!(IOError, "on buff.write_all".to_string()))?;
        //     }
        //     (&self.socket).write_all(&sendbuff).await
        //         .map_err(to_zerror!(IOError, "on socket.write_all".to_string()))?;
        // }
        
        // println!("Sending: {}", buffer.len());
        (&self.socket).write_all(buffer).await
            .map_err(to_zerror!(IOError, "on socket.write_all".to_string()))?;

        Ok(())
    }

    async fn start(&self) -> ZResult<()> {
        let link = if let Some(link) = zasyncread!(self.w_self).as_ref() {
            if let Some(link) = link.upgrade() {
                link
            } else {
                return Err(zerror!(ZErrorKind::Other{
                    descr: "The Link does not longer exist".to_string()
                }))
            }
        } else {
            panic!("Link is uninitialized");
        };
        task::spawn(read_task(link));

        Ok(())
    }

    async fn stop(&self) -> ZResult<()> {
        self.ch_send.send(false).await;
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
    async fn read_loop(link: &Arc<Tcp>) -> Option<bool> {   
        // The link object to be passed to the transport
        let link_obj: Link = link.clone();
        // Acquire the lock on the transport
        let mut guard = zasynclock!(link.transport);

        // The list of deserliazed messages
        let mut messages: Vec<SessionMessage> = Vec::with_capacity(*MESSAGES_TO_READ);

        // The RBuf to read a message batch onto
        let mut rbuf = RBuf::new();

        // The buffer allocated to read from a single syscall
        let mut buffer = vec![0u8; link.buff_size];        
        // The read position in the buffer
        let mut r_pos: usize = 0;
        let mut w_pos: usize = 0;

        // Mark if we are reading a new message batch
        let mut is_new = true;
        // Variables to keep track of the bytes that still need to be read
        let mut left_to_read: usize = 0;
        
        // Read loop
        loop {
            // Async read from the TCP socket
            // match (&link.socket).read(&mut buffer[w_pos..]).await {            
            //     Ok(mut read) => {
            //         // Reading zero bytes means error
            //         if read == 0 {
            //             // Close the underlying TCP socket
            //             let _ = link.socket.shutdown(Shutdown::Both);
            //             // Delete the link from the manager
            //             let _ = link.manager.del_link(&link.src_addr, &link.dst_addr).await;
            //             // Notify the transport
            //             guard.link_err(&link_obj).await;
            //             // Exit
            //             return Some(false)
            //         } 
                    
            //         // Increment the write position
            //         w_pos += read;

            //         // Loop over all the buffer which may contain multiple message batches
            //         loop {
            //             println!("--- Read pos: {}\t Write pos: {}", r_pos, w_pos);
            //             // Check if this is a new message batch
            //             if is_new {
            //                 // Check if we need to read more from the socket in order to decode 
            //                 // the message length (16 bits), which is encoded as little endian
            //                 if w_pos - r_pos < 2 {
            //                     // Keep reading from the socket
            //                     break
            //                 }

            //                 // Read the first 16 bits (2 bytes) to detect the length of the message
            //                 let end = r_pos + 2;
            //                 // Read the lenght as litlle endian from the buffer (array of 2 bytes in size)
            //                 let length: [u8; 2] = buffer[r_pos..end].try_into().unwrap();
            //                 // Update the total amount of bytes that we are expected to read
            //                 left_to_read = u16::from_le_bytes(length) as usize;       
            //                 println!("\tFrom the socket: {}\tReading {} bytes", read, left_to_read);

            //                 // Decrement the number of useful bytes just read (i.e. minus 2 bytes)
            //                 read -= 2;
            //                 // Mark the current reading as on-going reading
            //                 is_new = false;

            //                 // Check if we still have some bytes left to process
            //                 if read == 0 {
            //                     // Reset the read and write indexes
            //                     r_pos = 0;
            //                     w_pos = 0;
            //                     // Keep reading from the socket
            //                     break
            //                 }

            //                 // Update the read pointer to skip the first 16 bits (2 bytes)
            //                 r_pos = end;
            //             }
                 
            //             // Check if we still need to read more bytes before deserializing the buffer 
            //             if read < left_to_read {
            //                 // In total we have read fewer bytes than expected, add them to the current RBuf
            //                 let mut slice = Vec::with_capacity(read);
            //                 let end = w_pos; // Copy all what we have read
            //                 slice.extend_from_slice(&buffer[r_pos..end]);
            //                 rbuf.add_slice(ArcSlice::new(Arc::new(slice), 0, read));

            //                 // Decrement the amount of bytes still to be read
            //                 left_to_read -= read;
            //                 // Reset the read and write indexes
            //                 r_pos = 0;
            //                 w_pos = 0;
            //                 // Keep reading from the socket
            //                 break
            //             }
                            
            //             // We have read enough bytes, add them to the current RBuf
            //             let mut slice = Vec::with_capacity(left_to_read);
            //             let end = r_pos + left_to_read; // Copy only the bytes needed to complete the current batch
            //             slice.extend_from_slice(&buffer[r_pos..end]);
            //             rbuf.add_slice(ArcSlice::new(Arc::new(slice), 0, left_to_read));

            //             // Deserialize all the messages from the current RBuf
            //             // @TODO: Fix error handling
            //             loop {
            //                 let pos = rbuf.get_pos();
            //                 match rbuf.read_session_message() {
            //                     Ok(msg) => {
            //                         messages.push(msg);
            //                         rbuf.clean_read_slices();
            //                     },
            //                     Err(_) => {
            //                         if rbuf.set_pos(pos).is_err() {
            //                             panic!("Unrecoverable error in TCP read loop!")
            //                         }
            //                         break
            //                     }
            //                 }
            //             }

            //             // Propagate the messages to the upper logic
            //             for msg in messages.drain(..) {
            //                 let action = guard.receive_message(&link_obj, msg).await;
            //                 // Enforce the action as instructed by the upper logic
            //                 match action {
            //                     Action::Read => {},
            //                     Action::ChangeTransport(transport) => *guard = transport,
            //                     Action::Close => {
            //                         // Close the underlying TCP socket
            //                         let _ = link.socket.shutdown(Shutdown::Both);
            //                         // Delete the link from the manager
            //                         let _ = link.manager.del_link(&link.src_addr, &link.dst_addr).await;
            //                         // Exit
            //                         return Some(false)
            //                     }
            //                 }
            //             }

            //             // Reset the current RBuf
            //             rbuf = RBuf::new();                        
            //             // Mark that we are about to process a new message batch
            //             is_new = true;   
            //             // Decrement the number of remaining bytes that still need to be processed
            //             read -= left_to_read;

            //             // Check if we still have some bytes left that need to be processed in the current buffer
            //             if read == 0 {      
            //                 println!("Need to process more!!!"); 
            //                 // Reset the read and write indexes
            //                 r_pos = 0;
            //                 w_pos = 0;
            //                 // We are done, keep reading from the socket
            //                 break
            //             } else                        
            //             // Check if we are in the special case that we have only 1 byte left to read.
            //             // This case is problematic when the only byte left to read is the last byte
            //             // of the buffer (i.e. at index := buffer.len() - 1). If we fall in this case, it 
            //             // would be impossible to read additional bytes on the buffer unless we reset it.
            //             if read == 1 {       
            //                 println!("ARGH!!!");                     
            //                 // Manually copy the last byte at the beginning of the buffer
            //                 buffer[0] = buffer[left_to_read];
            //                 // Reset the read and write indexes
            //                 r_pos = 0;
            //                 w_pos = 1;
            //                 // We are done, keep reading from the socket
            //                 break
            //             }
                        
            //             // Keep processing the current buffer and update the read index
            //             r_pos = left_to_read;
            //             println!("Processing another batch");
            //         }
            //     },
            let mut buffer = vec![0u8; 2];
            match (&link.socket).read_exact(&mut buffer).await {
                Ok(_) => {
                    // Update the total amount of bytes that we are expected to read
                    // println!("<<< Length: {:?}", buffer);
                    let length: [u8; 2] = buffer[0..2].try_into().unwrap();
                    // Update the total amount of bytes that we are expected to read
                    let to_read = u16::from_le_bytes(length) as usize;
                    // Create a slice
                    let mut slice = vec![0u8; to_read];

                    match (&link.socket).read_exact(&mut slice).await {
                        Ok(_) => {
                            // println!("<<< Rx Buffer: {:?}", slice);
                            let mut rbuf = RBuf::new();
                            let len = slice.len();
                            rbuf.add_slice(ArcSlice::new(Arc::new(slice), 0, len));

                            // Deserialize all the messages from the current RBuf
                            // @TODO: Fix error handling
                            loop {
                                let pos = rbuf.get_pos();
                                match rbuf.read_session_message() {
                                    Ok(msg) => {
                                        messages.push(msg);
                                        rbuf.clean_read_slices();
                                    },
                                    Err(_) => {
                                        if rbuf.set_pos(pos).is_err() {
                                            panic!("Unrecoverable error in TCP read loop!")
                                        }
                                        break
                                    }
                                }
                            }

                            // Propagate the messages to the upper logic
                            for msg in messages.drain(..) {
                                let action = guard.receive_message(&link_obj, msg).await;
                                // Enforce the action as instructed by the upper logic
                                match action {
                                    Action::Read => {},
                                    Action::ChangeTransport(transport) => *guard = transport,
                                    Action::Close => {
                                        // Close the underlying TCP socket
                                        let _ = link.socket.shutdown(Shutdown::Both);
                                        // Delete the link from the manager
                                        let _ = link.manager.del_link(&link.src_addr, &link.dst_addr).await;
                                        // Exit
                                        return Some(false)
                                    }
                                }
                            }
                        },
                        Err(_) => {
                            // Close the underlying TCP socket
                            let _ = link.socket.shutdown(Shutdown::Both);
                            // Delete the link from the manager
                            let _ = link.manager.del_link(&link.src_addr, &link.dst_addr).await;
                            // Notify the transport
                            guard.link_err(&link_obj).await;
                            return Some(false)
                        }
                    }
                },
                Err(_) => {
                    // Close the underlying TCP socket
                    let _ = link.socket.shutdown(Shutdown::Both);
                    // Delete the link from the manager
                    let _ = link.manager.del_link(&link.src_addr, &link.dst_addr).await;
                    // Notify the transport
                    guard.link_err(&link_obj).await;
                    return Some(false)
                }
            }
        }
    }

    // Execute the read loop 
    let stop = link.ch_recv.recv();
    let read_loop = read_loop(&link);
    let _ = read_loop.race(stop).await;
}


impl Drop for Tcp {
    fn drop(&mut self) {
        // Close the underlying TCP socket
        let _ = self.socket.shutdown(Shutdown::Both);
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
            Err(e) => return Err(zerror!(ZErrorKind::Other{
                    descr: format!("{}", e)
                }))
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
            None => Err(zerror!(ZErrorKind::Other{
                    descr: format!("No active TCP link ({} => {})", src, dst)
                }))
        }
    }

    async fn get_link(&self, src: &SocketAddr, dst: &SocketAddr) -> ZResult<Arc<Tcp>> {
        // Remove the link from the manager list
        match zasyncwrite!(self.link).get(&(*src, *dst)) {
            Some(link) => Ok(link.clone()),
            None => Err(zerror!(ZErrorKind::Other{
                descr: format!("No active TCP link ({} => {})", src, dst)
            }))
        }
    }

    async fn new_listener(&self, a_self: &Arc<Self>, addr: &SocketAddr) -> ZResult<()> {
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

    async fn del_listener(&self, _a_self: &Arc<Self>, addr: &SocketAddr) -> ZResult<()> {
        // Stop the listener
        match zasyncwrite!(self.listener).remove(&addr) {
            Some(listener) => {
                listener.sender.send(false).await;
                Ok(())
            },
            None => Err(zerror!(ZErrorKind::Other{
                descr: format!("No TCP listener on address: {}", addr)
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
            let initial = a_self.inner.get_initial_transport().await;
            // Create the new link object
            let link = Arc::new(Tcp::new(stream, initial, a_self.clone()));
            link.initizalize(Arc::downgrade(&link));

            // Store a reference to the link into the manger
            zasyncwrite!(a_self.link).insert((link.src_addr, link.dst_addr), link.clone());

            // // Store a reference to the link into the session
            // let link_obj: Link = link.clone();
            // if initial.add_link(link_obj).await.is_err() {
            //     continue
            // }

            // Spawn the receive loop for the new link
            let _ = link.start().await;
        }
    }

    let stop = listener.receiver.recv();
    let accept_loop = accept_loop(&a_self, &listener);
    let _ = accept_loop.race(stop).await;
}