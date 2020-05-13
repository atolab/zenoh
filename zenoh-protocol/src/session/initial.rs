
use async_std::sync::{Arc, RwLock, Sender};
use async_trait::async_trait;
use rand::Rng;
use std::collections::HashMap;
use std::fmt;

use crate::core::{PeerId, ZError, ZErrorKind, ZInt, ZResult};
use crate::io::WBuf;
use crate::link::{Link, Locator};
use crate::proto::{Attachment, SessionMessage, WhatAmI, smsg};
use crate::session::defaults::SESSION_SEQ_NUM_RESOLUTION;
use crate::session::{Action, Channel, Session, SessionManagerInner, TransportTrait};
use crate::zerror;
use zenoh_util::{zasyncread, zasyncwrite};


const DEFAULT_WBUF_CAPACITY: usize = 64;

// Macro to send a message on a link
macro_rules! zsend {
    ($msg:expr, $link:expr) => ({
        // Serialize the message
        let mut buffer = WBuf::new(DEFAULT_WBUF_CAPACITY);
        buffer.write_message(&$msg);
        let buffer = buffer.as_rbuf();

        // Send the message on the link
        $link.send(buffer).await
    });
}

struct PendingOpen {
    lease: ZInt,
    initial_sn: ZInt,
    sn_resolution: ZInt,
    notify: Sender<ZResult<Session>>
}

impl PendingOpen {
    fn new(
        lease: ZInt,         
        sn_resolution: ZInt, 
        notify: Sender<ZResult<Session>>
    ) -> PendingOpen {
        let mut rng = rand::thread_rng();
        PendingOpen {
            lease,
            initial_sn: rng.gen_range(0, sn_resolution),
            sn_resolution,
            notify
        }
    }
}

pub(super) struct InitialSession {
    manager: Arc<SessionManagerInner>,
    pending: RwLock<HashMap<Link, PendingOpen>>
}

impl InitialSession {
    pub(super) fn new(manager: Arc<SessionManagerInner>) -> InitialSession {
        InitialSession {
            manager,
            pending: RwLock::new(HashMap::new())
        }
    }

    /*************************************/
    /*            OPEN/CLOSE             */
    /*************************************/
    pub(super) async fn open(&self, 
        link: &Link, 
        attachment: &Option<Attachment>,
        notify: &Sender<ZResult<Session>>
    ) -> ZResult<()> {
        let pending = PendingOpen::new(
            self.manager.config.lease, 
            self.manager.config.sn_resolution,
            notify.clone()
        );

        // Build the fields for the Open Message
        let version = self.manager.config.version;
        let whatami = self.manager.config.whatami;
        let pid = self.manager.config.pid.clone();
        let lease = pending.lease;
        let initial_sn = pending.initial_sn;
        let sn_resolution = if pending.sn_resolution == *SESSION_SEQ_NUM_RESOLUTION {
            None
        } else {
            Some(pending.sn_resolution)
        };
        let locators = self.manager.get_locators().await;
        let locators = match locators.len() {
            0 => None,
            _ => Some(locators),
        };        
        
        // Build the Open Message
        let message = SessionMessage::make_open(
            version, 
            whatami, 
            pid, 
            lease,
            initial_sn,
            sn_resolution,
            locators, 
            attachment.clone()
        );

        // Store the pending  for the callback to be used in the process_message
        let key = link.clone();
        zasyncwrite!(self.pending).insert(key, pending);

        // Send the message on the link
        zsend!(message, link)?;

        Ok(())
    }

    /*************************************/
    /*          PROCESS MESSAGES         */
    /*************************************/
    async fn process_accept(
        &self,
        link: &Link,
        whatami: &WhatAmI,
        opid: &PeerId,
        apid: &PeerId,
        initial_sn: ZInt,
        sn_resolution: Option<ZInt>,
        lease: Option<ZInt>,
        _locators: Option<Vec<Locator>>
    ) -> Action {
        // @TODO: handle the locators

        // Check if we had previously triggered the opening of a new connection
        let res = zasyncwrite!(self.pending).remove(link);
        if let Some(pending) = res {
            // Check if the opener peer of this accept was me
            if opid != &self.manager.config.pid {
                let s = "!!! Received an Accept with invalid Opener Peer Id";
                println!("{}", s);

                // Invalid value, send a Close message
                let peer_id = Some(self.manager.config.pid.clone());
                let reason_id = smsg::close_reason::INVALID;              
                let link_only = false;  // This is should always be true for invalid opener ID                
                let attachment = None;  // No attachment here
                let message = SessionMessage::make_close(peer_id, reason_id, link_only, attachment);

                // Send the message on the link
                let _ = zsend!(message, link);

                // Notify
                let err = Err(zerror!(ZErrorKind::InvalidMessage { descr: s.to_string() }));
                pending.notify.send(err).await;
                return Action::Close
            }

            // Get the agreed lease
            let lease = if let Some(l) = lease {
                if l <= pending.lease {
                    l
                } else {
                    let s = "!!! Received an Accept with invalid Lease";
                    println!("{}", s);

                    // Invalid value, send a Close message
                    let peer_id = Some(self.manager.config.pid.clone());
                    let reason_id = smsg::close_reason::INVALID;              
                    let link_only = false;  // This is should always be true for invalid lease                
                    let attachment = None;  // No attachment here
                    let message = SessionMessage::make_close(peer_id, reason_id, link_only, attachment);

                    // Send the message on the link
                    let _ = zsend!(message, link);

                    // Notify
                    let err = Err(zerror!(ZErrorKind::InvalidMessage { descr: s.to_string() }));
                    pending.notify.send(err).await;
                    return Action::Close
                }
            } else {
                pending.lease
            };

            // Get the agreed SN Resolution
            let sn_resolution = if let Some(r) = sn_resolution {
                if r <= pending.sn_resolution {
                    r
                } else {
                    let s = "!!! Received an Accept with invalid SN Resolution";
                    println!("{}", s);

                    // Invalid value, send a Close message
                    let peer_id = Some(self.manager.config.pid.clone());
                    let reason_id = smsg::close_reason::INVALID;              
                    let link_only = false;  // This is should always be true for invalid sn resolution                
                    let attachment = None;  // No attachment here
                    let message = SessionMessage::make_close(peer_id, reason_id, link_only, attachment);

                    // Send the message on the link
                    let _ = zsend!(message, link);

                    // Notify
                    let err = Err(zerror!(ZErrorKind::InvalidMessage { descr: s.to_string() }));
                    pending.notify.send(err).await;
                    return Action::Close
                }
            } else {
                pending.sn_resolution
            };

            // Get the agreed Initial SN for reception
            let initial_sn_rx = if initial_sn < sn_resolution {
                initial_sn
            } else {
                let s = "!!! Received an Accept with invalid Initial SN";
                println!("{}", s);

                // Invalid value, send a Close message
                let peer_id = Some(self.manager.config.pid.clone());
                let reason_id = smsg::close_reason::INVALID;              
                let link_only = false;  // This is should always be true for invalid sn resolution                
                let attachment = None;  // No attachment here
                let message = SessionMessage::make_close(peer_id, reason_id, link_only, attachment);

                // Send the message on the link
                let _ = zsend!(message, link);

                // Notify
                let err = Err(zerror!(ZErrorKind::InvalidMessage { descr: s.to_string() }));
                pending.notify.send(err).await;
                return Action::Close
            };

            // Get the agreed Initial SN for transmission
            let initial_sn_tx = if pending.initial_sn < sn_resolution {
                pending.initial_sn
            } else {
                pending.initial_sn % sn_resolution
            };

            // Get a new or an existing session
            // NOTE: In case of exsisting session, all the parameters in the accept are ignored
            let session = self.manager.get_or_new_session(
                &self.manager, apid, whatami, lease, sn_resolution, initial_sn_tx, initial_sn_rx
            ).await;

            // Add this link to the session
            let res = session.add_link(link.clone()).await;
            if let Err(e) = res {
                // Invalid value, send a Close message
                let peer_id = Some(self.manager.config.pid.clone());
                let reason_id = smsg::close_reason::INVALID;              
                let link_only = false;  // This is should always be true for invalid sn resolution                
                let attachment = None;  // No attachment here
                let message = SessionMessage::make_close(peer_id, reason_id, link_only, attachment);

                // Send the message on the link
                let _ = zsend!(message, link);

                // Notify
                pending.notify.send(Err(e)).await;
                return Action::Close
            }

            // Set the callback on the session if needed
            match session.has_callback() {
                Ok(has_callback) => {
                    if !has_callback {
                        // Notify the session handler that there is a new session and get back a callback
                        let callback = self.manager.config.handler.new_session(
                            self.manager.config.whatami, Arc::new(session.clone())
                        ).await;
                        // Set the callback on the transport
                        let res = session.set_callback(callback).await;
                        if let Err(e) = res {
                            // Notify
                            pending.notify.send(Err(e)).await;
                            return Action::Close
                        }
                    }
                },
                Err(e) => {
                    // Notify
                    pending.notify.send(Err(e)).await;
                    return Action::Close
                }
            }

            // Return the target transport to use in the link
            match session.get_transport() {
                Ok(transport) => {
                    // Notify
                    pending.notify.send(Ok(session)).await;
                    Action::ChangeTransport(transport)
                }
                Err(e) => {
                    // Notify
                    pending.notify.send(Err(e)).await;
                    Action::Close
                }
            }            
        } else { 
            println!("!!! Received an unsolicited Accept because no Open message was sent");
            Action::Read
        }
    }

    async fn process_close(&self, link: &Link, pid: &Option<PeerId>, _reason: u8) -> Action {
        // Check if the close target is me
        // if !self.is_initial && pid != &Some(self.peer.clone()) {
        //     println!("!!! PeerId mismatch on Close message");
        //     return Action::Read
        // }

        // // Delete the link
        // let _ = self.del_link(link).await;

        Action::Close
    }

    #[allow(clippy::too_many_arguments)]
    async fn process_open(
        &self,
        link: &Link,
        version: u8,
        whatami: WhatAmI,
        pid: &PeerId,
        lease: ZInt,
        initial_sn: ZInt,
        sn_resolution: Option<ZInt>,        
        locators: Option<Vec<Locator>>,
    ) -> Action {
        // @TODO: Manage locators

        // Check if the version is supported
        // if version > self.manager.config.version {
        //     // Send a close message
        //     let peer_id = Some(self.manager.config.pid.clone());
        //     let reason_id = smsg::close_reason::UNSUPPORTED;              
        //     let link_only = false;  // This is should always be false for invalid version                
        //     let attachment = None;  // Parameter of open_session
        //     let message = SessionMessage::make_close(peer_id, reason_id, link_only, attachment);
        // 
        //     // Send the message on the link
        //     let _ = zsend!(message, link);

        //     // Close the link
        // //     return Action::Close
        // }

        // // Check if an already established session exists with the peer
        // let target = self.manager.get_session(pid).await;

        // // Check if this open is related to a totally new session (i.e. new peer)
        // if target.is_err() {
        //     // Check if a limit for the maximum number of open sessions is set
        //     if let Some(limit) = self.manager.config.max_sessions {
        //         let num = self.manager.get_sessions().await.len();
        //         // Check if we have reached the session limit
        //         if num >= limit {
        //             // Send a close message
        //             let peer_id = Some(self.manager.config.pid.clone());
        //             let reason_id = smsg::close_reason::MAX_SESSIONS;                
        //             let link_only = false;  // This is should always be false when the session limit is reached                
        //             let attachment = None;  // Parameter of open_session
        //             let message = SessionMessage::make_close(peer_id, reason_id, link_only, attachment);

        //              // Send the message on the link
        //              let _ = zsend!(message, link);

        //             // Close the link
        //             return Action::Close
        //         }
        //     }
        // }

        // // Get the session associated to the peer
        // let target = self.manager.get_or_new_session(&self.manager, pid, whatami).await;

        // // Check if a limit for the maximum number of links associated to a session is set
        // if let Some(limit) = self.manager.config.max_links {
        //     // Check if we have reached the session limit
        //     if let Ok(links) = target.get_links().await{
        //         if links.len() >= limit {
        //             // Send a close message
        //             let peer_id = Some(self.manager.config.pid.clone());
        //             let reason_id = smsg::close_reason::MAX_LINKS;               
        //             let link_only = true;  // This is should always be true when the link limit is reached                
        //             let attachment = None;  // Parameter of open_session
        //             let message = SessionMessage::make_close(peer_id, reason_id, link_only, attachment);

        //              // Send the message on the link
        //              let _ = zsend!(message, link);

        //             // Close the link
        //             return Action::Close
        //         }
        //     }
        // }

        // // Set the lease to the transport
        // target.set_lease(lease);

        // // Add the link to the target
        // let res = target.add_link(link.clone()).await;
        // if res.is_err() {
        //     return Action::Close
        // }

        // // Build Accept message
        // let conduit_id = None; // Conduit ID always None
        // let properties = None; // Properties always None for the time being. May change in the future.
        // let message = SessionMessage::make_accept(
        //     self.manager.config.whatami,
        //     pid.clone(),
        //     self.manager.config.pid.clone(),
        //     self.manager.config.lease,
        //     conduit_id,
        //     properties,
        // );

        // // Send the message for transmission
        // let res = target.transport.send(message, *QUEUE_PRIO_CTRL, Some(link.clone())).await;

        // if res.is_ok() {
        //     if !target.transport.has_callback() {
        //         // Notify the session handler that there is a new session and get back a callback
        //         // NOTE: the read loop of the link the open message was sent on remains blocked
        //         //       until the new_session() returns. The read_loop in the various links
        //         //       waits for any eventual transport to associate to. This is transport is
        //         //       returned only by the process_open() -- this function.
        //         let callback = self.manager.config.handler.new_session(
        //             whatami.clone(), 
        //             Arc::new(Session::new(Arc::downgrade(&target)))
        //         ).await;
        //         // Set the callback on the transport
        //         target.transport.set_callback(callback).await;
        //     }
        // } else {
        //     return Action::Close
        // }

        // // Return the target transport to use in the link
        // Action::ChangeTransport(target.transport.clone())
        Action::Read
    }
}

#[async_trait]
impl TransportTrait for InitialSession {
    async fn receive_message(&self, link: &Link, message: SessionMessage) -> Action {
        // let conduit = self.get_or_new_conduit(message.cid).await;
        // // Process the message
        // conduit.receive_message(link, message).await
        Action::Read
    }

    async fn link_err(&self, link: &Link) {

    }
}