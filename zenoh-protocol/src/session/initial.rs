
use async_std::sync::{Arc, RwLock, Sender, Weak};
use async_trait::async_trait;
use rand::Rng;
use std::collections::HashMap;
use std::fmt;

use crate::core::{PeerId, ZError, ZErrorKind, ZInt, ZResult};
use crate::io::WBuf;
use crate::link::{Link, Locator};
use crate::proto::{Attachment, SessionMessage, WhatAmI, smsg};
use crate::session::defaults::{QUEUE_PRIO_CTRL, QUEUE_PRIO_DATA};
use crate::session::{Action, Channel, Session, SessionManagerInner, Transport};
use crate::zerror;
use zenoh_util::{zasyncread, zasyncwrite};


struct PendingOpen {
    lease: ZInt,
    initial_sn: ZInt,
    sn_resolution: ZInt,
    notify: Sender<ZResult<Weak<Channel>>>
}

impl PendingOpen {
    fn new(lease: ZInt, sn_resolution: ZInt, notify: Sender<ZResult<Weak<Channel>>>) -> PendingOpen {
        let mut rng = rand::thread_rng();
        PendingOpen {
            lease,
            initial_sn: rng.gen_range(0, sn_resolution),
            sn_resolution,
            notify
        }
    }
}

pub(crate) struct InitialSession {
    manager: Arc<SessionManagerInner>,
    pending: RwLock<HashMap<(Locator, Locator), PendingOpen>>
}

impl InitialSession {
    pub(crate) fn new(manager: Arc<SessionManagerInner>) -> InitialSession {
        InitialSession {
            manager,
            pending: RwLock::new(HashMap::new())
        }
    }

    /*************************************/
    /*            OPEN/CLOSE             */
    /*************************************/
    async fn open(&self, 
        link: Link, 
        attachment: Option<Attachment>,
        notify: Sender<ZResult<Weak<Channel>>>
    ) -> ZResult<()> {
        let pending = PendingOpen::new(
            self.manager.config.lease, 
            self.manager.config.sn_resolution,
            notify
        );

        // Store the sender for the callback to be used in the process_message
        let key = (link.get_src(), link.get_dst());
        zasyncwrite!(self.pending).insert(key, pending);

        // Build the fields for the Open Message
        let locators = self.manager.get_locators().await;
        let locators = match locators.len() {
            0 => None,
            _ => Some(locators),
        };
        // Attachment of open_session
        let properties = None;

        // Build the Open Message
        let message = SessionMessage::make_open(
            self.manager.config.version, 
            self.manager.config.whatami, 
            self.manager.config.pid, 
            pending.lease,
            pending.initial_sn,
            pending.sn_resolution,
            locators, 
            attachment
        );

        // Serialize the message
        let mut buffer = WBuf::new();
        buffer.write_message(message);
        let buffer = buffer.as_rbuf();

        // Schedule the message for transmission
        link.send(buffer).await?;

        Ok(())
    }

    /*************************************/
    /*          PROCESS MESSAGES         */
    /*************************************/
    pub(crate) async fn process_accept(
        &self,
        link: &Link,
        whatami: &WhatAmI,
        opid: &PeerId,
        apid: &PeerId,
        lease: ZInt
    ) -> Action {
        // Check if the opener peer of this accept was me
        if opid != &self.manager.config.id {
            println!("!!! Received an Accept with wrong Opener Peer Id");
            return Action::Read
        }

        // Check if had previously triggered the opening of a new connection
        let key = (link.get_src(), link.get_dst());
        let res = zasyncwrite!(self.channels).remove(&key);
        if let Some(sender) = res {
            // Remove the link from self
            let res = self.transport.del_link(link).await;
            if res.is_err() {
                return Action::Read
            }
            // Get a new or an existing session
            let weak_session = self.manager.get_or_new_session(&self.manager, apid, whatami).await;
            // Upgrade from Weak to Arc
            let arc_session = if let Some(session) = weak_session.upgrade() {
                session
            } else {
                // The session has been closed
                return Action::Close
            };
            // Configure the lease time on the transport
            arc_session.transport.set_lease(lease);
            // Add the link on the transport
            let res = arc_session.transport.add_link(link.clone()).await;
            if res.is_err() {
                return Action::Read
            }
            // Set the callback to the transport if needed
            if !arc_session.transport.has_callback() {
                // Notify the session handler that there is a new session and get back a callback
                let callback = self.manager.config.handler
                    .new_session(self.whatami.clone(), 
                    Arc::new(Session::new(Arc::downgrade(&arc_session)))
                ).await;
                // Set the callback on the transport
                arc_session.transport.set_callback(callback).await;
            }
            // Notify the opener
            sender.send(Ok(weak_session)).await;
            // Return the target transport to use in the link
            Action::ChangeTransport(arc_session.transport.clone())
        } else { 
            println!("!!! Received an unsolicited Accept because no Open message was sent");
            Action::Read
        }
    }

    pub(crate) async fn process_close(&self, link: &Link, pid: &Option<PeerId>, _reason: u8) -> Action {
        // Check if the close target is me
        if !self.is_initial && pid != &Some(self.peer.clone()) {
            println!("!!! PeerId mismatch on Close message");
            return Action::Read
        }

        // Delete the link
        let _ = self.del_link(link).await;

        Action::Close
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn process_open(
        &self,
        link: &Link,
        version: u8,
        whatami: &WhatAmI,
        pid: &PeerId,
        lease: ZInt,
        _locators: &Option<Vec<Locator>>,
    ) -> Action {
        // @TODO: Manage locators

        // Check if the version is supported
        if version > self.manager.config.version {
            // Send a close message
            let peer_id = Some(self.manager.config.id.clone());
            let reason_id = smsg::close_reason::UNSUPPORTED;              
            let conduit_id = None;  // This is should always be None for Close Messages                
            let properties = None;  // Parameter of open_session
            let message = SessionMessage::make_close(peer_id, reason_id, conduit_id, properties);

            // Send the close message for this link
            let _ = self.transport.send(message, *QUEUE_PRIO_CTRL, Some(link.clone())).await;

            // Close the link
            return Action::Close
        }

        // Check if an already established session exists with the peer
        let target = self.manager.get_session(pid).await;

        // Check if this open is related to a totally new session (i.e. new peer)
        if target.is_err() {
            // Check if a limit for the maximum number of open sessions is set
            if let Some(limit) = self.manager.config.max_sessions {
                let num = self.manager.get_sessions().await.len();
                // Check if we have reached the session limit
                if num >= limit {
                    // Send a close message
                    let peer_id = Some(self.manager.config.id.clone());
                    let reason_id = smsg::close_reason::MAX_SESSIONS;                
                    let conduit_id = None;  // This is should always be None for Close Messages                
                    let properties = None;  // Parameter of open_session
                    let message = SessionMessage::make_close(peer_id, reason_id, conduit_id, properties);

                    // Send the close message for this link
                    let _ = self.transport.send(message, *QUEUE_PRIO_CTRL, Some(link.clone())).await;

                    // Close the link
                    return Action::Close
                }
            }
        }

        // Get the session associated to the peer
        let target = self.manager.get_or_new_session(&self.manager, pid, whatami).await;
        let target = if let Some(session) = target.upgrade() {
            session
        } else {
            return Action::Close
        };

        // Check if a limit for the maximum number of links associated to a session is set
        if let Some(limit) = self.manager.config.max_links {
            // Check if we have reached the session limit
            if target.transport.num_links() >= limit {
                // Send a close message
                let peer_id = Some(self.manager.config.id.clone());
                let reason_id = smsg::close_reason::MAX_LINKS;               
                let conduit_id = None;  // This is should always be None for Close Messages                
                let properties = None;  // Parameter of open_session
                let message = SessionMessage::make_close(peer_id, reason_id, conduit_id, properties);

                // Send the close message for this link
                let _ = self.transport.send(message, *QUEUE_PRIO_CTRL, Some(link.clone())).await;

                // Close the link
                return Action::Close
            }
        }

        // Set the lease to the transport
        target.transport.set_lease(lease);

        // Remove the link from self
        let res = self.transport.del_link(&link).await;
        if res.is_err() {
            return Action::Close
        }
        // Add the link to the target
        let res = target.transport.add_link(link.clone()).await;
        if res.is_err() {
            return Action::Close
        }

        // Build Accept message
        let conduit_id = None; // Conduit ID always None
        let properties = None; // Properties always None for the time being. May change in the future.
        let message = SessionMessage::make_accept(
            self.manager.config.whatami.clone(),
            pid.clone(),
            self.manager.config.id.clone(),
            self.manager.config.lease,
            conduit_id,
            properties,
        );

        // Send the message for transmission
        let res = target.transport.send(message, *QUEUE_PRIO_CTRL, Some(link.clone())).await;

        if res.is_ok() {
            if !target.transport.has_callback() {
                // Notify the session handler that there is a new session and get back a callback
                // NOTE: the read loop of the link the open message was sent on remains blocked
                //       until the new_session() returns. The read_loop in the various links
                //       waits for any eventual transport to associate to. This is transport is
                //       returned only by the process_open() -- this function.
                let callback = self.manager.config.handler.new_session(
                    whatami.clone(), 
                    Arc::new(Session::new(Arc::downgrade(&target)))
                ).await;
                // Set the callback on the transport
                target.transport.set_callback(callback).await;
            }
        } else {
            return Action::Close
        }

        // Return the target transport to use in the link
        Action::ChangeTransport(target.transport.clone())
    }
}

#[async_trait]
impl Transport for InitialSession {
    async fn receive_message(&self, link: &Link, message: SessionMessage) -> Action {
        // let conduit = self.get_or_new_conduit(message.cid).await;
        // // Process the message
        // conduit.receive_message(link, message).await
        Action::Read
    }

    async fn link_err(&self, link: &Link) {

    }
}