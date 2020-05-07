use crate::io::RBuf;
use crate::core::{ZError, ZErrorKind, ZResult, PeerId, Property, ResKey, TimeStamp, NO_RESOURCE_ID};
use crate::link::Locator;
use crate::zerror;

use super::msg::*;
use super::decl::{Declaration, SubInfo, SubMode, Reliability, Period};
use std::sync::Arc;

impl RBuf {

    pub fn read_message(&mut self, with_length: bool) -> ZResult<SessionMessage> {
        self.read_smsg()
    }

    pub fn read_smsg(&mut self) -> ZResult<SessionMessage> {
        use super::smsg::id::*;

        let mut has_attachment = false;
        loop {
            let header = self.read()?;
            match smsg::mid(header) {
                ATTACHMENT => {
                    has_attachment = true;
                    continue;
                },

                SCOUT => {
                    let what = if smsg::has_flag(header, smsg::flag::W) {
                        Some(self.read_zint()?)
                    } else { 
                        None 
                    };
                    let attachment = if has_attachment {
                        Some(self.read_attachment()?)
                    } else {
                        None
                    }; 
                    return Ok(SessionMessage::make_scout(what, attachment));
                },

                HELLO => {
                    let whatami = if smsg::has_flag(header, smsg::flag::W) {
                        Some(self.read_zint()?)
                    } else { 
                        Some(whatami::BROKER)
                    };
                    let locators = if smsg::has_flag(header, smsg::flag::L) {
                        Some(self.read_locators()?)
                    } else { 
                        None 
                    };
                    let attachment = if has_attachment {
                        Some(self.read_attachment()?)
                    } else {
                        None
                    }; 
                    return Ok(SessionMessage::make_hello(whatami, locators, attachment));
                },

                OPEN => {
                    let version = self.read()?;
                    let whatami = self.read_zint()?;
                    let pid = self.read_peerid()?;
                    let lease = self.read_zint()?;
                    let initial_sn = self.read_zint()?;
                    let (sn_resolution, locators) = if smsg::has_flag(header, smsg::flag::O) {
                        let options = self.read()?;
                        let sn_resolution = if smsg::has_flag(options, smsg::flag::S) {
                            Some(self.read_zint()?)
                        } else {
                            None
                        };
                        let locators = if smsg::has_flag(options, smsg::flag::L) {
                            Some(self.read_locators()?)
                        } else { 
                            None 
                        };
                        (sn_resolution, locators)
                    } else {
                        (None, None)
                    };
                    let attachment = if has_attachment {
                        Some(self.read_attachment()?)
                    } else {
                        None
                    }; 
                    return Ok(SessionMessage::make_open(version, whatami, pid, lease, initial_sn, sn_resolution, locators, attachment));
                },

                ACCEPT => {
                    let whatami = self.read_zint()?;
                    let opid = self.read_peerid()?;
                    let apid = self.read_peerid()?;
                    let initial_sn = self.read_zint()?;
                    let (sn_resolution, lease, locators) = if smsg::has_flag(header, smsg::flag::O) {
                        let options = self.read()?;
                        let sn_resolution = if smsg::has_flag(options, smsg::flag::S) {
                            Some(self.read_zint()?)
                        } else {
                            None
                        };
                        let lease = if smsg::has_flag(options, smsg::flag::D) {
                            Some(self.read_zint()?)
                        } else {
                            None
                        };
                        let locators = if smsg::has_flag(options, smsg::flag::L) {
                            Some(self.read_locators()?)
                        } else { 
                            None 
                        };
                        (sn_resolution, lease, locators)
                    } else {
                        (None, None, None)
                    };
                    let attachment = if has_attachment {
                        Some(self.read_attachment()?)
                    } else {
                        None
                    }; 
                    return Ok(SessionMessage::make_accept(whatami, opid, apid, initial_sn, sn_resolution, lease, locators, attachment));
                },

                CLOSE => {
                    let link_only = smsg::has_flag(header, smsg::flag::K);
                    let pid = if smsg::has_flag(header, smsg::flag::I) {
                        Some(self.read_peerid()?)
                    } else { 
                        None 
                    };
                    let reason = self.read()?;
                    let attachment = if has_attachment {
                        Some(self.read_attachment()?)
                    } else {
                        None
                    }; 
                    return Ok(SessionMessage::make_close(pid, reason, link_only, attachment));
                },

                SYNC => {
                    let reliable = smsg::has_flag(header, smsg::flag::R);
                    let sn = self.read_zint()?;
                    let count = if smsg::has_flag(header, smsg::flag::C) {
                        Some(self.read_zint()?)
                    } else { 
                        None 
                    };
                    let attachment = if has_attachment {
                        Some(self.read_attachment()?)
                    } else {
                        None
                    }; 
                    return Ok(SessionMessage::make_sync(reliable, sn, count, attachment))
                },

                ACK_NACK => {
                    let sn = self.read_zint()?;
                    let mask = if smsg::has_flag(header, smsg::flag::M) {
                        Some(self.read_zint()?)
                    } else { 
                        None 
                    };
                    let attachment = if has_attachment {
                        Some(self.read_attachment()?)
                    } else {
                        None
                    };
                    return Ok(SessionMessage::make_ack_nack(sn, mask, attachment))
                },

                KEEP_ALIVE => {
                    let pid = if smsg::has_flag(header, smsg::flag::I) {
                        Some(self.read_peerid()?)
                    } else { 
                        None 
                    };
                    let attachment = if has_attachment {
                        Some(self.read_attachment()?)
                    } else {
                        None
                    };
                    return Ok(SessionMessage::make_keep_alive(pid, attachment));
                },

                PING_PONG => {
                    let hash = self.read_zint()?;
                    let attachment = if has_attachment {
                        Some(self.read_attachment()?)
                    } else {
                        None
                    };
                    if smsg::has_flag(header, smsg::flag::P) {
                        return Ok(SessionMessage::make_ping(hash, attachment))
                    } else {
                        return Ok(SessionMessage::make_pong(hash, attachment))
                    }
                },

                id => return Err(zerror!(ZErrorKind::InvalidMessage {
                    descr: format!("ID unknown: {}", id)
                }))
            }
        }
    }

    pub fn read_zmsg(&mut self) -> ZResult<ZenohMessage> {
        use super::zmsg::id::*;

        let mut reply_context = None;
        let mut has_attachment = false;
        loop {
            let header = self.read()?;
            match zmsg::mid(header) {
                REPLY => {
                    reply_context = Some(self.read_deco_reply(header)?);
                    continue;
                },

                ATTACHMENT => {
                    has_attachment = true;
                    continue;
                },

                DECLARE => {
                    let declarations = self.read_declarations()?;
                    let attachment = if has_attachment {
                        Some(self.read_attachment()?)
                    } else {
                        None
                    };
                    return Ok(ZenohMessage::make_declare(declarations, attachment));
                },

                DATA => {
                    let reliable = zmsg::has_flag(header, zmsg::flag::R);
                    let key = self.read_reskey(zmsg::has_flag(header, zmsg::flag::K))?;
                    let info = if zmsg::has_flag(header, zmsg::flag::I) {
                        Some(RBuf::from(self.read_bytes_array()?))
                    } else { 
                        None 
                    };
                    let payload = RBuf::from(self.read_bytes_array()?);
                    let attachment = if has_attachment {
                        Some(self.read_attachment()?)
                    } else {
                        None
                    };
                    return Ok(ZenohMessage::make_data(reliable, key, info, payload, reply_context, attachment))
                },

                UNIT => {
                    let reliable = zmsg::has_flag(header, zmsg::flag::R);
                    let attachment = if has_attachment {
                        Some(self.read_attachment()?)
                    } else {
                        None
                    };
                    return Ok(ZenohMessage::make_unit(reliable, reply_context, attachment))
                }

                PULL => {
                    let is_final = zmsg::has_flag(header, zmsg::flag::F);
                    let key = self.read_reskey(zmsg::has_flag(header, zmsg::flag::K))?;
                    let pull_id = self.read_zint()?;
                    let max_samples = if zmsg::has_flag(header, zmsg::flag::N) {
                        Some(self.read_zint()?)
                    } else { 
                        None 
                    };
                    let attachment = if has_attachment {
                        Some(self.read_attachment()?)
                    } else {
                        None
                    };
                    return Ok(ZenohMessage::make_pull(is_final, key, pull_id, max_samples, attachment))
                },

                QUERY => {
                    let key = self.read_reskey(zmsg::has_flag(header, zmsg::flag::K))?;
                    let predicate = self.read_string()?;
                    let qid = self.read_zint()?;
                    let target = if zmsg::has_flag(header, zmsg::flag::T) {
                        Some(self.read_query_target()?)
                    } else { 
                        None 
                    };
                    let consolidation = self.read_consolidation()?;
                    let attachment = if has_attachment {
                        Some(self.read_attachment()?)
                    } else {
                        None
                    };
                    return Ok(ZenohMessage::make_query(key, predicate, qid, target, consolidation, attachment))
                }

                id => return Err(zerror!(ZErrorKind::InvalidMessage {
                    descr: format!("ID unknown: {}", id)
                }))
            }
        }
    }

    pub fn read_attachment(&mut self) -> ZResult<Arc<RBuf>> {
        // @TODO
        Ok(Arc::new(RBuf::new()))
    }

    pub fn read_datainfo(&mut self) -> ZResult<DataInfo> {
        let header = self.read()?;
        let source_id = if header & zmsg::info_flag::SRCID > 0 {
            Some(self.read_peerid()?)
        } else { None };
        let source_sn = if header & zmsg::info_flag::SRCID > 0 {
            Some(self.read_zint()?)
        } else { None };
        let fist_broker_id = if header & zmsg::info_flag::SRCID > 0 {
            Some(self.read_peerid()?)
        } else { None };
        let fist_broker_sn = if header & zmsg::info_flag::SRCID > 0 {
            Some(self.read_zint()?)
        } else { None };
        let timestamp = if header & zmsg::info_flag::SRCID > 0 {
            Some(self.read_timestamp()?)
        } else { None };
        let kind = if header & zmsg::info_flag::SRCID > 0 {
            Some(self.read_zint()?)
        } else { None };
        let encoding = if header & zmsg::info_flag::SRCID > 0 {
            Some(self.read_zint()?)
        } else { None };

        Ok(DataInfo { header, source_id, source_sn, fist_broker_id, fist_broker_sn, timestamp, kind, encoding })
    }

    // @TODO: Update the Reply format
    fn read_deco_reply(&mut self, header: u8) -> ZResult<ReplyContext> {
        let is_final = zmsg::has_flag(header, zmsg::flag::F);
        let source = if zmsg::has_flag(header, zmsg::flag::E) { ReplySource::Eval } else { ReplySource::Storage };
        let qid = self.read_zint()?;
        let replier_id = if is_final { None } else {
            Some(self.read_peerid()?)
        };
        Ok(ReplyContext{ is_final, qid, source, replier_id })
    }

    fn read_property(&mut self) -> ZResult<Property> {
        let key = self.read_zint()?;
        let value = self.read_bytes_array()?;
        Ok(Property{ key, value })
    }

    fn read_locators(&mut self) -> ZResult<Vec<Locator>> {
        let len = self.read_zint()?;
        let mut vec: Vec<Locator> = Vec::new();
        for _ in 0..len {
            vec.push(self.read_string()?.parse()?);
        }
        Ok(vec)
    }

    fn read_declarations(&mut self) -> ZResult<Vec<Declaration>> {
        let len = self.read_zint()?;
        let mut vec: Vec<Declaration> = Vec::new();
        for _ in 0..len {
            vec.push(self.read_declaration()?);
        }
        Ok(vec)
    }

    fn read_declaration(&mut self) -> ZResult<Declaration> {
        use super::decl::{Declaration::*, id::*};

        macro_rules! read_key_delc {
            ($buf:ident, $header:ident, $type:ident) => {{
                Ok($type{ 
                    key: $buf.read_reskey(zmsg::has_flag($header, zmsg::flag::K))?
                })
            }}
        }

        let header = self.read()?;
        match zmsg::mid(header) {
            RESOURCE => {
                let rid = self.read_zint()?;
                let key = self.read_reskey(zmsg::has_flag(header, zmsg::flag::K))?;
                Ok(Resource{ rid, key })
            }

            FORGET_RESOURCE => {
                let rid = self.read_zint()?;
                Ok(ForgetResource{ rid })
            }

            SUBSCRIBER => {
                let reliability = if zmsg::has_flag(header, zmsg::flag::R) {
                    Reliability::Reliable 
                } else {
                    Reliability::BestEffort
                };
                let key = self.read_reskey(zmsg::has_flag(header, zmsg::flag::K))?;
                let (mode, period) = if zmsg::has_flag(header, zmsg::flag::S) {
                    self.read_submode()?
                } else {
                    (SubMode::Push, None)
                };
                Ok(Subscriber{ key, info: SubInfo { reliability, mode, period } })
            }

            FORGET_SUBSCRIBER => read_key_delc!(self, header, ForgetSubscriber),
            PUBLISHER => read_key_delc!(self, header, Publisher),
            FORGET_PUBLISHER => read_key_delc!(self, header, ForgetPublisher),
            QUERYABLE => read_key_delc!(self, header, Queryable),
            FORGET_QUERYABLE => read_key_delc!(self, header, ForgetQueryable),

            id => panic!("UNEXPECTED ID FOR Declaration: {}", id)   //@TODO: return error
        }
    }

    fn read_submode(&mut self) -> ZResult<(SubMode, Option<Period>)> {
        use super::decl::{SubMode::*, id::*};
        let mode_flag = self.read()?;
        let mode = match mode_flag & !PERIOD {
            MODE_PUSH => Push,
            MODE_PULL => Pull,
            id => panic!("UNEXPECTED ID FOR SubMode: {}", id)   //@TODO: return error
        };
        let period = if mode_flag & PERIOD > 0{
            Some(Period{
                origin:   self.read_zint()?,
                period:   self.read_zint()?,
                duration: self.read_zint()?
            })
        } else {
            None
        };
        Ok((mode, period))
    }

    fn read_reskey(&mut self, is_numeric: bool) -> ZResult<ResKey> {
        let id = self.read_zint()?;
        if is_numeric {
            Ok(ResKey::RId(id))
        } else {
            let s = self.read_string()?;
            if id == NO_RESOURCE_ID {
                Ok(ResKey::RName(s))
            } else {
                Ok(ResKey::RIdWithSuffix(id, s))
            }
        }
    }

    fn read_query_target(&mut self) -> ZResult<QueryTarget> {
        let storage = self.read_target()?;
        let eval = self.read_target()?;
        Ok(QueryTarget{ storage, eval })
    }

    fn read_target(&mut self) -> ZResult<Target> {
        let t = self.read_zint()?;
        match t {
            0 => Ok(Target::BestMatching),
            1 => {
                let n = self.read_zint()?;
                Ok(Target::Complete{n})
            },
            2 => Ok(Target::All),
            3 => Ok(Target::None),
            id => panic!("UNEXPECTED ID FOR Target: {}", id)   //@TODO: return error
        }
    }

    fn read_consolidation(&mut self) -> ZResult<QueryConsolidation> {
        match self.read_zint()? {
            0 => Ok(QueryConsolidation::None),
            1 => Ok(QueryConsolidation::LastBroker),
            2 => Ok(QueryConsolidation::Incremental),
            id => panic!("UNEXPECTED ID FOR QueryConsolidation: {}", id)   //@TODO: return error
        }
    }

    pub fn read_timestamp(&mut self) -> ZResult<TimeStamp> {
        let time = self.read_zint()?;
        let mut bytes = [0u8; 16];
        self.read_bytes(&mut bytes[..])?;
        let id = uuid::Builder::from_bytes(bytes).build();
        Ok(TimeStamp { time, id })
    }

    fn read_peerid(&mut self) -> ZResult<PeerId> {
        let id = self.read_bytes_array()?;
        Ok(PeerId { id })
    }

}