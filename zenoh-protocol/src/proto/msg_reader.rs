use crate::io::rwbuf::{RWBuf, OutOfBounds};
use crate::core::*;
use super::msg::*;
use super::decl;
use std::sync::Arc;
use std::convert::TryInto;

impl RWBuf {

    pub fn read_message(&mut self) -> Result<Message, OutOfBounds> {
        let mut kind = MessageKind::FullMessage;
        let mut cid = None;
        let mut reply_context = None;
        let mut properties : Option<Arc<Vec<Property>>> = None;

        loop {
            let header = self.read()?;
            match flag::mid(header) {
                id::FRAGMENT => {
                    kind = self.read_decl_frag(header)?;
                    continue;
                }

                id::CONDUIT => {
                    cid = Some(self.read_decl_conduit(header)?);
                    continue;
                }

                id::REPLY => {
                    reply_context = Some(self.read_decl_reply(header)?);
                    continue;
                }

                id::PROPERTIES => {
                    properties = Some(Arc::new(self.read_decl_properties(header)?));
                    continue;
                }

                id::SCOUT => {
                    let what = if flag::has_flag(header, flag::W) {
                        Some(self.read_zint()?)
                    } else { None };
                    return Ok(Message::make_scout(what, cid, properties));
                }

                id::HELLO => {
                    let whatami = if flag::has_flag(header, flag::W) {
                        Some(self.read_zint()?)
                    } else { None };
                    let locators = if flag::has_flag(header, flag::L) {
                        Some(self.read_locators()?)
                    } else { None };
                    return Ok(Message::make_hello(whatami, locators, cid, properties));
                }

                id::OPEN => {
                    let version = self.read()?;
                    let whatami = if flag::has_flag(header, flag::W) {
                        Some(self.read_zint()?)
                    } else { None };
                    let pid = self.read_peerid()?;
                    let lease = self.read_zint()?;
                    let locators = if flag::has_flag(header, flag::L) {
                        Some(self.read_locators()?)
                    } else { None };
                    return Ok(Message::make_open(version, whatami, pid, lease, locators, cid, properties));
                }

                id::ACCEPT => {
                    let opid = self.read_peerid()?;
                    let apid = self.read_peerid()?;
                    let lease = self.read_zint()?;
                    return Ok(Message::make_accept(opid, apid, lease, cid, properties));
                }

                id::CLOSE => {
                    let pid = if flag::has_flag(header, flag::P) {
                        Some(self.read_peerid()?)
                    } else { None };
                    let reason = self.read()?;
                    return Ok(Message::make_close(pid, reason, cid, properties));
                }

                id::KEEP_ALIVE => {
                    let pid = if flag::has_flag(header, flag::P) {
                        Some(self.read_peerid()?)
                    } else { None };
                    return Ok(Message::make_keep_alive(pid, reply_context, cid, properties));
                }

                id::DECLARE => {
                    let sn = self.read_zint()?;
                    let declarations = self.read_declarations()?;
                    return Ok(Message::make_declare(sn, declarations, cid, properties));
                }

                id::DATA => {
                    let reliable = flag::has_flag(header, flag::R);
                    let sn = self.read_zint()?;
                    let key = self.read_reskey(flag::has_flag(header, flag::C))?;
                    let info = if flag::has_flag(header, flag::I) {
                        Some(Arc::new(self.read_bytes_array()?))
                    } else { None };
                    let payload = Arc::new(self.read_bytes_array()?);
                    return Ok(Message::make_data(reliable, sn, key, info, payload, cid, reply_context, properties))
                }

                id::PULL => {
                    let is_final = flag::has_flag(header, flag::F);
                    let sn = self.read_zint()?;
                    let key = self.read_reskey(flag::has_flag(header, flag::C))?;
                    let pull_id = self.read_zint()?;
                    let max_samples = if flag::has_flag(header, flag::N) {
                        Some(self.read_zint()?)
                    } else { None };
                    return Ok(Message::make_pull(is_final, sn, key, pull_id, max_samples, cid, properties))
                }

                id::QUERY => {
                    let sn = self.read_zint()?;
                    let key = self.read_reskey(flag::has_flag(header, flag::C))?;
                    let predicate = self.read_string()?;
                    let qid = self.read_zint()?;
                    let target = if flag::has_flag(header, flag::T) {
                        Some(self.read_query_target()?)
                    } else { None };
                    let consolidation = self.read_consolidation()?;
                    return Ok(Message::make_query(sn, key, predicate, qid, target, consolidation, cid, properties))
                }

                id::PING_PONG => {
                    let hash = self.read_zint()?;
                    if flag::has_flag(header, flag::P) {
                        return Ok(Message::make_ping(hash, cid, properties))
                    } else {
                        return Ok(Message::make_pong(hash, cid, properties))
                    }
                }

                id::SYNC => {
                    let reliable = flag::has_flag(header, flag::R);
                    let sn = self.read_zint()?;
                    let count = if flag::has_flag(header, flag::C) {
                        Some(self.read_zint()?)
                    } else { None };
                    return Ok(Message::make_sync(reliable, sn, count, cid, properties))
                }

                id::ACK_NACK => {
                    let sn = self.read_zint()?;
                    let mask = if flag::has_flag(header, flag::M) {
                        Some(self.read_zint()?)
                    } else { None };
                    return Ok(Message::make_ack_nack(sn, mask, cid, properties))
                }

                mid @ _ => panic!("UNEXPECTED MESSAGE ID: {:#02x}", mid)
            }
        }
    }

    fn read_decl_frag(&mut self, header: u8) -> Result<MessageKind, OutOfBounds> {
        if flag::has_flag(header, flag::F) {
            if flag::has_flag(header, flag::C) {
                let n = self.read_zint()?;
                Ok(MessageKind::FirstFragment{ n:Some(n) })
            } else {
                Ok(MessageKind::FirstFragment{ n:None })
            }
        } else if flag::has_flag(header, flag::L) {
            Ok(MessageKind::LastFragment)
        } else {
            Ok(MessageKind::InbetweenFragment)
        }
    }

    fn read_decl_conduit(&mut self, header: u8) -> Result<ZInt, OutOfBounds> {
        if flag::has_flag(header, flag::Z) {
            let hl = ((flag::flags(header) ^ flag::Z)) >> 5;
            Ok(hl as ZInt)
        } else {
            let id = self.read_zint()?;
            Ok(id)
        }
    }

    fn read_decl_reply(&mut self, header: u8) -> Result<ReplyContext, OutOfBounds> {
        let is_final = flag::has_flag(header, flag::F);
        let source = if flag::has_flag(header, flag::E) { ReplySource::Eval } else { ReplySource::Storage };
        let qid = self.read_zint()?;
        let replier_id = if is_final { None } else {
            Some(self.read_peerid()?)
        };
        Ok(ReplyContext{ is_final, qid, source, replier_id })
    }

    fn read_decl_properties(&mut self, _: u8) -> Result<Vec<Property> , OutOfBounds> {
        let len = self.read_zint()?;
        let mut vec: Vec<Property> = Vec::new();
        for _ in 0..len {
            vec.push(self.read_property()?);
        }
        Ok(vec)
    }

    fn read_property(&mut self) -> Result<Property , OutOfBounds> {
        let key = self.read_zint()?;
        let value = self.read_bytes_array()?;
        Ok(Property{ key, value })
    }

    fn read_locators(&mut self) -> Result<Vec<String>, OutOfBounds> {
        let len = self.read_zint()?;
        let mut vec: Vec<String> = Vec::new();
        for _ in 0..len {
            vec.push(self.read_string()?);
        }
        Ok(vec)
    }

    fn read_declarations(&mut self) -> Result<Vec<decl::Declaration>, OutOfBounds> {
        // @TODO
        Ok(Vec::new())
    }

    fn read_reskey(&mut self, is_numeric: bool) -> Result<ResKey, OutOfBounds> {
        let id = self.read_zint()?;
        if is_numeric {
            Ok(ResKey::ResId{ id })
        } else {
            let s = self.read_string()?;
            if id == 0 {
                Ok(ResKey::ResName{ name: s })
            } else {
                Ok(ResKey::ResGenId{ id, suffix: s })
            }
        }
    }

    fn read_query_target(&mut self) -> Result<QueryTarget, OutOfBounds> {
        let storage = self.read_target()?;
        let eval = self.read_target()?;
        Ok(QueryTarget{ storage, eval })
    }

    fn read_target(&mut self) -> Result<Target, OutOfBounds> {
        let t = self.read_zint()?;
        match t {
            0 => Ok(Target::BestMatching),
            1 => {
                let n = self.read_zint()?;
                Ok(Target::Complete{n})
            },
            2 => Ok(Target::All),
            3 => Ok(Target::None),
            _ => panic!("UNEXPECTED VALUE FOR TARGET: {}", t)  //@TODO: return error
        }
    }

    fn read_consolidation(&mut self) -> Result<QueryConsolidation, OutOfBounds> {
        panic!("NOT YET IMPLEMENTED: read_consolidation")
    }

    fn read_timestamp(&mut self) -> Result<TimeStamp, OutOfBounds> {
        let time = self.read_zint()?;
        let bytes : [u8; 16] = self.read_slice(16)?.try_into().expect("SHOULDN'T HAPPEN");
        let id = uuid::Builder::from_bytes(bytes).build();
        Ok(TimeStamp { time, id })
    }

    fn read_peerid(&mut self) -> Result<PeerId, OutOfBounds> {
        let id = self.read_bytes_array()?;
        Ok(PeerId { id })
    }

}