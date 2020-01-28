use crate::io::rwbuf::{RWBuf, OutOfBounds};
use crate::core::*;
use super::msg::*;
use super::decl;

impl RWBuf {
    pub fn write_message(&mut self, msg: &Message) -> Result<(), OutOfBounds> {
        self.write_deco_frag(&msg.kind)?;

        if msg.has_decorators {
            if msg.cid != 0 {
                self.write_deco_conduit(msg.cid)?;
            }
            if let Some(reply) = &msg.reply_context {
                self.write_deco_reply(reply)?;
            }
            if let Some(props) = &msg.properties {
                    self.write_deco_properties(&props)?;
            }
        }

        self.write(msg.header)?;
        match &msg.body {
            Body::Scout { what } => {
                if let Some(w) = what {
                    self.write_zint(*w)?;
                }
                Ok(())
            }

            Body::Hello { whatami, locators } => {
                if let Some(w) = whatami {
                    self.write_zint(*w)?;
                }
                if let Some(locs) = locators {
                    self.write_locators(locs.as_ref())?;
                }
                Ok(())
            }

            Body::Open { version, whatami, pid, lease, locators } => {
                self.write(*version)?;
                if let Some(w) = whatami {
                    self.write_zint(*w)?;
                }
                self.write_bytes_array(&pid.id)?;
                self.write_zint(*lease)?;
                if let Some(l) = locators {
                    self.write_locators(l)?;
                }
                Ok(())
            }

            Body::Accept { opid, apid, lease } => {
                self.write_bytes_array(&opid.id)?;
                self.write_bytes_array(&apid.id)?;
                self.write_zint(*lease)
            }

            Body::Close { pid, reason } => {
                if let Some(p) = pid {
                    self.write_bytes_array(&p.id)?;
                }
                self.write(*reason)
            }

            Body::KeepAlive { pid } => {
                if let Some(p) = pid {
                    self.write_bytes_array(&p.id)?;
                }
                Ok(())
            }

            Body::Declare { sn, declarations } => {
                self.write_zint(*sn)?;
                self.write_declarations(&declarations)
            }

            Body::Data { reliable:_, sn, key, info, payload } => {
                self.write_zint(*sn)?;
                self.write_reskey(&key)?;
                if let Some(i) = info {
                    self.write_bytes_array(&i)?;
                }
                self.write_bytes_array(&payload)
            }

            Body::Pull { sn, key, pull_id, max_samples } => {
                self.write_zint(*sn)?;
                self.write_reskey(&key)?;
                self.write_zint(*pull_id)?;
                if let Some(n) = max_samples {
                    self.write_zint(*n)?;
                }
                Ok(())
            }

            Body::Query { sn, key, predicate, qid, target, consolidation } => {
                self.write_zint(*sn)?;
                self.write_reskey( &key)?;
                self.write_string(predicate)?;
                self.write_zint(*qid)?;
                if let Some(t) = target {
                    self.write_query_target(t)?;
                }
                self.write_consolidation(consolidation)
            }

            Body::Ping { hash } | 
            Body::Pong { hash } => {
                self.write_zint(*hash)
            }

            Body::Sync { sn, count } => {
                self.write_zint(*sn)?;
                if let Some(c) = count {
                    self.write_zint(*c)?;
                }
                Ok(())
            }

            Body::AckNack { sn, mask } => {
                self.write_zint(*sn)?;
                if let Some(m) = mask {
                    self.write_zint(*m)?;
                }
                Ok(())
            }
        }
    }

    pub fn write_datainfo(&mut self, info: &DataInfo) -> Result<(), OutOfBounds> {
        self.write(info.header)?;
        if let Some(pid) = &info.source_id {
            self.write_bytes_array(&pid.id)?;
        }
        if let Some(sn) = &info.source_sn {
            self.write_zint(*sn)?;
        }
        if let Some(pid) = &info.fist_broker_id {
            self.write_bytes_array(&pid.id)?;
        }
        if let Some(sn) = &info.fist_broker_sn {
            self.write_zint(*sn)?;
        }
        if let Some(ts) = &info.timestamp {
            self.write_timestamp(&ts)?;
        }
        if let Some(kind) = &info.kind {
            self.write_zint(*kind)?;
        }
        if let Some(enc) = &info.encoding {
            self.write_zint(*enc)?;
        }
        Ok(())
    }

    fn write_deco_frag(&mut self, kind: &MessageKind) -> Result<(), OutOfBounds> {
        match kind {
            MessageKind::FullMessage => {
                Ok(())    // No decorator in this case
            }
            MessageKind::FirstFragment{n: None} => {
                self.write(flag::F | id::FRAGMENT)
            }
            MessageKind::FirstFragment{n: Some(i)} => {
                self.write(flag::F | flag::C | id::FRAGMENT)?;
                self.write_zint(*i)
            }
            MessageKind::InbetweenFragment => {
                self.write(id::FRAGMENT)
            }
            MessageKind::LastFragment => {
                self.write(flag::L | id::FRAGMENT)
            }
        }
    }

    fn write_deco_conduit(&mut self, cid: ZInt) -> Result<(), OutOfBounds> {
        if cid <= 4 {
            let hl = ((cid-1) <<5) as u8;
            self.write(flag::Z | hl | id::CONDUIT)
        } else {
            self.write(id::CONDUIT)?;
            self.write_zint(cid)
        }
    }

    fn write_deco_reply(&mut self, reply: &ReplyContext) -> Result<(), OutOfBounds> {
        let fflag = if reply.is_final { flag::F } else { 0 };
        let eflag = match &reply.source {
            ReplySource::Eval => flag::E,
            ReplySource::Storage => 0
        };
        self.write(id::REPLY | fflag | eflag)?;
        self.write_zint(reply.qid)?;
        if let Some(pid) = &reply.replier_id {
            self.write_bytes_array(&pid.id)?;
        } 
        Ok(())
    }

    fn write_deco_properties(&mut self, props: &[Property]) -> Result<(), OutOfBounds> {
        self.write(id::PROPERTIES)?;
        let len = props.len() as ZInt;
        self.write_zint(len)?;
        for p in props {
            self.write_property(p)?;
        }
        Ok(())
    }

    fn write_property(&mut self, p: &Property) -> Result<(), OutOfBounds> {
        self.write_zint(p.key)?;
        self.write_bytes_array(&p.value)
    }

    fn write_locators(&mut self, locators: &[String]) -> Result<(), OutOfBounds> {
        let len = locators.len() as ZInt;
        self.write_zint(len)?;
        for l in locators {
            self.write_string(l)?;
        }
        Ok(())
    }

    fn write_declarations(&mut self, declarations: &[decl::Declaration]) -> Result<(), OutOfBounds> {
        // @TODO !!
        Ok(())
    }

    fn write_reskey(&mut self, key: &ResKey) -> Result<(), OutOfBounds> {
        match key {
            ResKey::ResId { id } => {
                self.write_zint(*id)
            }
            ResKey::ResName { name } => {
                self.write_zint(0 as ZInt)?;
                self.write_string(name)
            }
            ResKey::ResGenId{ id, suffix} => {
                self.write_zint(*id)?;
                self.write_string(suffix)
            }
        }
    }

    fn write_query_target(&mut self, target: &QueryTarget) -> Result<(), OutOfBounds> {
        self.write_target(&target.storage)?;
        self.write_target(&target.eval)
    }

    fn write_target(&mut self, target: &Target) -> Result<(), OutOfBounds> {
        match target {
            Target::BestMatching => {
                self.write_zint(0 as ZInt)
            }
            Target::Complete { n } => {
                self.write_zint(1 as ZInt)?;
                self.write_zint(*n)
            }
            Target::All => {
                self.write_zint(2 as ZInt)
            }
            Target::None => {
                self.write_zint(3 as ZInt)
            }
        }
    }

    fn write_consolidation(&mut self, consolidation: &QueryConsolidation) -> Result<(), OutOfBounds> {
        match consolidation {
            QueryConsolidation::None        => self.write_zint(0),
            QueryConsolidation::LastBroker  => self.write_zint(1),
            QueryConsolidation::Incremental => self.write_zint(2),
        }
    }

    fn write_timestamp(&mut self, tstamp: &TimeStamp) -> Result<(), OutOfBounds> {
        self.write_zint(tstamp.time)?;
        self.write_bytes(tstamp.id.as_bytes())
    }
}
