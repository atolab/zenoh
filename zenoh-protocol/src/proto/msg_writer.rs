use crate::io::WBuf;
use crate::core::{ZInt, Property, ResKey, TimeStamp, NO_RESOURCE_ID};
use crate::link::Locator;
use super::msg::*;
use super::decl::{Declaration, SubMode, Reliability, Period};

macro_rules! check {
    ($op:expr) => (if ! $op { return false })
}


impl WBuf {
    pub fn write_message(&mut self, msg: &Message) -> bool {
        check!(self.write_deco_frag(&msg.kind));

        if msg.has_decorators {
            if msg.cid != 0 {
                check!(self.write_deco_conduit(msg.cid));
            }
            if let Some(reply) = &msg.reply_context {
                check!(self.write_deco_reply(reply));
            }
            if let Some(props) = &msg.properties {
                check!(self.write_deco_properties(&props));
            }
        }

        check!(self.write(msg.header));
        match &msg.body {
            Body::Scout { what } => {
                if let Some(w) = what {
                    check!(self.write_zint(*w));
                }
            }

            Body::Hello { whatami, locators } => {
                if *whatami != WhatAmI::Broker {
                    check!(self.write_zint(WhatAmI::to_zint(whatami)));
                }
                if let Some(locs) = locators {
                    check!(self.write_locators(locs.as_ref()));
                }
            }

            Body::Open { version, whatami, pid, lease, locators } => {
                check!(self.write(*version));
                if *whatami != WhatAmI::Broker {
                    check!(self.write_zint(WhatAmI::to_zint(whatami)));
                }
                check!(
                    self.write_bytes_array(&pid.id) &&
                    self.write_zint(*lease));
                if let Some(l) = locators {
                    check!(self.write_locators(l));
                }
            }

            Body::Accept {whatami, opid, apid, lease } => {
                if *whatami != WhatAmI::Broker {
                    check!(self.write_zint(WhatAmI::to_zint(whatami)));
                }
                check!(
                    self.write_bytes_array(&opid.id) &&
                    self.write_bytes_array(&apid.id) &&
                    self.write_zint(*lease));
            }

            Body::Close { pid, reason } => {
                if let Some(p) = pid {
                    check!(self.write_bytes_array(&p.id));
                }
                check!(self.write(*reason));
            }

            Body::KeepAlive { pid } => {
                if let Some(p) = pid {
                    check!(self.write_bytes_array(&p.id));
                }
            }

            Body::Declare { sn, declarations } => {
                check!(
                    self.write_zint(*sn) &&
                    self.write_declarations(&declarations));
            }

            Body::Data { sn, key, info, payload, .. } => {
                check!(
                    self.write_zint(*sn) &&
                    self.write_reskey(&key));
                if let Some(rbuf) = info {
                    check!(self.write_rbuf(&rbuf));
                }
                check!(self.write_rbuf(&payload));
            }

            Body::Unit { sn, .. } => {
                check!(self.write_zint(*sn));
            }

            Body::Pull { sn, key, pull_id, max_samples } => {
                check!(
                    self.write_zint(*sn) &&
                    self.write_reskey(&key) &&
                    self.write_zint(*pull_id));
                if let Some(n) = max_samples {
                    check!(self.write_zint(*n));
                }
            }

            Body::Query { sn, key, predicate, qid, target, consolidation } => {
                check!(
                    self.write_zint(*sn) &&
                    self.write_reskey( &key) &&
                    self.write_string(predicate) &&
                    self.write_zint(*qid));
                if let Some(t) = target {
                    check!(self.write_query_target(t));
                }
                check!(self.write_consolidation(consolidation));
            }

            Body::Ping { hash } | 
            Body::Pong { hash } => {
                check!(self.write_zint(*hash));
            }

            Body::Sync { sn, count } => {
                check!(self.write_zint(*sn));
                if let Some(c) = count {
                    check!(self.write_zint(*c));
                }
            }

            Body::AckNack { sn, mask } => {
                check!(self.write_zint(*sn));
                if let Some(m) = mask {
                    check!(self.write_zint(*m));
                }
            }
        }
        true
    }

    pub fn write_datainfo(&mut self, info: &DataInfo) -> bool {
        check!(self.write(info.header));
        if let Some(pid) = &info.source_id {
            check!(self.write_bytes_array(&pid.id));
        }
        if let Some(sn) = &info.source_sn {
            check!(self.write_zint(*sn));
        }
        if let Some(pid) = &info.fist_broker_id {
            check!(self.write_bytes_array(&pid.id));
        }
        if let Some(sn) = &info.fist_broker_sn {
            check!(self.write_zint(*sn));
        }
        if let Some(ts) = &info.timestamp {
            check!(self.write_timestamp(&ts));
        }
        if let Some(kind) = &info.kind {
            check!(self.write_zint(*kind));
        }
        if let Some(enc) = &info.encoding {
            check!(self.write_zint(*enc));
        }
        true
    }

    fn write_deco_frag(&mut self, kind: &MessageKind) -> bool {
        match kind {
            MessageKind::FullMessage => { true }, // No decorator in this case
            MessageKind::FirstFragment{n: None} => {
                self.write(flag::F | id::FRAGMENT)
            }
            MessageKind::FirstFragment{n: Some(i)} => {
                self.write(flag::F | flag::C | id::FRAGMENT) &&
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

    fn write_deco_conduit(&mut self, cid: ZInt) -> bool {
        if cid <= 4 {
            let hl = ((cid-1) <<5) as u8;
            self.write(flag::Z | hl | id::CONDUIT)
        } else {
            self.write(id::CONDUIT) &&
            self.write_zint(cid)
        }
    }

    fn write_deco_reply(&mut self, reply: &ReplyContext) -> bool {
        let fflag = if reply.is_final { flag::F } else { 0 };
        let eflag = match &reply.source {
            ReplySource::Eval => flag::E,
            ReplySource::Storage => 0
        };
        check!(self.write(id::REPLY | fflag | eflag) &&
               self.write_zint(reply.qid));
        if let Some(pid) = &reply.replier_id {
            check!(self.write_bytes_array(&pid.id));
        }
        true
    }

    fn write_deco_properties(&mut self, props: &[Property]) -> bool {
        let len = props.len() as ZInt;        
        check!(self.write(id::PROPERTIES) &&
               self.write_zint(len));
        for p in props {
            check!(self.write_property(p));
        }
        true
    }

    fn write_property(&mut self, p: &Property) -> bool {
        self.write_zint(p.key) &&
        self.write_bytes_array(&p.value)
    }

    fn write_locators(&mut self, locators: &[Locator]) -> bool {
        let len = locators.len() as ZInt;
        check!(self.write_zint(len));
        for l in locators {
            check!(self.write_string(&l.to_string()));
        }
        true
    }

    fn write_declarations(&mut self, declarations: &[Declaration]) -> bool {
        let len = declarations.len() as ZInt;
        check!(self.write_zint(len));
        for l in declarations {
            check!(self.write_declaration(l));
        }
        true
    }

    fn write_declaration(&mut self, declaration: &Declaration) -> bool {
        use super::decl::{Declaration::*, id::*};

        macro_rules! write_key_decl {
            ($buf:ident, $flag:ident, $key:ident) => {{
                $buf.write($flag | (if $key.is_numerical() { flag::C } else { 0 })) &&
                $buf.write_reskey($key)
            }}
        }
          
        match declaration {
            Resource { rid, key } => {
                let cflag = if key.is_numerical() { flag::C } else { 0 };
                self.write(RESOURCE | cflag) &&
                self.write_zint(*rid) &&
                self.write_reskey(key)
            }

            ForgetResource { rid } => {
                self.write(FORGET_RESOURCE) &&
                self.write_zint(*rid)
            }

            Subscriber { key, info } =>  {
                let sflag = if info.mode == SubMode::Push && info.period.is_none() { 0 } else { flag::S };
                let rflag = if info.reliability == Reliability::Reliable { flag::R } else { 0 };
                let cflag = if key.is_numerical() { flag::C } else { 0 };
                self.write(SUBSCRIBER | sflag | rflag | cflag) &&
                self.write_reskey(key) &&
                (sflag == 0 || self.write_submode(&info.mode, &info.period))
            }

            ForgetSubscriber { key } => write_key_decl!(self, FORGET_SUBSCRIBER, key),
            Publisher { key }        => write_key_decl!(self, PUBLISHER, key),
            ForgetPublisher { key }  => write_key_decl!(self, FORGET_PUBLISHER, key),
            Queryable { key }        => write_key_decl!(self, QUERYABLE, key),
            ForgetQueryable { key }  => write_key_decl!(self, FORGET_QUERYABLE, key),
        }
    }

    fn write_submode(&mut self, mode: &SubMode, period: &Option<Period>) -> bool {
        use super::decl::{SubMode::*, id::*};
        let period_mask: u8 = if period.is_some() { PERIOD } else { 0x00 };
        check!(
            match mode {
                Push => self.write(MODE_PUSH | period_mask),
                Pull => self.write(MODE_PULL | period_mask),
            });
        if let Some(p) = period {
            self.write_zint(p.origin) &&
            self.write_zint(p.period) &&
            self.write_zint(p.duration)
        } else {
            true
        }
    }

    fn write_reskey(&mut self, key: &ResKey) -> bool {
        match key {
            ResKey::RId(rid) => {
                self.write_zint(*rid)
            }
            ResKey::RName(name) => {
                self.write_zint(NO_RESOURCE_ID) &&
                self.write_string(name)
            }
            ResKey::RIdWithSuffix(rid, suffix) => {
                self.write_zint(*rid) &&
                self.write_string(suffix)
            }
        }
    }

    fn write_query_target(&mut self, target: &QueryTarget) -> bool {
        self.write_target(&target.storage) &&
        self.write_target(&target.eval)
    }

    fn write_target(&mut self, target: &Target) -> bool {
        match target {
            Target::BestMatching => {
                self.write_zint(0 as ZInt)
            }
            Target::Complete { n } => {
                self.write_zint(1 as ZInt) &&
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

    fn write_consolidation(&mut self, consolidation: &QueryConsolidation) -> bool {
        match consolidation {
            QueryConsolidation::None        => self.write_zint(0),
            QueryConsolidation::LastBroker  => self.write_zint(1),
            QueryConsolidation::Incremental => self.write_zint(2),
        }
    }

    fn write_timestamp(&mut self, tstamp: &TimeStamp) -> bool {
        self.write_zint(tstamp.time) &&
        self.write_bytes(tstamp.id.as_bytes())
    }

}
