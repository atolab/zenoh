use crate::io::rwbuf::{RWBuf, OutOfBounds};
// use crate::io::codec::RWBuf;
use crate::core::*;
use super::msg::*;

// type Msg = Message;

pub fn write_message<'a>(buf: &'a mut RWBuf, msg: &Message) -> Result<&'a mut RWBuf, OutOfBounds> {
    write_decl_frag(buf, &msg.kind)?;
    
    if msg.cid != 0 {
        write_decl_conduit(buf, msg.cid)?;
    }

    if let Some(reply) = &msg.reply_context {
        write_decl_reply(buf, reply)?;
    }

    if let Some(props) = &msg.properties {
        write_decl_properties(buf, props.as_ref())?;
    }

    buf.write(msg.header)?;
    match &msg.body {
        Body::Scout { what: Some(w) } => {
            buf.write_zint(*w)
        }

        Body::Hello { whatami, locators } => {
            if let Some(w) = whatami {
                buf.write_zint(*w)?;
            }
            if let Some(locs) = locators {
                write_locators(buf, locs.as_ref())?;
            }
            Ok(buf)
        }

        _ => Ok(buf) // Nothing more to add after header
    }
}


fn write_decl_frag<'a>(buf: &'a mut RWBuf, kind: &MessageKind) -> Result<&'a mut RWBuf, OutOfBounds> {
    match kind {
        MessageKind::FullMessage => {
            Ok(buf)    // No decorator in this case
        }
        MessageKind::FirstFragment{n: None} => {
            buf.write(flag::F | id::FRAGMENT)
        }
        MessageKind::FirstFragment{n: Some(i)} => {
            buf.write(flag::F | flag::C | id::FRAGMENT)?
               .write_zint(*i)
        }
        MessageKind::InbetweenFragment => {
            buf.write(id::FRAGMENT)
        }
        MessageKind::LastFragment => {
            buf.write(flag::L | id::FRAGMENT)
        }
    }
}

fn write_decl_conduit(buf: &mut RWBuf, cid: ZInt) -> Result<&mut RWBuf, OutOfBounds> {
    if cid <= 4 {
        let hl = ((cid-1) <<5) as u8;
        buf.write(flag::Z | hl | id::CONDUIT)
    } else {
        buf.write(id::CONDUIT)?
           .write_zint(cid)
    }
}

fn write_decl_reply<'a>(buf: &'a mut RWBuf, reply: &ReplyContext) -> Result<&'a mut RWBuf, OutOfBounds> {
    // @TODO: complete...
    buf.write(id::REPLY)?
           .write_zint(reply.qid)
}

fn write_decl_properties<'a>(buf: &'a mut RWBuf, props: &[Property]) -> Result<&'a mut RWBuf, OutOfBounds> {
    let len = props.len() as ZInt;
    buf.write_zint(len)?;
    for p in props {
        write_property(buf, p)?;
    }
    Ok(buf)
}

fn write_property<'a>(buf: &'a mut RWBuf, p: &Property) -> Result<&'a mut RWBuf, OutOfBounds> {
    buf.write_zint(p.key)?
       .write_bytes(&p.value)
}

fn write_locators<'a>(buf: &'a mut RWBuf, locators: &[String]) -> Result<&'a mut RWBuf, OutOfBounds> {
    let len = locators.len() as ZInt;
    buf.write_zint(len)?;
    for l in locators {
        buf.write_string(l)?;
    }
    Ok(buf)
}
