use std::sync::Arc;
use zenoh_protocol::core::ZInt;
use crate::routing::face::Face;

pub struct Query {
    pub(super) src_face: Arc<Face>,
    pub(super) src_qid: ZInt,
}
