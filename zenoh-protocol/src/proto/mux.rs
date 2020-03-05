use std::sync::Arc;
use async_trait::async_trait;
use crate::core::{ZInt, PeerId, ResKey};
use crate::io::ArcSlice;
use crate::proto::{
    Message, SubMode, Declaration, 
    Primitives, MessageKind, QueryTarget, 
    QueryConsolidation, ReplyContext, ReplySource};
use crate::session::Session;

pub struct Mux {
    session: Session,
}

impl Mux {
    pub fn new(session: Session) -> Mux {
        Mux {session: session,}
    }
}

#[async_trait]
impl Primitives for Mux {
    async fn resource(&self, rid: &u64, reskey: &ResKey) {
        let mut decls = Vec::new();
        decls.push(Declaration::Resource{rid: *rid, key: reskey.clone()});
        self.session.send(Arc::new(Message::make_declare(
            0, decls, None, None))).await;
    }

    async fn forget_resource(&self, rid: &u64) {
        let mut decls = Vec::new();
        decls.push(Declaration::ForgetResource{rid: *rid});
        self.session.send(Arc::new(Message::make_declare(
            0, decls, None, None))).await;
    }
    
    async fn subscriber(&self, reskey: &ResKey, mode: &SubMode) {
        let mut decls = Vec::new();
        decls.push(Declaration::Subscriber{key: reskey.clone(), mode: mode.clone()});
        self.session.send(Arc::new(Message::make_declare(
            0, decls, None, None))).await;
    }

    async fn forget_subscriber(&self, reskey: &ResKey) {
        let mut decls = Vec::new();
        decls.push(Declaration::ForgetSubscriber{key: reskey.clone()});
        self.session.send(Arc::new(Message::make_declare(
            0, decls, None, None))).await;
    }
    
    async fn publisher(&self, reskey: &ResKey) {
        let mut decls = Vec::new();
        decls.push(Declaration::Publisher{key: reskey.clone()});
        self.session.send(Arc::new(Message::make_declare(
            0, decls, None, None))).await;
    }

    async fn forget_publisher(&self, reskey: &ResKey) {
        let mut decls = Vec::new();
        decls.push(Declaration::ForgetPublisher{key: reskey.clone()});
        self.session.send(Arc::new(Message::make_declare(
            0, decls, None, None))).await;
    }
    
    async fn storage(&self, reskey: &ResKey) {
        let mut decls = Vec::new();
        decls.push(Declaration::Storage{key: reskey.clone()});
        self.session.send(Arc::new(Message::make_declare(
            0, decls, None, None))).await;
    }

    async fn forget_storage(&self, reskey: &ResKey) {
        let mut decls = Vec::new();
        decls.push(Declaration::ForgetStorage{key: reskey.clone()});
        self.session.send(Arc::new(Message::make_declare(
            0, decls, None, None))).await;
    }
    
    async fn eval(&self, reskey: &ResKey) {
        let mut decls = Vec::new();
        decls.push(Declaration::Eval{key: reskey.clone()});
        self.session.send(Arc::new(Message::make_declare(
            0, decls, None, None))).await;
    }

    async fn forget_eval(&self, reskey: &ResKey) {
        let mut decls = Vec::new();
        decls.push(Declaration::ForgetEval{key: reskey.clone()});
        self.session.send(Arc::new(Message::make_declare(
            0, decls, None, None))).await;
    }

    async fn data(&self, reskey: &ResKey, info: &Option<ArcSlice>, payload: &ArcSlice) {
        self.session.send(Arc::new(Message::make_data(
            MessageKind::FullMessage, true, 0, reskey.clone(), info.clone(), payload.clone(), None, None, None))).await;
    }

    async fn query(&self, reskey: &ResKey, predicate: &String, qid: &ZInt, target: &Option<QueryTarget>, consolidation: &QueryConsolidation) {
        self.session.send(Arc::new(Message::make_query(
            0, reskey.clone(), predicate.clone(), *qid, target.clone(), consolidation.clone(), None, None))).await;
    }

    async fn reply(&self, qid: &ZInt, source: &ReplySource, replierid: &Option<PeerId>, reskey: &ResKey, info: &Option<ArcSlice>, payload: &ArcSlice) {
        self.session.send(Arc::new(Message::make_data(
            MessageKind::FullMessage, true, 0, reskey.clone(), info.clone(), payload.clone(), 
            Some(ReplyContext::make(*qid, source.clone(), replierid.clone())), None, None))).await;
    }

    async fn pull(&self, is_final: bool, reskey: &ResKey, pull_id: &ZInt, max_samples: &Option<ZInt>) {
        self.session.send(Arc::new(Message::make_pull(is_final, 0, reskey.clone(), *pull_id, max_samples.clone(), None, None))).await;
    }
}