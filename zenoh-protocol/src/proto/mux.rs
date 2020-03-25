use std::sync::Arc;
use async_trait::async_trait;
use crate::core::{ZInt, PeerId, ResKey};
use crate::io::ArcSlice;
use crate::proto::{
    Message, SubInfo, Declaration, 
    Primitives, MessageKind, QueryTarget, 
    QueryConsolidation, ReplyContext, ReplySource};
use crate::session::MsgHandler;

pub struct Mux<T: MsgHandler + Send + Sync + ?Sized> {
    handler: Arc<T>,
}

impl<T: MsgHandler + Send + Sync + ?Sized> Mux<T> {
    pub fn new(handler: Arc<T>) -> Mux<T> {
        Mux {handler}
    }
}

#[allow(unused_must_use)] // TODO
#[async_trait]
impl<T: MsgHandler + Send + Sync + ?Sized> Primitives for Mux<T> {
    async fn resource(&self, rid: u64, reskey: &ResKey) {
        let mut decls = Vec::new();
        decls.push(Declaration::Resource{rid, key: reskey.clone()});
        self.handler.handle_message(Message::make_declare(
            0, decls, None, None)).await;
    }

    async fn forget_resource(&self, rid: u64) {
        let mut decls = Vec::new();
        decls.push(Declaration::ForgetResource{rid});
        self.handler.handle_message(Message::make_declare(
            0, decls, None, None)).await;
    }
    
    async fn subscriber(&self, reskey: &ResKey, sub_info: &SubInfo) {
        let mut decls = Vec::new();
        decls.push(Declaration::Subscriber{key: reskey.clone(), info: sub_info.clone()});
        self.handler.handle_message(Message::make_declare(
            0, decls, None, None)).await;
    }

    async fn forget_subscriber(&self, reskey: &ResKey) {
        let mut decls = Vec::new();
        decls.push(Declaration::ForgetSubscriber{key: reskey.clone()});
        self.handler.handle_message(Message::make_declare(
            0, decls, None, None)).await;
    }
    
    async fn publisher(&self, reskey: &ResKey) {
        let mut decls = Vec::new();
        decls.push(Declaration::Publisher{key: reskey.clone()});
        self.handler.handle_message(Message::make_declare(
            0, decls, None, None)).await;
    }

    async fn forget_publisher(&self, reskey: &ResKey) {
        let mut decls = Vec::new();
        decls.push(Declaration::ForgetPublisher{key: reskey.clone()});
        self.handler.handle_message(Message::make_declare(
            0, decls, None, None)).await;
    }
    
    async fn storage(&self, reskey: &ResKey) {
        let mut decls = Vec::new();
        decls.push(Declaration::Storage{key: reskey.clone()});
        self.handler.handle_message(Message::make_declare(
            0, decls, None, None)).await;
    }

    async fn forget_storage(&self, reskey: &ResKey) {
        let mut decls = Vec::new();
        decls.push(Declaration::ForgetStorage{key: reskey.clone()});
        self.handler.handle_message(Message::make_declare(
            0, decls, None, None)).await;
    }
    
    async fn eval(&self, reskey: &ResKey) {
        let mut decls = Vec::new();
        decls.push(Declaration::Eval{key: reskey.clone()});
        self.handler.handle_message(Message::make_declare(
            0, decls, None, None)).await;
    }

    async fn forget_eval(&self, reskey: &ResKey) {
        let mut decls = Vec::new();
        decls.push(Declaration::ForgetEval{key: reskey.clone()});
        self.handler.handle_message(Message::make_declare(
            0, decls, None, None)).await;
    }

    async fn data(&self, reskey: &ResKey, reliable: bool, info: &Option<ArcSlice>, payload: &ArcSlice) {
        self.handler.handle_message(Message::make_data(
            MessageKind::FullMessage, reliable, 0, reskey.clone(), info.clone(), payload.clone(), None, None, None)).await;
    }

    async fn query(&self, reskey: &ResKey, predicate: &str, qid: ZInt, target: QueryTarget, consolidation: QueryConsolidation) {
        let target_opt = if target == QueryTarget::default() { None } else { Some(target) };
        self.handler.handle_message(Message::make_query(
            0, reskey.clone(), predicate.to_string(), qid, target_opt, consolidation.clone(), None, None)).await;
    }

    async fn reply(&self, qid: ZInt, source: &ReplySource, replierid: &Option<PeerId>, reskey: &ResKey, info: &Option<ArcSlice>, payload: &ArcSlice) {
        self.handler.handle_message(Message::make_data(
            MessageKind::FullMessage, true, 0, reskey.clone(), info.clone(), payload.clone(), 
            Some(ReplyContext::make(qid, source.clone(), replierid.clone())), None, None)).await;
    }

    async fn pull(&self, is_final: bool, reskey: &ResKey, pull_id: ZInt, max_samples: &Option<ZInt>) {
        self.handler.handle_message(Message::make_pull(is_final, 0, reskey.clone(), pull_id, *max_samples, None, None)).await;
    }

    async fn close(&self) {
        self.handler.close().await;
    }
}