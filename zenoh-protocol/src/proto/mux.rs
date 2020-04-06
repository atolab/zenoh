use std::sync::Arc;
use async_trait::async_trait;
use crate::core::{ZInt, ResKey};
use crate::io::RBuf;
use crate::proto::{
    Message, SubInfo, Declaration, 
    Primitives, MessageKind, QueryTarget, 
    QueryConsolidation, ReplyContext, Reply, ReplySource};
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
    async fn resource(&self, rid: u64, key: ResKey) {
        let mut decls = Vec::new();
        decls.push(Declaration::Resource{rid, key});
        self.handler.handle_message(Message::make_declare(
            0, decls, None, None)).await;
    }

    async fn forget_resource(&self, rid: u64) {
        let mut decls = Vec::new();
        decls.push(Declaration::ForgetResource{rid});
        self.handler.handle_message(Message::make_declare(
            0, decls, None, None)).await;
    }
    
    async fn subscriber(&self, key: ResKey, info: SubInfo) {
        let mut decls = Vec::new();
        decls.push(Declaration::Subscriber{key, info});
        self.handler.handle_message(Message::make_declare(
            0, decls, None, None)).await;
    }

    async fn forget_subscriber(&self, key: ResKey) {
        let mut decls = Vec::new();
        decls.push(Declaration::ForgetSubscriber{key});
        self.handler.handle_message(Message::make_declare(
            0, decls, None, None)).await;
    }
    
    async fn publisher(&self, key: ResKey) {
        let mut decls = Vec::new();
        decls.push(Declaration::Publisher{key});
        self.handler.handle_message(Message::make_declare(
            0, decls, None, None)).await;
    }

    async fn forget_publisher(&self, key: ResKey) {
        let mut decls = Vec::new();
        decls.push(Declaration::ForgetPublisher{key});
        self.handler.handle_message(Message::make_declare(
            0, decls, None, None)).await;
    }
    
    async fn storage(&self, key: ResKey) {
        let mut decls = Vec::new();
        decls.push(Declaration::Storage{key});
        self.handler.handle_message(Message::make_declare(
            0, decls, None, None)).await;
    }

    async fn forget_storage(&self, key: ResKey) {
        let mut decls = Vec::new();
        decls.push(Declaration::ForgetStorage{key});
        self.handler.handle_message(Message::make_declare(
            0, decls, None, None)).await;
    }
    
    async fn eval(&self, key: ResKey) {
        let mut decls = Vec::new();
        decls.push(Declaration::Eval{key});
        self.handler.handle_message(Message::make_declare(
            0, decls, None, None)).await;
    }

    async fn forget_eval(&self, key: ResKey) {
        let mut decls = Vec::new();
        decls.push(Declaration::ForgetEval{key});
        self.handler.handle_message(Message::make_declare(
            0, decls, None, None)).await;
    }

    async fn data(&self, key: ResKey, reliable: bool, info: Option<RBuf>, payload: RBuf) {
        self.handler.handle_message(Message::make_data(
            MessageKind::FullMessage, reliable, 0, key, info, payload, None, None, None)).await;
    }

    async fn query(&self, key: ResKey, predicate: String, qid: ZInt, target: QueryTarget, consolidation: QueryConsolidation) {
        let target_opt = if target == QueryTarget::default() { None } else { Some(target) };
        self.handler.handle_message(Message::make_query(
            0, key, predicate, qid, target_opt, consolidation, None, None)).await;
    }

    async fn reply(&self, qid: ZInt, reply: Reply) {
        match reply {
            Reply::ReplyData {source, replier_id, reskey, info, payload} => {
                self.handler.handle_message(Message::make_data(
                    MessageKind::FullMessage, true, 0, reskey.clone(), info.clone(), payload.clone(), 
                    Some(ReplyContext::make(qid, source.clone(), Some(replier_id.clone()))), None, None)).await;
            }
            Reply::SourceFinal {source, replier_id} => {
                self.handler.handle_message(Message::make_unit(
                    true, 0, Some(ReplyContext::make(qid, source.clone(), Some(replier_id.clone()))), None, None)).await;
            }
            Reply::ReplyFinal {} => {
                self.handler.handle_message(Message::make_unit(
                    true, 0, Some(ReplyContext::make(qid, ReplySource::Storage, None)), None, None)).await;
            }
        }
    }

    async fn pull(&self, is_final: bool, key: ResKey, pull_id: ZInt, max_samples: Option<ZInt>) {
        self.handler.handle_message(Message::make_pull(is_final, 0, key, pull_id, max_samples, None, None)).await;
    }

    async fn close(&self) {
        self.handler.close().await;
    }
}