use async_trait::async_trait;
use crate::core::ZError;
use crate::proto::{Message, Body, Declaration, MsgHandler, flag};
use crate::session::SessionCallback;

pub struct DeMux<Handler: MsgHandler + Send + Sync> {
    handler: Handler,
}

impl<Handler: MsgHandler + Send + Sync> DeMux<Handler> {
    pub fn new(handler: Handler) -> DeMux<Handler> {
        DeMux {handler: handler,}
    }
}

#[async_trait]
impl<Handler: MsgHandler + Send + Sync> SessionCallback for DeMux<Handler> {

    async fn receive_message(&self, msg: Message) -> Result<(), ZError> {
        match msg.get_body() {
            Body::Declare{sn: _, declarations} => {
                for declaration in declarations {
                    match declaration {
                        Declaration::Resource {rid, key} => {
                            self.handler.resource(rid, key).await;
                        }
                        Declaration::Publisher {key} => {
                            self.handler.publisher(key).await;
                        }
                        Declaration::Subscriber {key, mode} => {
                            self.handler.subscriber(key, mode).await;
                        }
                        Declaration::Storage {key} => {
                            self.handler.storage(key).await;
                        }
                        Declaration::Eval {key} => {
                            self.handler.eval(key).await;
                        }
                        Declaration::ForgetResource {rid} => {
                            self.handler.forget_resource(rid).await;
                        }
                        Declaration::ForgetPublisher {key} => {
                            self.handler.forget_publisher(key).await;
                        }
                        Declaration::ForgetSubscriber {key} => {
                            self.handler.forget_subscriber(key).await;
                        }
                        Declaration::ForgetStorage {key} => {
                            self.handler.forget_storage(key).await;
                        }
                        Declaration::ForgetEval {key} => {
                            self.handler.forget_eval(key).await;
                        }
                    }

                }
            }
            Body::Data{reliable:_, sn:_, key, info, payload} => {
                match &msg.reply_context {
                    None => {self.handler.data(key, info, payload).await;}
                    Some(rep) => {self.handler.reply(&rep.qid, &rep.source, &rep.replier_id, key, info, payload).await;}
                }
            }
            Body::Query{sn:_, key, predicate, qid, target, consolidation} => {
                self.handler.query(key, predicate, qid, target, consolidation).await;
            }
            Body::Pull{sn:_, key, pull_id, max_samples} => {
                self.handler.pull(flag::has_flag(msg.header, flag::F), key, pull_id, max_samples).await;
            }
            _ => () 
        }

        Ok(())
    }
}