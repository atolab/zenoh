use async_trait::async_trait;
use crate::core::ZError;
use crate::proto::{Message, Body, Declaration, Primitives, flag};
use crate::session::MsgHandler;

pub struct DeMux<P: Primitives + Send + Sync> {
    primitives: P,
}

impl<P: Primitives + Send + Sync> DeMux<P> {
    pub fn new(primitives: P) -> DeMux<P> {
        DeMux {primitives,}
    }
}

#[async_trait]
impl<P: Primitives + Send + Sync> MsgHandler for DeMux<P> {

    async fn handle_message(&self, msg: Message) -> Result<(), ZError> {
        match msg.get_body() {
            Body::Declare{declarations, ..} => {
                for declaration in declarations {
                    match declaration {
                        Declaration::Resource {rid, key} => {
                            self.primitives.resource(*rid, key).await;
                        }
                        Declaration::Publisher {key} => {
                            self.primitives.publisher(key).await;
                        }
                        Declaration::Subscriber {key, info} => {
                            self.primitives.subscriber(key, info).await;
                        }
                        Declaration::Storage {key} => {
                            self.primitives.storage(key).await;
                        }
                        Declaration::Eval {key} => {
                            self.primitives.eval(key).await;
                        }
                        Declaration::ForgetResource {rid} => {
                            self.primitives.forget_resource(*rid).await;
                        }
                        Declaration::ForgetPublisher {key} => {
                            self.primitives.forget_publisher(key).await;
                        }
                        Declaration::ForgetSubscriber {key} => {
                            self.primitives.forget_subscriber(key).await;
                        }
                        Declaration::ForgetStorage {key} => {
                            self.primitives.forget_storage(key).await;
                        }
                        Declaration::ForgetEval {key} => {
                            self.primitives.forget_eval(key).await;
                        }
                    }

                }
            }
            Body::Data{reliable, key, info, payload, ..} => {
                match &msg.reply_context {
                    None => {self.primitives.data(key, *reliable, info, payload).await;}
                    Some(rep) => {self.primitives.reply(rep.qid, &rep.source, &rep.replier_id, key, info, payload).await;}
                }
            }
            Body::Query{key, predicate, qid, target, consolidation, ..} => {
                self.primitives.query(key, predicate, *qid, target, consolidation).await;
            }
            Body::Pull{key, pull_id, max_samples, ..} => {
                self.primitives.pull(flag::has_flag(msg.header, flag::F), key, *pull_id, max_samples).await;
            }
            _ => () 
        }

        Ok(())
    }

    async fn close(&self) {
        self.primitives.close().await;
    }
}