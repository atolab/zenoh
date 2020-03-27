use async_trait::async_trait;
use crate::zerror; 
use crate::core::{ZError, ZErrorKind};
use crate::proto::{Message, Body, Declaration, Primitives, Reply, flag};
use crate::session::MsgHandler;

#[cfg(demuxtraces)]
macro_rules! trace { ($($arg:tt)*) => (println!($($arg)*)); }
#[cfg(not(demuxtraces))]
macro_rules! trace { ($($arg:tt)*) => (); }

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
                            trace!("DECLARE RESOURCE rid({:?}) key({:?})", rid, key);
                            self.primitives.resource(*rid, key).await;
                        }
                        Declaration::Publisher {key} => {
                            trace!("DECLARE PUBLISHER key({:?})", key);
                            self.primitives.publisher(key).await;
                        }
                        Declaration::Subscriber {key, info} => {
                            trace!("DECLARE SUBSCRIBER key({:?}) info({:?})", key, info);
                            self.primitives.subscriber(key, info).await;
                        }
                        Declaration::Storage {key} => {
                            trace!("DECLARE STORAGE key({:?})", key);
                            self.primitives.storage(key).await;
                        }
                        Declaration::Eval {key} => {
                            trace!("DECLARE EVAL key({:?})", key);
                            self.primitives.eval(key).await;
                        }
                        Declaration::ForgetResource {rid} => {
                            trace!("FORGET RESOURCE rid({:?})", rid);
                            self.primitives.forget_resource(*rid).await;
                        }
                        Declaration::ForgetPublisher {key} => {
                            trace!("FORGET PUBLISHER key({:?})", key);
                            self.primitives.forget_publisher(key).await;
                        }
                        Declaration::ForgetSubscriber {key} => {
                            trace!("FORGET SUBSCRIBER key({:?})", key);
                            self.primitives.forget_subscriber(key).await;
                        }
                        Declaration::ForgetStorage {key} => {
                            trace!("FORGET STORAGE key({:?})", key);
                            self.primitives.forget_storage(key).await;
                        }
                        Declaration::ForgetEval {key} => {
                            trace!("FORGET EVAL key({:?})", key);
                            self.primitives.forget_eval(key).await;
                        }
                    }

                }
            }
            Body::Data{reliable, key, info, payload, ..} => {
                match &msg.reply_context {
                    None => {
                        trace!("DATA key({:?}) relibale({:?})", key, reliable);
                        self.primitives.data(key, *reliable, info, payload.clone()).await;
                    }
                    Some(rep) => {
                        match &rep.replier_id {
                            Some(replier_id) => {
                                let reply = Reply::ReplyData {source: rep.source.clone(), replier_id: replier_id.clone(), reskey: key.clone(), info: info.clone(), payload: payload.clone()};
                                trace!("REPLY qid({:?}) reply({:?})", rep.qid, &reply);
                                self.primitives.reply(rep.qid, &reply).await}
                            None => return Err(zerror!(ZErrorKind::Other{descr: "ReplyData with no replier_id".to_string()}))
                        }
                    }
                }
            }
            Body::Query{key, predicate, qid, target, consolidation, ..} => {
                trace!("QUERY key({:?}) predicate({:?}) qid({:?}) target({:?}) consolidation({:?})", key, predicate, *qid, target, consolidation);
                self.primitives.query(key, predicate, *qid, target.clone().unwrap_or_default(), consolidation.clone()).await;
            }
            Body::Pull{key, pull_id, max_samples, ..} => {
                trace!("PULL is_final({:?}) key({:?}) pull_id({:?}) max_samples({:?})", flag::has_flag(msg.header, flag::F), key, *pull_id, max_samples);
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