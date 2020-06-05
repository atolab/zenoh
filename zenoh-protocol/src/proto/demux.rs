use async_trait::async_trait;

use crate::proto::{ZenohMessage, ZenohBody, Declaration, Primitives, Reply, zmsg};
use crate::session::MsgHandler;
use zenoh_util::zerror;
use zenoh_util::core::{ZResult, ZError, ZErrorKind};

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

    async fn handle_message(&self, msg: ZenohMessage) -> ZResult<()> {
        match msg.get_body() {
            ZenohBody::Declare{ declarations, .. } => {
                for declaration in declarations {
                    match declaration {
                        Declaration::Resource { rid, key } => {
                            self.primitives.resource(*rid, key).await;
                        }
                        Declaration::Publisher { key } => {
                            self.primitives.publisher(key).await;
                        }
                        Declaration::Subscriber { key, info } => {
                            self.primitives.subscriber(key, info).await;
                        }
                        Declaration::Queryable { key } => {
                            self.primitives.queryable(key).await;
                        }
                        Declaration::ForgetResource { rid } => {
                            self.primitives.forget_resource(*rid).await;
                        }
                        Declaration::ForgetPublisher { key } => {
                            self.primitives.forget_publisher(key).await;
                        }
                        Declaration::ForgetSubscriber { key } => {
                            self.primitives.forget_subscriber(key).await;
                        }
                        Declaration::ForgetQueryable { key } => {
                            self.primitives.forget_queryable(key).await;
                        }
                    }

                }
            },
            
            ZenohBody::Data { key, info, payload, .. } => {
                match &msg.reply_context {
                    None => {
                        self.primitives.data(key, msg.is_reliable(), info, payload.clone()).await;
                    }
                    Some(rep) => {
                        match &rep.replier_id {
                            Some(replier_id) => {
                                let reply = Reply::ReplyData {source_kind: rep.source_kind, replier_id: replier_id.clone(), reskey: key.clone(), info: info.clone(), payload: payload.clone()};
                                self.primitives.reply(rep.qid, &reply).await}
                            None => return zerror!(ZErrorKind::Other {descr: "ReplyData with no replier_id".to_string()})
                        }
                    }
                }
            },

            ZenohBody::Unit { .. } => {
                if let Some(rep) = &msg.reply_context {
                    if rep.is_final {
                        let reply = Reply::ReplyFinal {};
                        self.primitives.reply(rep.qid, &reply).await
                    } else {
                        let reply = Reply::SourceFinal {source_kind: rep.source_kind, replier_id: rep.replier_id.clone().unwrap()};
                        self.primitives.reply(rep.qid, &reply).await
                    }
                }
            },

            ZenohBody::Query{ key, predicate, qid, target, consolidation, .. } => {
                self.primitives.query(key, predicate, *qid, target.clone().unwrap_or_default(), consolidation.clone()).await;
            },

            ZenohBody::Pull{ key, pull_id, max_samples, .. } => {
                self.primitives.pull(zmsg::has_flag(msg.header, zmsg::flag::F), key, *pull_id, max_samples).await;
            }
        }

        Ok(())
    }

    async fn close(&self) {
        self.primitives.close().await;
    }
}