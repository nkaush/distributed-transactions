use crate::sharding::{object::{Diffable, Updateable}, shard::Shard};
use tx_common::{Amount, config::NodeId};
use crate::BalanceDiff;
use std::sync::Arc;

pub struct Server {
    shard: Arc<Shard<String, Amount, BalanceDiff>>
}

impl Server {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            shard: Arc::new(Shard::new(node_id))
        }
    }
}