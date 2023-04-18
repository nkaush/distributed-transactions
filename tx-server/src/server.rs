use crate::sharding::object::{Diffable, Updateable};
use crate::sharding::shard::Shard;
use crate::BalanceDiff;
use tx_common::Amount;
use tx_common::config::NodeId;
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