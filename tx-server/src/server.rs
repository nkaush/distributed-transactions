use crate::sharding::object::{Diffable, Updateable};
use crate::sharding::shard::Shard;
use tx_common::Balance;
use std::sync::Arc;

struct BalanceDiff(i64);

impl Updateable for BalanceDiff {
    fn update(&mut self, other: &Self) {
        let BalanceDiff(inner) = self;
        let BalanceDiff(other) = other;

        *inner += other;
    }
}

impl Diffable<BalanceDiff> for i64 {
    type ConsistencyCheckError = ();
    fn diff(&self, diff: &BalanceDiff) -> Self { 
        let BalanceDiff(change) = diff;
        self + change
    }

    fn check(self) -> Result<Self, Self::ConsistencyCheckError> {
        if self >= 0 {
            Ok(self)
        } else {
            Err(())
        }
    }
}

pub struct Server {
    shard: Arc<Shard<String, Balance, BalanceDiff>>
}