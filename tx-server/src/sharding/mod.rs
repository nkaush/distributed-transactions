pub mod transaction_id;
pub mod object;
pub mod shard;

#[cfg(test)]
mod test {
    use crate::sharding::object::{Diffable, Updateable};

    #[derive(Debug, Clone)]
    pub struct SignedDiff(pub i64);

    impl Updateable for SignedDiff {
        fn update(&mut self, other: &Self) {
            let SignedDiff(inner) = self;
            let SignedDiff(other) = other;

            *inner += other;
        }
    }

    impl Diffable<SignedDiff> for i64 {
        type ConsistencyCheckError = ();
        fn diff(&self, diff: &SignedDiff) -> Self { 
            let SignedDiff(change) = diff;
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
}