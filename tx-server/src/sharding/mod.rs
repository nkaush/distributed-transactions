mod transaction_id;
mod shard;
mod object;

pub use transaction_id::{TransactionIdGenerator, TransactionId};
pub use shard::{Abort, Shard};
pub use object::CommitSuccess;

pub trait Updateable {
    fn update(&mut self, other: &Self);
}

pub trait Diffable<D> 
where 
    D: Updateable 
{
    type ConsistencyCheckError: std::fmt::Debug + Send;
    
    fn diff(&self, diff: &D) -> Self;
    fn check(self) -> Result<Self, Self::ConsistencyCheckError> where Self: Sized;
}