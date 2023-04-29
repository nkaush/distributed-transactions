mod transaction_id;
mod shard;
mod object;

pub use transaction_id::{TransactionIdGenerator, TransactionId};
pub use shard::{Abort, Shard};
pub use object::CommitSuccess; 

pub trait Checkable {
    type ConsistencyCheckError: std::fmt::Debug + Send;
    
    fn check(&self) -> Result<(), Self::ConsistencyCheckError>;
}