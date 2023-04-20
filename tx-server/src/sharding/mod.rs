pub mod transaction_id;
pub mod shard;
mod object;

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