pub mod coordinator;
pub mod sharding;
pub mod pool;

use sharding::Checkable;
pub use tx_common::BalanceDiff;

#[derive(Debug, Clone)]
pub struct NegativeBalance(tx_common::Amount);

impl Checkable for tx_common::Amount {
    #[cfg(test)]
    type ConsistencyCheckError = ();

    #[cfg(not(test))]
    type ConsistencyCheckError = NegativeBalance;

    #[cfg(test)]
    fn check(&self) -> Result<(), Self::ConsistencyCheckError> {
        if self >= &0 {
            Ok(())
        } else {
            Err(())
        }
    }

    #[cfg(not(test))]
    fn check(&self) -> Result<(), Self::ConsistencyCheckError> {
        if self >= &0 {
            Ok(())
        } else {
            Err(NegativeBalance(*self))
        }
    }
}