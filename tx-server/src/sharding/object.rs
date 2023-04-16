use std::{
    collections::{BTreeMap, BTreeSet}, 
    ops::Bound::{Excluded, Included},
    convert::Infallible
};
use super::transaction_id::{Id, TransactionId};
use tx_common::config::NodeId;

pub trait Updateable {
    fn update(&mut self, other: &Self);
}

pub trait Diffable<D> 
where 
    D: Updateable 
{
    type ConsistencyCheckError;
    fn diff(&self, diff: &D) -> Self;
    fn check(&self) -> Result<(), Self::ConsistencyCheckError>;
}

#[derive(Debug)]
pub struct TentativeWrite<D> 
where 
    D: Updateable 
{
    diff: D,
}

impl<D> TentativeWrite<D> 
where 
    D: Updateable 
{
    fn new(diff: D) -> Self {
        Self { diff }
    }

    fn update(&mut self, diff: &D) {
        self.diff.update(diff);
    }
}

pub struct TimestampedObject<T, D> 
where 
    T: Diffable<D>, 
    D: Updateable 
{
    value: T,
    committed_timestamp: TransactionId,
    read_timestamps: BTreeSet<TransactionId>,
    tentative_writes: BTreeMap<TransactionId, TentativeWrite<D>>
}

#[derive(Debug, PartialEq, Eq)]
pub enum RWFailure {
    WaitFor(TransactionId),
    Abort
}

#[derive(Debug, PartialEq, Eq)]
pub enum CommitFailure<E> {
    WaitFor(TransactionId),
    ConsistencyCheckFailed(E),
    NoTransactionsToCommit,
    InvalidId
}

impl<T, D> TimestampedObject<T, D> 
where 
    T: Diffable<D>, 
    D: Updateable 
{
    pub fn new(value: T, owner_id: NodeId) -> Self {
        Self {
            value,
            committed_timestamp: Id::default(owner_id),
            read_timestamps: BTreeSet::new(),
            tentative_writes: BTreeMap::new()
        }
    }

    pub fn default(owner_id: NodeId) -> Self where T: Default {
        Self {
            value: Default::default(),
            committed_timestamp: Id::default(owner_id),
            read_timestamps: BTreeSet::new(),
            tentative_writes: BTreeMap::new()
        }
    }

    pub fn read(&mut self, id: &TransactionId) -> Result<T, RWFailure> where T: Clone {
        if id > &self.committed_timestamp {
            // Get a range of timestamps starting from the committed timestamp
            // to the timestamp of the read request transaction, inclusive
            let ts_range = (Excluded(self.committed_timestamp), Included(*id));

            // Get the final timestamp of the range such that we have the 
            // version of the obkect with the maximum write timestamp less than 
            // or equal to the requested read timestamp
            let mut tw_range = self.tentative_writes.range(ts_range);
            let ts_lte_id = tw_range.next_back();

            match ts_lte_id {
                None => { 
                    // if the timestamp we found is the committed timestamp
                    // read Ds and add Tc to RTS list (if not already added)
                    self.read_timestamps.insert(*id);
                    Ok(self.value.clone())
                },
                Some((ts, tw)) => {
                    if ts == id { // if Ds was written by Tc, simply read Ds
                        Ok(self.value.diff(&tw.diff))
                    } else {
                        // Wait until the transaction that wrote Ds is committed 
                        // or aborted, and reapply the read rule. If the 
                        // transaction is committed, Tc will read its value 
                        // after the wait. If the transaction is aborted, Tc 
                        // will read the value from an older transaction.
                        match tw_range.next() {
                            Some((first, _)) => Err(RWFailure::WaitFor(*first)),
                            None => unreachable!()
                        }
                    }
                }
            }
        } else {
            // Too late! A transaction with a later timestamp has either already 
            // read or has already written to this object
            Err(RWFailure::Abort)
        }
    }

    pub fn write(&mut self, diff: D, id: &TransactionId) -> Result<(), RWFailure> {
        let is_after_mrt = self.read_timestamps
            .iter()
            .next_back()
            .map_or_else(|| true, |mrt| id >= mrt);

        // If the requesting transaction is OR is after the max read timestamp 
        // on the object AND is after the write timestamp on the committed 
        // version of the object, then perform a tentative write on the object
        if is_after_mrt && id > &self.committed_timestamp {
            // Modify the entry for the tentative write if the requesting 
            // transaction has already performed a tentative write. Otherwise,
            // insert a tentative write for the object for the transaction.
            self.tentative_writes
                .entry(*id)
                .and_modify(|tw| tw.update(&diff))
                .or_insert(TentativeWrite::new(diff));

            Ok(())
        } else {
            // Too late! A transaction with a later timestamp has either already 
            // read or has already written to this object
            Err(RWFailure::Abort)
        }
    }

    pub fn check_commit(&self, id: &TransactionId) -> Result<bool, CommitFailure<T::ConsistencyCheckError>> {
        if !self.tentative_writes.contains_key(id) {
            return Ok(false);
        }
        
        match self.tentative_writes.keys().next() {
            Some(first) => {
                if id == first {
                    // TODO: drain read timestamps that are less than committed timestamp???
                    let tw = self.tentative_writes                    
                        .get(id)
                        .unwrap();

                    self.value
                        .diff(&tw.diff)
                        .check()
                        .map(|_| true)
                        .map_err(|e| CommitFailure::ConsistencyCheckFailed(e))
                } else {
                    Err(CommitFailure::WaitFor(*first))
                }
            },
            None => unreachable!()
        }
    }

    pub fn commit(&mut self, id: &TransactionId) -> Result<(), CommitFailure<T::ConsistencyCheckError>> {
        self.check_commit(id)
            .map(|contains_entry| {
                if contains_entry {
                    let (ts, tw) = self.tentative_writes                    
                        .remove_entry(id)
                        .unwrap();
                    self.committed_timestamp = ts;
                    self.value = self.value.diff(&tw.diff);
                }     
        })
    }

    pub fn abort(&mut self, id: &TransactionId) -> Result<(), Infallible> {
        self.tentative_writes.remove(id);

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::super::transaction_id::*;
    use super::*;

    #[derive(Debug)]
    struct SignedDiff(i64);

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

        fn check(&self) -> Result<(), Self::ConsistencyCheckError> {
            if self >= &0 {
                Ok(())
            } else {
                Err(())
            }
        }
    }

    fn verify_check_commit_success(object: &TimestampedObject<i64, SignedDiff>, id: &TransactionId) {
        let check = object.check_commit(&id);
        assert!(check.is_ok());
        assert!(check.unwrap());
    }

    fn verify_check_commit_failure(object: &TimestampedObject<i64, SignedDiff>, id: &TransactionId, f: CommitFailure<()>) {
        let check = object.check_commit(&id);
        assert!(check.is_err());
        assert_eq!(check.unwrap_err(), f);
    }

    fn verify_commit_success(object: &mut TimestampedObject<i64, SignedDiff>, id: &TransactionId, expected: i64) {
        let commit_res = object.commit(&id);
        assert!(commit_res.is_ok());
        assert_eq!(object.value, expected);
        assert_eq!(&object.committed_timestamp, id);
    }

    fn verify_commit_failure(object: &mut TimestampedObject<i64, SignedDiff>, id: &TransactionId, f: CommitFailure<()>) {
        let original_value = object.value;
        let original_cts = object.committed_timestamp;

        let commit_res = object.commit(&id);
        assert!(commit_res.is_err());
        assert_eq!(commit_res.unwrap_err(), f);

        // Ensure that the object's committed value was not changed
        assert_eq!(object.value, original_value);
        assert_eq!(object.committed_timestamp, original_cts);
    }

    #[test]
    fn test_basic_write() {
        let mut object = TimestampedObject::new(0, 'A');
        let mut id_gen = TransactionIdGenerator::new('B');
        let tx = id_gen.next();

        // Basic write should be able to write with no conflicting transactions
        assert!(object.write(SignedDiff(10), &tx).is_ok());

        verify_check_commit_success(&object, &tx);
        verify_commit_success(&mut object, &tx, 10);
    }

    #[test]
    fn test_basic_write_with_update() {
        let mut object = TimestampedObject::new(0, 'A');
        let mut id_gen = TransactionIdGenerator::new('B');
        let tx = id_gen.next();

        // Basic write should be able to write with no conflicting transactions
        assert!(object.write(SignedDiff(10), &tx).is_ok());

        // Basic write should be able to write again with no conflicting transactions
        assert!(object.write(SignedDiff(20), &tx).is_ok());

        verify_check_commit_success(&object, &tx);
        verify_commit_success(&mut object, &tx, 30);
    }

    #[test]
    fn test_commit_stall() {
        let mut object = TimestampedObject::new(0, 'A');
        let mut id_gen = TransactionIdGenerator::new('B');
        let tx1 = id_gen.next();
        let tx2 = id_gen.next();

        // Older transaction writes first...
        assert!(object.write(SignedDiff(10), &tx1).is_ok());

        // Newer transaction writes next...
        assert!(object.write(SignedDiff(20), &tx2).is_ok());

        // Newer transaction must wait for older transaction to commit/abort
        verify_check_commit_failure(&object, &tx2, CommitFailure::WaitFor(tx1));
        verify_commit_failure(&mut object, &tx2, CommitFailure::WaitFor(tx1));

        // Older transaction should be able to commit without failure
        verify_check_commit_success(&object, &tx1);
        verify_commit_success(&mut object, &tx1, 10);

        // Newer transaction should be able to commit after older transaction
        verify_check_commit_success(&object, &tx2);
        verify_commit_success(&mut object, &tx2, 30);
    }

    #[test]
    fn test_write_after_newer_commit() {
        let mut object = TimestampedObject::new(0, 'A');
        let mut id_gen = TransactionIdGenerator::new('B');
        let tx1 = id_gen.next();
        let tx2 = id_gen.next();

        // Newer write should succeed without any other writes present
        assert!(object.write(SignedDiff(20), &tx2).is_ok());

        // Newer transaction should be able to commit since no older 
        // transactions have written to this object yet
        verify_check_commit_success(&object, &tx2);
        verify_commit_success(&mut object, &tx2, 20);

        // Older transaction should not be able to write since a newer 
        // transaction has written and committed a value
        let write_res = object.write(SignedDiff(10), &tx1);
        assert!(write_res.is_err());
        assert_eq!(write_res.unwrap_err(), RWFailure::Abort);
    }
    
    #[test]
    fn test_newer_transaction_writes_first() {
        let mut object = TimestampedObject::new(0, 'A');
        let mut id_gen = TransactionIdGenerator::new('B');
        let tx1 = id_gen.next();
        let tx2 = id_gen.next();

        // Newer write should succeed without any other writes present
        assert!(object.write(SignedDiff(20), &tx2).is_ok());

        // Older write should also succeed.
        assert!(object.write(SignedDiff(10), &tx1).is_ok());

        // Older transaction should be able to commit
        verify_check_commit_success(&object, &tx1);
        verify_commit_success(&mut object, &tx1, 10);

        // Newer transaction should also be able to commit
        verify_check_commit_success(&object, &tx2);
        verify_commit_success(&mut object, &tx2, 30);
    }

    #[test]
    fn test_basic_abort() {
        let mut object = TimestampedObject::new(0, 'A');
        let mut id_gen = TransactionIdGenerator::new('B');
        let tx = id_gen.next();

        // Basic write should be able to write with no conflicting transactions
        assert!(object.write(SignedDiff(10), &tx).is_ok());

        // Basic write should be able to write again with no conflicting transactions
        assert!(object.write(SignedDiff(20), &tx).is_ok());

        // Abort the transaction
        assert!(object.abort(&tx).is_ok());

        // Ensure that no updates have been made to the object
        assert_eq!(object.value, 0);
        assert_eq!(object.committed_timestamp, Id::default('A'));
    }

    #[test]
    fn test_aborted_transaction_with_future_commits() {
        let mut object = TimestampedObject::new(0, 'A');
        let mut id_gen = TransactionIdGenerator::new('B');
        let tx1 = id_gen.next();
        let tx2 = id_gen.next();

        // Older transaction writes first...
        assert!(object.write(SignedDiff(10), &tx1).is_ok());

        // Newer transaction writes next...
        assert!(object.write(SignedDiff(20), &tx2).is_ok());

        // Abort the older transaction
        assert!(object.abort(&tx1).is_ok());
        
        // Newer transaction should be able to commit after older transaction
        // was aborted, and the older transaction should not be applied.
        verify_check_commit_success(&object, &tx2);
        verify_commit_success(&mut object, &tx2, 20);
    }

    #[test]
    fn test_basic_consistency_check_failure() {
        let mut object = TimestampedObject::new(0, 'A');
        let mut id_gen = TransactionIdGenerator::new('B');
        let tx = id_gen.next();

        // Basic write should be able to write with no conflicting transactions
        assert!(object.write(SignedDiff(1), &tx).is_ok());

        // Another write should be able to write with no conflicting transactions
        assert!(object.write(SignedDiff(-10), &tx).is_ok());

        verify_check_commit_failure(&object, &tx, CommitFailure::ConsistencyCheckFailed(()));
        verify_commit_failure(&mut object, &tx, CommitFailure::ConsistencyCheckFailed(()));
    }

    #[test]
    fn test_consistency_check_failure_with_future_commit() {
        let mut object = TimestampedObject::new(0, 'A');
        let mut id_gen = TransactionIdGenerator::new('B');
        let tx1 = id_gen.next();
        let tx2 = id_gen.next();

        // Write the diff that will make the consistency check fail
        assert!(object.write(SignedDiff(-10), &tx1).is_ok());

        // Different tx makes a write that passes the consistency check
        assert!(object.write(SignedDiff(10), &tx2).is_ok());

        verify_check_commit_failure(&object, &tx1, CommitFailure::ConsistencyCheckFailed(()));
        verify_commit_failure(&mut object, &tx1, CommitFailure::ConsistencyCheckFailed(()));

        assert!(object.abort(&tx1).is_ok());

        verify_check_commit_success(&object, &tx2);
        verify_commit_success(&mut object, &tx2, 10);
    }
}