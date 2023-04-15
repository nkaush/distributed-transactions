use std::{collections::{BTreeMap, BTreeSet}, convert::Infallible};
use super::transaction_id::{Id, TransactionId};
use tx_common::config::NodeId;

pub trait Updateable {
    fn update(&mut self, other: &Self);
}

pub trait Diffable<D> where D: Updateable {
    type DiffError;
    fn diff(&mut self, diff: D) -> Result<(), Self::DiffError>;
}

#[derive(Debug)]
pub struct TentativeWrite<D> where D: Updateable {
    diff: D
}

pub struct TimestampedObject<T, D> where T: Diffable<D>, D: Updateable {
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

impl<T, D> TimestampedObject<T, D> where T: Diffable<D>, D: Updateable {
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

    pub fn read(&self, id: &TransactionId) -> Result<T, RWFailure> {
        // Transaction Tc requests a read operation on object D
        // if (Tc > write timestamp on committed version of D) {
        //     Ds = version of D with the maximum write timestamp that is ≤ Tc
        //     // search across the committed timestamp and the TW list for object D.
        //     if (Ds is committed)
        //         read Ds and add Tc to RTS list (if not already added)
        //     else
        //         if Ds was written by Tc, simply read Ds
        //         else
        //             wait until the transaction that wrote Ds is committed or aborted, and reapply the read rule.
        //             // if the transaction is committed, Tc will read its value after the wait.
        //             // if the transaction is aborted, Tc will read the value from an older transaction.
        // } else
        //     abort transaction Tc
        //     // too late; a transaction with later timestamp has already written the object. 
        todo!()
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
                .and_modify(|tw| tw.diff.update(&diff))
                .or_insert(TentativeWrite { diff } );

            Ok(())
        } else {
            // Too late! A transaction with a later timestamp has either already 
            // read or has already written to this object
            Err(RWFailure::Abort)
        }
    }

    pub fn commit(&mut self, id: &TransactionId) -> Result<(), CommitFailure<T::DiffError>> {
        if !self.tentative_writes.contains_key(id) {
            return Err(CommitFailure::InvalidId);
        }

        match self.tentative_writes.keys().next() {
            Some(first) => {
                if id == first {
                    // TODO: drain read timestamps that are less than committed timestamp???
                    let (ts, v) = self.tentative_writes
                        .remove_entry(id)
                        .unwrap();
                    
                    self.committed_timestamp = ts;
                    self.value
                        .diff(v.diff)
                        .map_err(|e| CommitFailure::ConsistencyCheckFailed(e))
                } else {
                    Err(CommitFailure::WaitFor(*first))
                }
            },
            None => return Err(CommitFailure::NoTransactionsToCommit)
        }
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
        type DiffError = ();
        fn diff(&mut self, diff: SignedDiff) -> Result<(), Self::DiffError> { 
            let SignedDiff(change) = diff;

            match self.checked_add(change) {
                Some(val) => { *self = val; Ok(()) },
                None => { Err(()) }
            }
        }
    }

    #[test]
    fn test_basic_write() {
        let mut object = TimestampedObject::new(0, 'A');
        let mut id_gen = TransactionIdGenerator::new('B');
        let tx = id_gen.next();

        // Basic write should be able to write with no conflicting transactions
        let write_res = object.write(SignedDiff(10), &tx);
        assert!(write_res.is_ok());

        let commit_res = object.commit(&tx);
        assert!(commit_res.is_ok());

        assert_eq!(object.value, 10);
        assert_eq!(object.committed_timestamp, tx);
    }

    #[test]
    fn test_basic_write_with_update() {
        let mut object = TimestampedObject::new(0, 'A');
        let mut id_gen = TransactionIdGenerator::new('B');
        let tx = id_gen.next();

        // Basic write should be able to write with no conflicting transactions
        let write_res = object.write(SignedDiff(10), &tx);
        assert!(write_res.is_ok());

        // Basic write should be able to write again with no conflicting transactions
        let write_res = object.write(SignedDiff(20), &tx);
        assert!(write_res.is_ok());

        let commit_res = object.commit(&tx);
        assert!(commit_res.is_ok());

        assert_eq!(object.value, 30);
        assert_eq!(object.committed_timestamp, tx);
    }

    #[test]
    fn test_commit_stall() {
        let mut object = TimestampedObject::new(0, 'A');
        let mut id_gen = TransactionIdGenerator::new('B');
        let tx1 = id_gen.next();
        let tx2 = id_gen.next();

        // Older transaction writes first...
        let write_res = object.write(SignedDiff(10), &tx1);
        assert!(write_res.is_ok());

        // Newer transaction writes next...
        let write_res = object.write(SignedDiff(20), &tx2);
        assert!(write_res.is_ok());

        // Newer transaction must wait for older transaction to commit/abort
        let commit_res = object.commit(&tx2);
        assert!(commit_res.is_err());
        assert_eq!(commit_res.unwrap_err(), CommitFailure::WaitFor(tx1));

        // Older transaction should be able to commit without failure
        let commit_res2 = object.commit(&tx1);
        assert!(commit_res2.is_ok());
        assert_eq!(object.value, 10);
        assert_eq!(object.committed_timestamp, tx1);

        // Newer transaction should be able to commit after older transaction
        let commit_res3 = object.commit(&tx2);
        assert!(commit_res3.is_ok());
        assert_eq!(object.value, 30);
        assert_eq!(object.committed_timestamp, tx2);
    }

    #[test]
    fn test_write_after_newer_commit() {
        let mut object = TimestampedObject::new(0, 'A');
        let mut id_gen = TransactionIdGenerator::new('B');
        let tx1 = id_gen.next();
        let tx2 = id_gen.next();

        // Newer write should succeed without any other writes present
        let write_res = object.write(SignedDiff(20), &tx2);
        assert!(write_res.is_ok());

        // Newer transaction should be able to commit since no older 
        // transactions have written to this object yet
        let commit_res = object.commit(&tx2);
        assert!(commit_res.is_ok());
        assert_eq!(object.value, 20);
        assert_eq!(object.committed_timestamp, tx2);

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
        let write_res = object.write(SignedDiff(20), &tx2);
        assert!(write_res.is_ok());

        // Older write should also succeed.
        let write_res = object.write(SignedDiff(10), &tx1);
        assert!(write_res.is_ok());

        // Older transaction should be able to commit
        let commit_res = object.commit(&tx1);
        assert!(commit_res.is_ok());
        assert_eq!(object.value, 10);
        assert_eq!(object.committed_timestamp, tx1);

        // Newer transaction should also be able to commit
        let commit_res = object.commit(&tx2);
        assert!(commit_res.is_ok());
        assert_eq!(object.value, 30);
        assert_eq!(object.committed_timestamp, tx2);
    }

    #[test]
    fn test_basic_abort() {
        let mut object = TimestampedObject::new(0, 'A');
        let mut id_gen = TransactionIdGenerator::new('B');
        let tx = id_gen.next();

        // Basic write should be able to write with no conflicting transactions
        let write_res = object.write(SignedDiff(10), &tx);
        assert!(write_res.is_ok());

        // Basic write should be able to write again with no conflicting transactions
        let write_res = object.write(SignedDiff(20), &tx);
        assert!(write_res.is_ok());

        // Abort the transaction
        let abort_res = object.abort(&tx);
        assert!(abort_res.is_ok());

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
        let write_res = object.write(SignedDiff(10), &tx1);
        assert!(write_res.is_ok());

        // Newer transaction writes next...
        let write_res = object.write(SignedDiff(20), &tx2);
        assert!(write_res.is_ok());

        // Abort the older transaction
        let abort_res = object.abort(&tx1);
        assert!(abort_res.is_ok());

        // Newer transaction should be able to commit after older transaction
        // was aborted, and the older transaction should not be applied.
        let commit_res3 = object.commit(&tx2);
        assert!(commit_res3.is_ok());
        assert_eq!(object.value, 20);
        assert_eq!(object.committed_timestamp, tx2);
    }

    #[test]
    fn test_basic_consistency_check_failure() {
        let mut object = TimestampedObject::new(i64::MAX - 1, 'A');
        let mut id_gen = TransactionIdGenerator::new('B');
        let tx = id_gen.next();

        // Basic write should be able to write with no conflicting transactions
        let write_res = object.write(SignedDiff(1), &tx);
        assert!(write_res.is_ok());

        // Another write should be able to write with no conflicting transactions
        let write_res = object.write(SignedDiff(1), &tx);
        assert!(write_res.is_ok());

        let commit_res = object.commit(&tx);
        assert!(commit_res.is_err());
        assert_eq!(commit_res.unwrap_err(), CommitFailure::ConsistencyCheckFailed(()));
    }

    #[test]
    fn test_consistency_check_failure_with_future_commit() {
        let mut object = TimestampedObject::new(i64::MAX, 'A');
        let mut id_gen = TransactionIdGenerator::new('B');
        let tx1 = id_gen.next();
        let tx2 = id_gen.next();

        // Write the diff that will make the consistency check fail
        let write_res = object.write(SignedDiff(1), &tx1);
        assert!(write_res.is_ok());

        // Different tx makes a write that passes the consistency check
        let write_res = object.write(SignedDiff(-1), &tx2);
        assert!(write_res.is_ok());

        let commit_res = object.commit(&tx1);
        assert!(commit_res.is_err());
        assert_eq!(commit_res.unwrap_err(), CommitFailure::ConsistencyCheckFailed(()));

        let commit_res = object.commit(&tx2);
        assert!(commit_res.is_ok());
        assert_eq!(object.value, i64::MAX - 1);
        assert_eq!(object.committed_timestamp, tx2);
    }
}