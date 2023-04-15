use super::transaction_id::{Id, TransactionId};
use std::{collections::{BTreeMap, BTreeSet}, convert::Infallible};
use tx_common::config::NodeId;

pub trait Diffable<D> {
    fn diff(&mut self, diff: D);
}

pub struct TentativeWrite<D> {
    diff: D
}

pub struct TimestampedObject<T, D> where T: Diffable<D> {
    value: T,
    committed_ts: TransactionId,
    read_transaction_ids: BTreeSet<TransactionId>,
    tentative_writes: BTreeMap<TransactionId, TentativeWrite<D>>
}

#[derive(Debug)]
pub enum RWFailure {
    Wait(TransactionId),
    Abort
}

#[derive(Debug)]
pub enum CommitFailure {
    Wait(TransactionId),
    InvalidId
}

impl<T, D> TimestampedObject<T, D> where T: Diffable<D> {
    pub fn new(value: T, owner_id: NodeId) -> Self {
        Self {
            value,
            committed_ts: Id::default(owner_id),
            read_transaction_ids: BTreeSet::new(),
            tentative_writes: BTreeMap::new()
        }
    }

    pub fn default(owner_id: NodeId) -> Self where T: Default {
        Self {
            value: Default::default(),
            committed_ts: Id::default(owner_id),
            read_transaction_ids: BTreeSet::new(),
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

    pub fn write(&mut self, value: T, id: &TransactionId) -> Result<(), RWFailure> {
        // Transaction Tc requests a write operation on object D
        // if (Tc ≥ max. read timestamp on D && Tc > write timestamp on committed version of D)
        //     Perform a tentative write on D:
        //         If Tc already has an entry in the TW list for D, update it.
        //         Else, add Tc and its write value to the TW list.
        // else
        //     abort transaction Tc
        //     // too late; a transaction with later timestamp has already read or
        //     written the object.
        todo!()
    }

    pub fn commit(&mut self, id: &TransactionId) -> Result<(), CommitFailure> {
        match self.tentative_writes.keys().next() {
            Some(first) => {
                if id == first {
                    let (ts, v) = self.tentative_writes
                        .remove_entry(id)
                        .unwrap();
                    
                    self.committed_ts = ts;
                    self.value.diff(v.diff);

                    Ok(())
                } else {
                    Err(CommitFailure::Wait(*first))
                }
            },
            None => return Err(CommitFailure::InvalidId)
        }
    }

    pub fn abort(&mut self, id: &TransactionId) -> Result<(), Infallible> {
        self.tentative_writes.remove(id);

        Ok(())
    }
}
