use tx_common::config::NodeId;
use std::time::SystemTime;

pub trait Id where Self: Copy {
    fn default(coordinator: NodeId) -> Self;
}

pub trait IdGen {
    type TxId: Id;

    fn new(node_id: NodeId) -> Self;
    fn next(&mut self) -> Self::TxId;
}

#[derive(Clone, Copy, Debug, PartialOrd, Ord, PartialEq, Eq)]
pub struct ClockTransactionId {
    ts: u128,
    coordinator: NodeId
}

impl Id for ClockTransactionId {
    fn default(coordinator: NodeId) -> Self {
        Self { ts: 0, coordinator }
    }
}

pub struct ClockTransactionIdGenerator {
    node_id: NodeId,
    last_systime: u128
}

impl ClockTransactionIdGenerator {
    fn get_system_time() -> u128 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Clock may have gone backwards")
            .as_nanos()
    }
}

impl IdGen for ClockTransactionIdGenerator {
    type TxId = ClockTransactionId;

    fn new(node_id: NodeId) -> Self {
        Self { node_id, last_systime: 0 }
    }

    fn next(&mut self) -> Self::TxId {
        let mut ts = Self::get_system_time();
        if ts == self.last_systime {
            ts += 1;  
        }
        
        self.last_systime = ts;
        Self::TxId { ts, coordinator: self.node_id }
    }
}

#[derive(Clone, Copy, Debug, PartialOrd, Ord, PartialEq, Eq)]
pub struct CounterTransactionId {
    ts: usize,
    coordinator: NodeId
}

impl Id for CounterTransactionId {
    fn default(coordinator: NodeId) -> Self {
        Self { ts: 0, coordinator }
    }
}

pub struct CounterTransactionIdGenerator {
    counter: usize,
    node_id: NodeId
}

impl IdGen for CounterTransactionIdGenerator {
    type TxId = CounterTransactionId;

    fn new(node_id: NodeId) -> Self {
        Self { node_id, counter: 0 }
    }

    fn next(&mut self) -> Self::TxId {
        self.counter += 1;
        Self::TxId { ts: self.counter, coordinator: self.node_id }
    }
}

pub type TransactionIdGenerator = ClockTransactionIdGenerator;
pub type TransactionId = <TransactionIdGenerator as IdGen>::TxId;