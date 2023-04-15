use tx_common::config::NodeId;
use std::time::SystemTime;

pub trait Id: Copy {
    fn default(coordinator: NodeId) -> Self;
}

pub trait IdGen {
    type Timestamp: Id;

    fn new(node_id: NodeId) -> Self;
    fn next(&mut self) -> Self::Timestamp;
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
    node_id: NodeId
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
    type Timestamp = ClockTransactionId;

    fn new(node_id: NodeId) -> Self {
        Self { node_id }
    }

    fn next(&mut self) -> Self::Timestamp {
        Self::Timestamp { ts: Self::get_system_time(), coordinator: self.node_id }
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
    type Timestamp = CounterTransactionId;

    fn new(node_id: NodeId) -> Self {
        Self { node_id, counter: 0 }
    }

    fn next(&mut self) -> Self::Timestamp {
        self.counter += 1;
        Self::Timestamp { ts: self.counter, coordinator: self.node_id }
    }
}

pub type TransactionIdGenerator = CounterTransactionIdGenerator;
pub type TransactionId = <TransactionIdGenerator as IdGen>::Timestamp;