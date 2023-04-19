use std::fmt::{Display, Formatter};
use tx_common::config::NodeId;
use std::time::SystemTime;
use std::hash::Hash;

pub trait Id where Self: Copy + Hash + Display {
    fn is_default(&self) -> bool;
    fn default(coordinator: NodeId) -> Self;
}

pub trait IdGen {
    type TxId: Id;

    fn new(node_id: NodeId) -> Self;
    fn next(&mut self) -> Self::TxId;
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct ClockTransactionId {
    ts: u128,
    coordinator: NodeId
}

impl Display for ClockTransactionId {
    fn fmt(&self, fmt: &mut Formatter<'_>) -> Result<(), std::fmt::Error> { 
        fmt.write_str(&format!("ID({}, {})", self.ts, self.coordinator))
    }
}

impl Id for ClockTransactionId {
    fn is_default(&self) -> bool {
        self.ts == 0
    }

    fn default(coordinator: NodeId) -> Self {
        Self { ts: 0, coordinator }
    }
}

pub struct ClockTransactionIdGenerator {
    node_id: NodeId,
    last_systime: u128,
    last_count: usize
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
        Self { node_id, last_systime: 0, last_count: 0 }
    }

    fn next(&mut self) -> Self::TxId {
        let mut ts = Self::get_system_time();
        if ts <= self.last_systime {
            self.last_count += 1;
            ts = self.last_systime + self.last_count as u128;
        } else {
            self.last_count = 0;
            self.last_systime = ts;
        }
        
        Self::TxId { ts, coordinator: self.node_id }
    }
}

pub type TransactionIdGenerator = ClockTransactionIdGenerator;
pub type TransactionId = <TransactionIdGenerator as IdGen>::TxId;

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_unique_id_generation() {
        let mut id_gen = TransactionIdGenerator::new('A');

        for _ in 0..100 {
            let id1 = id_gen.next();
            let id2 = id_gen.next();
            let id3 = id_gen.next();
            let id4 = id_gen.next();

            assert!(id1 < id2);
            assert!(id2 < id3);
            assert!(id3 < id4);
        }
    }

    #[test]
    fn test_unique_id_generation_different_nodes() {
        let mut id_gen_a = TransactionIdGenerator::new('A');
        let mut id_gen_b = TransactionIdGenerator::new('B');

        for _ in 0..100 {
            let id1 = id_gen_a.next();
            let id2 = id_gen_b.next();
            let id3 = id_gen_a.next();
            let id4 = id_gen_b.next();
    
            assert!(id1 < id2);
            assert!(id1 < id3);
            assert!(id2 < id4);
            assert!(id3 < id4);
        }
    }
}