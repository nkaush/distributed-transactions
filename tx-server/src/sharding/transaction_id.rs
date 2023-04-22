use std::fmt::{Display, Formatter};
use serde::{Deserialize, Serialize};
use tx_common::config::NodeId;
use std::time::SystemTime;
use std::hash::Hash;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialOrd, PartialEq, Serialize)]
pub struct ClockTransactionId {
    ts: u128,
    coordinator: NodeId
}

impl Display for ClockTransactionId {
    fn fmt(&self, fmt: &mut Formatter<'_>) -> Result<(), std::fmt::Error> { 
        fmt.write_str(&format!("ID({}, {})", self.ts, self.coordinator))
    }
}

impl ClockTransactionId {
    pub fn is_default(&self) -> bool {
        self.ts == 0
    }

    pub fn default(coordinator: NodeId) -> Self {
        Self { ts: 0, coordinator }
    }
}

pub struct ClockTransactionIdGenerator {
    node_id: NodeId,
    last_systime: u128,
    last_count: usize
}

impl ClockTransactionIdGenerator {
    pub fn new(node_id: NodeId) -> Self {
        Self { node_id, last_systime: 0, last_count: 0 }
    }

    fn get_system_time() -> u128 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Clock may have gone backwards")
            .as_nanos()
    }

    pub fn next(&mut self) -> ClockTransactionId {
        let mut ts = Self::get_system_time();
        if ts <= self.last_systime {
            self.last_count += 1;
            ts = self.last_systime + self.last_count as u128;
        } else {
            self.last_count = 0;
            self.last_systime = ts;
        }
        
        ClockTransactionId { ts, coordinator: self.node_id }
    }
}

pub type TransactionIdGenerator = ClockTransactionIdGenerator;
pub type TransactionId = ClockTransactionId;

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