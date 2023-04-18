use std::{collections::HashMap, hash::Hash, sync::Arc, convert::Infallible};
use crate::sharding::{object::*, transaction_id::TransactionId};
use tx_common::config::NodeId;
use futures::lock::Mutex;
use tokio::sync::Notify;
use log::{trace, error};

#[derive(Default, Debug)]
pub struct Abort {}

pub struct Shard<K, T, D> 
where 
    K: Hash + Eq, 
    T: Diffable<D>, 
    D: Updateable 
{
    shard_id: NodeId, 

    // A collection of all the objects that this shard manages
    objects: Mutex<HashMap<K, Arc<Mutex<TimestampedObject<T, D>>>>>,

    // A collection of notifications that are triggered when transactions are
    // resolved. These notifications wake up other operations waiting on pending 
    // transactions to resolve. 
    notifications: Mutex<HashMap<TransactionId, Arc<Notify>>>
}

impl<K, T, D> Shard<K, T, D>
where 
    K: Clone + Eq + Hash, 
    T: Clone + Default + Diffable<D>, 
    D: Updateable 
{
    pub fn new(shard_id: NodeId) -> Self {
        Self {
            shard_id,
            objects: Default::default(),
            notifications: Default::default()
        }
    }

    async fn get_object(&self, object_id: K) -> Arc<Mutex<TimestampedObject<T, D>>> {
        self.objects
            .lock()
            .await
            .entry(object_id)
            .or_insert(Arc::new(Mutex::new(TimestampedObject::default(self.shard_id))))
            .clone()
    }

    async fn get_notification(&self, id: &TransactionId) -> Arc<Notify> {
        self.notifications
            .lock()
            .await
            .entry(id.clone())
            .or_insert(Arc::new(Notify::new()))
            .clone()
    }

    async fn notify_and_remove(&self, id: &TransactionId) {
        self.notifications
            .lock()
            .await
            .remove(id)
            .and_then(|notify| Some(notify.notify_waiters()));
    }

    pub async fn read(&self, id: &TransactionId, object_id: K) -> Result<T, Abort> where T: Clone, K: std::fmt::Debug {
        let obj_id_fmt = format!("{object_id:?}");
        trace!("read(id={id:?}, object_id={object_id:?})");
        let obj = self.get_object(object_id).await;

        loop {
            let mut guard = obj.lock().await;
            match guard.read(id) {
                Ok(value) => return Ok(value),
                Err(RWFailure::Abort) => return Err(Abort::default()),
                Err(RWFailure::WaitFor(waiting_on)) => {
                    drop(guard);
                    trace!("check_commit(id={id:?}, object_id={obj_id_fmt}) waiting on {waiting_on:?}");
                    self.get_notification(&waiting_on).await.notified().await;
                }
            }
        }
    }

    pub async fn write(&self, id: &TransactionId, object_id: K, diff: D) -> Result<(), Abort> where D: Clone, K: std::fmt::Debug {
        let obj_id_fmt = format!("{object_id:?}");
        trace!("write(id={id:?}, object_id={object_id:?})");
        let obj = self.get_object(object_id).await;

        loop {
            let mut guard = obj.lock().await;
            match guard.write(id, diff.clone()) {
                Ok(_) => return Ok(()),
                Err(RWFailure::Abort) => return Err(Abort::default()),
                Err(RWFailure::WaitFor(waiting_on)) => {
                    drop(guard);
                    trace!("check_commit(id={id:?}, object_id={obj_id_fmt}) waiting on {waiting_on:?}");
                    self.get_notification(&waiting_on).await.notified().await;
                }
            }
        }
    }

    pub async fn check_commit(&self, id: &TransactionId, object_id: K) -> Result<(), Abort> where K: std::fmt::Debug {
        let obj_id_fmt = format!("{object_id:?}");
        trace!("check_commit(id={id:?}, object_id={object_id:?})");
        let obj = self.get_object(object_id).await;

        loop {
            let guard = obj.lock().await;
            match guard.check_commit(id) {
                Ok(_) => return Ok(()),
                Err(CommitFailure::ConsistencyCheckFailed(_)) => return Err(Abort::default()),
                Err(CommitFailure::WaitFor(waiting_on)) => {
                    drop(guard);
                    trace!("check_commit(id={id:?}, object_id={obj_id_fmt}) waiting on {waiting_on:?}");
                    self.get_notification(&waiting_on).await.notified().await;
                }
            }
        }
    }

    pub async fn commit(&self, id: &TransactionId, object_id: K) -> Result<T, Abort> where K: std::fmt::Debug {
        let obj_id_fmt = format!("{object_id:?}");
        trace!("commit(id={id:?}, object_id={obj_id_fmt})");
        let obj = self.get_object(object_id).await;

        loop {
            let mut guard = obj.lock().await;
            match guard.commit(id) {
                Ok(v) => {
                    self.notify_and_remove(id).await;
                    return Ok(v)
                },
                Err(CommitFailure::ConsistencyCheckFailed(e)) => {
                    error!("commit(id={id:?}, object_id={obj_id_fmt}) getting aborted -- {e:?}");
                    self.notify_and_remove(id).await;
                    return Err(Abort::default())
                },
                Err(CommitFailure::WaitFor(waiting_on)) => {
                    drop(guard);
                    error!("commit(id={id:?}, object_id={obj_id_fmt}) looping -- waiting on {waiting_on:?}");
                    self.get_notification(&waiting_on).await.notified().await;
                }
            };
        }
    }

    pub async fn abort(&self, id: &TransactionId, object_id: K) -> Result<(), Infallible> where K: std::fmt::Debug {
        trace!("abort(id={id:?}, object_id={object_id:?})");
        let res = self.get_object(object_id)
            .await
            .lock()
            .await
            .abort(id);

        self.notify_and_remove(id).await;

        res
    }
}

#[cfg(test)]
mod test {
    use crate::sharding::{transaction_id::*};
    use std::time::{Duration, Instant};
    use tokio::time::sleep;
    use crate::BalanceDiff;
    use super::*;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    async fn verify_commit(shard: &Arc<Shard::<i32, i64, BalanceDiff>>, object: i32, id: &TransactionId, expected: i64) {
        assert!(shard.check_commit(&id, object).await.is_ok());
        let commit_res = shard.commit(&id, object).await;
        assert!(commit_res.is_ok());
        assert_eq!(commit_res.unwrap(), expected);
    }

    #[tokio::test]
    async fn test_basic_write_stall() {
        init();
        let shard: Arc<Shard::<i32, i64, BalanceDiff>> = Arc::new(Shard::new('A'));
        let mut id_gen = TransactionIdGenerator::new('B');
        let tx1 = id_gen.next();
        let tx2 = id_gen.next();

        assert!(shard.write(&tx1, 1, BalanceDiff(10)).await.is_ok());
        assert!(shard.write(&tx2, 1, BalanceDiff(20)).await.is_ok());

        let shard_clone2 = shard.clone();
        let join_tx2 = tokio::spawn(async move {
            verify_commit(&shard_clone2, 1, &tx2, 30).await;
            Instant::now()
        });

        let shard_clone1 = shard.clone();
        let join_tx1 = tokio::spawn(async move {
            sleep(Duration::from_millis(100)).await;
            verify_commit(&shard_clone1, 1, &tx1, 10).await;
            Instant::now()
        });

        assert!(join_tx1.await.unwrap() < join_tx2.await.unwrap());
    }

    #[tokio::test]
    async fn test_aborted_write_stall() {
        init();
        let shard: Arc<Shard::<i32, i64, BalanceDiff>> = Arc::new(Shard::new('A'));
        let mut id_gen = TransactionIdGenerator::new('B');
        let tx1 = id_gen.next();
        let tx2 = id_gen.next();

        assert!(shard.write(&tx1, 1, BalanceDiff(-10)).await.is_ok());
        assert!(shard.write(&tx2, 1, BalanceDiff(20)).await.is_ok());

        let shard_clone2 = shard.clone();
        let join_tx2 = tokio::spawn(async move {
            verify_commit(&shard_clone2, 1, &tx2, 20).await;
            Instant::now()
        });

        let shard_clone1 = shard.clone();
        let join_tx1 = tokio::spawn(async move {
            sleep(Duration::from_millis(100)).await;
            assert!(shard_clone1.check_commit(&tx1, 1).await.is_err());
            assert!(shard_clone1.abort(&tx1, 1).await.is_ok());
            Instant::now()
        });

        assert!(join_tx1.await.unwrap() < join_tx2.await.unwrap());
    }

    #[tokio::test]
    async fn test_read_after_non_committed_write() {
        init();
        let shard: Arc<Shard::<i32, i64, BalanceDiff>> = Arc::new(Shard::new('A'));
        let mut id_gen = TransactionIdGenerator::new('B');
        let tx1 = id_gen.next();
        let tx2 = id_gen.next();
        let tx3 = id_gen.next();

        assert!(shard.write(&tx1, 1, BalanceDiff(10)).await.is_ok());
        verify_commit(&shard, 1, &tx1, 10).await;

        assert!(shard.write(&tx2, 1, BalanceDiff(20)).await.is_ok());

        let shard_clone3 = shard.clone();
        let join_tx3 = tokio::spawn(async move {
            let read_res = shard_clone3.read(&tx3, 1).await;
            assert!(read_res.is_ok());
            assert_eq!(read_res.unwrap(), 30);

            Instant::now()
        });

        let shard_clone2 = shard.clone();
        let join_tx2 = tokio::spawn(async move {
            sleep(Duration::from_millis(100)).await;
            verify_commit(&shard_clone2, 1, &tx2, 30).await;
            Instant::now()
        });

        assert!(join_tx2.await.unwrap() < join_tx3.await.unwrap());
    }
}