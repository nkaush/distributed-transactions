use std::{collections::HashMap, hash::Hash, sync::Arc, convert::Infallible};
use crate::sharding::{object::*, transaction_id::TransactionId};
use futures::{future, lock::Mutex, stream::FuturesUnordered};
use super::{Diffable, Updateable};
use tx_common::config::NodeId;
use tokio::sync::Notify;
use log::{trace, error};

#[derive(Debug, Eq, PartialEq)]
pub enum Abort<K> {
    ConsistencyCheckFailed,
    OrderViolation,
    ObjectNotFound(K)
}

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
    K: 'static + Send + Clone + Eq + Hash, 
    T: 'static + Send + Clone + Default + Diffable<D>, 
    D: 'static + Send + Updateable
{
    pub fn new(shard_id: NodeId) -> Self {
        Self {
            shard_id,
            objects: Default::default(),
            notifications: Default::default()
        }
    }

    async fn get_object(&self, object_id: &K) -> Option<Arc<Mutex<TimestampedObject<T, D>>>> {
        self.objects
            .lock()
            .await
            .get(object_id)
            .map(Clone::clone)
    }

    async fn get_object_or_insert(&self, object_id: K) -> Arc<Mutex<TimestampedObject<T, D>>> {
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

    pub async fn read(&self, id: &TransactionId, object_id: K) -> Result<T, Abort<K>> where T: Clone, K: std::fmt::Debug {
        trace!("read(id={id}, object_id={object_id:?})");
        loop {
            let obj = match self.get_object(&object_id).await {
                Some(obj) => obj,
                None => {
                    trace!("ABORT read(id={id}, object_id={object_id:?}) -- object does not exist");
                    return Err(Abort::ObjectNotFound(object_id))
                }
            };
            let mut guard = obj.lock().await;

            match guard.read(id) {
                Ok(value) => {
                    trace!("read(id={id}, object_id={object_id:?}) DONE");
                    return Ok(value)
                },
                Err(RWFailure::Abort) => {
                    trace!("ABORT read(id={id}, object_id={object_id:?}) -- timestamp ordering violation");
                    return Err(Abort::OrderViolation)
                },
                Err(RWFailure::WaitFor(waiting_on)) => {
                    drop(guard);
                    trace!("read(id={id}, object_id={object_id:?}) waiting on {waiting_on}");
                    self.get_notification(&waiting_on).await.notified().await;
                }
            }
        }
    }

    pub async fn write(&self, id: &TransactionId, object_id: K, diff: D) -> Result<(), Abort<K>> where D: Clone, K: std::fmt::Debug {
        let obj_id_fmt = format!("{object_id:?}");
        trace!("write(id={id}, object_id={object_id:?})");

        loop {
            let obj: Arc<Mutex<TimestampedObject<T, D>>> = self.get_object_or_insert(object_id.clone()).await;
            let mut guard = obj.lock().await;
            match guard.write(id, diff.clone()) {
                Ok(_) => {
                    trace!("write(id={id}, object_id={obj_id_fmt}) DONE");
                    return Ok(())
                },
                Err(RWFailure::Abort) => {
                    trace!("ABORT write(id={id}, object_id={obj_id_fmt}) -- timestamp ordering violation");
                    return Err(Abort::OrderViolation)
                },
                Err(RWFailure::WaitFor(waiting_on)) => {
                    drop(guard);
                    trace!("write(id={id}, object_id={obj_id_fmt}) waiting on {waiting_on}");
                    self.get_notification(&waiting_on).await.notified().await;
                }
            }
        }
    }

    pub async fn check_commit(&self, id: &TransactionId) -> Result<(), Abort<K>> {
        trace!("check_commit(id={id})");
        loop {
            let map_guard = self.objects.lock().await;
            let tasks = map_guard
                .values()
                .map(Clone::clone)
                .map(|v| {
                    let tx = id.clone();
                    tokio::spawn(async move {
                        let obj = v.lock().await;
                        obj.check_commit(&tx)
                    }
                )}).collect::<FuturesUnordered<_>>();
            drop(map_guard);

            let mut wait = None;
            for fut in future::join_all(tasks).await.into_iter() {
                let commit_res = fut.unwrap();
                match commit_res {
                    Err(CommitFailure::ConsistencyCheckFailed(e)) => {
                        trace!("ABORT check_commit(id={id}) -- consistency check fail: {e:?}");
                        return Err(Abort::ConsistencyCheckFailed)
                    },
                    Err(CommitFailure::WaitFor(waiting_on)) => {
                        trace!("check_commit(id={id}) waiting on {waiting_on}");
                        wait = Some(waiting_on);
                        break
                    },
                    _ => ()
                }
            }

            match wait {
                Some(wait_on) => self.get_notification(&wait_on).await.notified().await,
                None => {
                    trace!("check_commit(id={id}) DONE");
                    return Ok(())
                }
            }
        }
    }

    pub async fn commit(&self, id: &TransactionId) -> Result<Vec<(K, T)>, Abort<K>> where K: std::fmt::Debug {
        trace!("commit(id={id})");
        loop {
            let map_guard = self.objects.lock().await;
            let tasks = map_guard
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .map(|(k, v)| {
                    let tx = id.clone();
                    tokio::spawn(async move {
                        let mut obj = v.lock().await;
                        (k, obj.commit(&tx))
                    }
                )}).collect::<FuturesUnordered<_>>();
            drop(map_guard);

            let mut wait = None;
            let mut result = Vec::new();
            for fut in future::join_all(tasks).await.into_iter() {
                let (key, commit_res) = fut.unwrap();
                match commit_res {
                    Ok(v) => result.push((key, v)),
                    Err(CommitFailure::ConsistencyCheckFailed(e)) => {
                        error!("SHOULD NOT BE HERE ... commit(id={id}, object_id={key:?}) getting aborted -- {e:?}");
                        self.notify_and_remove(id).await;
                        return Err(Abort::ConsistencyCheckFailed)
                    },
                    Err(CommitFailure::WaitFor(waiting_on)) => {
                        error!("SHOULD NOT BE HERE ... commit(id={id}, object_id={key:?}) looping -- waiting on {waiting_on}");
                        wait = Some(waiting_on);
                        break
                    }
                }
            }

            match wait {
                Some(wait_on) => self.get_notification(&wait_on).await.notified().await,
                None => {
                    trace!("commit(id={id}) DONE");
                    self.notify_and_remove(id).await;
                    return Ok(result)
                }
            }
        }
    }

    pub async fn abort(&self, id: &TransactionId) -> Result<(), Infallible> where K: std::fmt::Debug {
        trace!("abort({id})");

        let mut map_guard = self.objects.lock().await;
        let tasks = map_guard
            .iter()
            .map(|(k, v)| {
                let k = k.clone();
                let v = v.clone();
                let tx = id.clone();
                tokio::spawn(async move {
                    let mut obj = v.lock().await;
                    obj.abort(&tx).unwrap();

                    (k, obj.can_reap(&tx))
                }
            )}).collect::<FuturesUnordered<_>>();

        trace!("abort({id}) -- beginning reap");

        future::join_all(tasks)
            .await
            .into_iter()
            .map(Result::unwrap)
            .filter(|(_, can_reap)| *can_reap)
            .for_each(|(k, _)| { 
                trace!("Reaping {k:?}...");
                map_guard.remove(&k); 
            });

        drop(map_guard);
        trace!("abort({id}) -- reap finished");
        self.notify_and_remove(id).await;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{BalanceDiff, sharding::{transaction_id::*}};
    use tokio::{task::JoinHandle, time::sleep};
    use std::time::{Duration, Instant};
    use super::*;

    async fn verify_commit(shard: &Arc<Shard<i32, i64, BalanceDiff>>, id: &TransactionId, expected: Vec<(i32, i64)>) {
        assert!(shard.check_commit(&id).await.is_ok());
        let commit_res = shard.commit(&id).await;
        assert!(commit_res.is_ok());
        assert_eq!(commit_res.unwrap(), expected);
    }

    #[test_log::test(tokio::test(flavor="multi_thread", worker_threads=2))]
    async fn test_basic_write_stall() {
        let shard: Arc<Shard<i32, i64, BalanceDiff>> = Arc::new(Shard::new('A'));
        let mut id_gen = TransactionIdGenerator::new('B');
        let tx1 = id_gen.next();
        let tx2 = id_gen.next();

        // Perform 2 consecutive writes from different transactions
        assert!(shard.write(&tx1, 1, BalanceDiff(10)).await.is_ok());
        assert!(shard.write(&tx2, 1, BalanceDiff(20)).await.is_ok());

        let shard_clone2 = shard.clone();
        let join_tx2: JoinHandle<Instant> = tokio::spawn(async move {
            // Try to commit the newer transaction first. It should wait for the
            // second older transaction to commit, then will commit sucessfully.
            verify_commit(&shard_clone2, &tx2, vec![(1, 30)]).await;
            Instant::now()
        });

        let shard_clone1 = shard.clone();
        let join_tx1: JoinHandle<Instant> = tokio::spawn(async move {
            // Wait for a while then commit the older transaction. It should 
            // commit successfully on the first go.
            sleep(Duration::from_millis(100)).await;
            verify_commit(&shard_clone1, &tx1, vec![(1, 10)]).await;
            Instant::now()
        });

        assert!(join_tx1.await.unwrap() < join_tx2.await.unwrap());
    }

    #[test_log::test(tokio::test(flavor="multi_thread", worker_threads=2))]
    async fn test_aborted_write_stall() {
        let shard: Arc<Shard<i32, i64, BalanceDiff>> = Arc::new(Shard::new('A'));
        let mut id_gen = TransactionIdGenerator::new('B');
        let tx1 = id_gen.next();
        let tx2 = id_gen.next();

        assert!(shard.write(&tx1, 1, BalanceDiff(-10)).await.is_ok());
        assert!(shard.write(&tx2, 1, BalanceDiff(20)).await.is_ok());

        let shard_clone2 = shard.clone();
        let join_tx2: JoinHandle<Instant> = tokio::spawn(async move {
            verify_commit(&shard_clone2, &tx2, vec![(1, 20)]).await;
            Instant::now()
        });

        let shard_clone1 = shard.clone();
        let join_tx1: JoinHandle<Instant> = tokio::spawn(async move {
            sleep(Duration::from_millis(100)).await;
            assert!(shard_clone1.check_commit(&tx1).await.is_err());
            assert!(shard_clone1.abort(&tx1).await.is_ok());
            Instant::now()
        });

        assert!(join_tx1.await.unwrap() < join_tx2.await.unwrap());
    }

    #[test_log::test(tokio::test(flavor="multi_thread", worker_threads=2))]
    async fn test_read_after_non_committed_write() {
        let shard: Arc<Shard<i32, i64, BalanceDiff>> = Arc::new(Shard::new('A'));
        let mut id_gen = TransactionIdGenerator::new('B');
        let tx1 = id_gen.next();
        let tx2 = id_gen.next();
        let tx3 = id_gen.next();

        assert!(shard.write(&tx1, 1, BalanceDiff(10)).await.is_ok());
        verify_commit(&shard, &tx1, vec![(1, 10)]).await;

        assert!(shard.write(&tx2, 1, BalanceDiff(20)).await.is_ok());

        let shard_clone3 = shard.clone();
        let join_tx3: JoinHandle<Instant> = tokio::spawn(async move {
            let read_res = shard_clone3.read(&tx3, 1).await;
            assert!(read_res.is_ok());
            assert_eq!(read_res.unwrap(), 30);

            Instant::now()
        });

        let shard_clone2 = shard.clone();
        let join_tx2: JoinHandle<Instant> = tokio::spawn(async move {
            sleep(Duration::from_millis(100)).await;
            verify_commit(&shard_clone2, &tx2, vec![(1, 30)]).await;
            Instant::now()
        });

        assert!(join_tx2.await.unwrap() < join_tx3.await.unwrap());
    }

    #[test_log::test(tokio::test(flavor="multi_thread", worker_threads=2))]
    async fn test_read_after_aborted_write() {
        let shard: Arc<Shard<i32, i64, BalanceDiff>> = Arc::new(Shard::new('A'));
        let mut id_gen = TransactionIdGenerator::new('B');
        let tx1 = id_gen.next();
        let tx2 = id_gen.next();

        // Perform a write that will fail upon commit
        assert!(shard.write(&tx1, 1, BalanceDiff(-10)).await.is_ok());

        let shard_clone1 = shard.clone();
        let join_tx1: JoinHandle<Instant> = tokio::spawn(async move {
            // Wait for a while before trying to commit
            sleep(Duration::from_millis(100)).await;

            // Try to commit, but fail due to failed consistency check
            let check_res = shard_clone1.check_commit(&tx1).await;
            assert!(check_res.is_err());
            assert_eq!(check_res.unwrap_err(), Abort::ConsistencyCheckFailed);

            // Abort the transaction
            let abort_res = shard_clone1.abort(&tx1).await;
            assert!(abort_res.is_ok());

            Instant::now()
        });

        let shard_clone2 = shard.clone();
        let join_tx2: JoinHandle<Instant> = tokio::spawn(async move {
            // The first inner call to read will be blocked on tx1 then will wait
            // The next inner call to read will fail since the object has been
            // deleted after tx1 failed the consistency check
            let read_res = shard_clone2.read(&tx2, 1).await;
            assert!(read_res.is_err());
            assert_eq!(read_res.unwrap_err(), Abort::ObjectNotFound(1));

            Instant::now()
        });

        assert!(join_tx1.await.unwrap() < join_tx2.await.unwrap());
        let guard = shard.objects.lock().await;
        assert!(guard.is_empty());
    }
    
    #[test_log::test(tokio::test(flavor="multi_thread", worker_threads=2))]
    async fn test_write_after_aborted_write_and_read() {
        let shard: Arc<Shard<i32, i64, BalanceDiff>> = Arc::new(Shard::new('A'));
        let mut id_gen = TransactionIdGenerator::new('B');
        let tx1 = id_gen.next();
        let tx2 = id_gen.next();
        let tx3 = id_gen.next();

        // Write a value that will be rejected at the consistency check
        assert!(shard.write(&tx1, 1, BalanceDiff(-20)).await.is_ok());

        let shard_clone2 = shard.clone();
        let join_tx2 = tokio::spawn(async move {
            // Attempt to read the value from a newer transaction. This will 
            // wait until the older transaction has been resolved. The older 
            // transaction will be aborted, so this transaction must also be 
            // aborted since no other older transactions have written. 
            let read_res = shard_clone2.read(&tx2, 1).await;
            assert!(read_res.is_err());
            assert_eq!(read_res.unwrap_err(), Abort::OrderViolation);
            assert!(shard_clone2.abort(&tx1).await.is_ok());

            Instant::now()
        });

        // A newer transaction than the read should be able to write to the 
        // object despite the earlier aborted transactions
        assert!(shard.write(&tx3, 1, BalanceDiff(10)).await.is_ok());

        // Try to commit then abort the oldest transaction since it fails the 
        // consistency check upon attempting to commit
        let check_res = shard.check_commit(&tx1).await;
        assert!(check_res.is_err());
        assert_eq!(check_res.unwrap_err(), Abort::ConsistencyCheckFailed);
        assert!(shard.abort(&tx1).await.is_ok());

        // The read transaction should finish after the oldest transaction 
        // finishes since the read must wait for the oldest write to resolve
        assert!(Instant::now() < join_tx2.await.unwrap());

        // Verify that the newest write following the aborts will be committed
        verify_commit(&shard, &tx3, vec![(1, 10)]).await;
    }
}
