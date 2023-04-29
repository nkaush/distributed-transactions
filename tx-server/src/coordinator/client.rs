use tx_common::{
    ClientRequest, ClientResponse, AccountId,
    config::NodeId, stream::MessageStream
};
use super::{protocol::*, ServerHandle, AtomicShard, format_commit_result};
use crate::{sharding::{Abort, TransactionId}, BalanceDiff};
use tokio::{sync::mpsc::*, net::TcpStream};
use log::{error, info, trace};

/// This struct contains all the data that a client handler task uses to process
/// a transaction from a client. This struct contains data pertaining to the 
/// shard this server represents and channels for communicating with the client 
/// and with the main server task.
pub(super) struct Client {
    /// The NodeId of the shard this client handler is running on
    server_id: NodeId,
    /// The TransactionId associated with the client this task is handling
    transaction_id: TransactionId,
    /// A list of all the shards in the system so this client task can 
    /// coordinate with other shards in the case other shards own an object that 
    /// the client that this task is handling requests to read or write
    shard_ids: Vec<NodeId>,
    /// A TCP stream for communicating with the client this task is handling
    stream: MessageStream, 
    /// An atomic pointer to the shard on this server
    shard: AtomicShard,
    /// This channel is used to pass messages to the server task so that the 
    /// server task can forward them onto the associated shard
    forward_snd: UnboundedSender<ClientState>,
    /// The channel used for the server task to pass responses back to this task
    forward_rcv: UnboundedReceiver<ClientResponse>
}

impl Client {
    pub(super) fn new(server_handle: ServerHandle, stream: TcpStream, forward_rcv: UnboundedReceiver<ClientResponse>) -> Self {
        Client {
            shard: server_handle.shard,
            server_id: server_handle.server_id,
            transaction_id: server_handle.tx_id,
            shard_ids: server_handle.shard_ids,
            forward_snd: server_handle.forwarding_handle,
            stream: MessageStream::from_tcp_stream(stream),
            forward_rcv
        }
    }

    fn extract_shard(&self, acct: &AccountId) -> TargetShard {
        use TargetShard::*;

        acct.chars()
            .next()
            .map_or_else(|| DoesNotExist, |shard_id| {
                if self.shard_ids.contains(&shard_id) {
                    if self.server_id == shard_id {
                        Local
                    } else {
                        Remote(shard_id)
                    }
                } else {
                    DoesNotExist
                }
            })
    }

    async fn handle_balance_change_request(&mut self, account_id: AccountId, diff: BalanceDiff) -> Result<(), ()> {
        let account_id_fmt = format!("{account_id}");
        let resp: ClientResponse = match self.extract_shard(&account_id) {
            TargetShard::Remote(shard_id) => {
                trace!("Forwarding client request on {} to shard {shard_id}: BalanceChange({account_id}, {diff:?})", self.transaction_id);
                let state = ClientState::Forward(
                    ForwardTarget::Node(shard_id), 
                    self.transaction_id, 
                    ClientRequest::WriteBalance(account_id, diff)
                );

                if let Err(_) = self.forward_snd.send(state) {
                    error!("Failed to pass message to the shard server...");
                }

                trace!("Blocking wait for shard {shard_id}'s response to client request on {}", self.transaction_id);
                self.forward_rcv.recv().await.unwrap()
            },
            TargetShard::Local => {
                trace!("Handling client request on {} locally: BalanceChange({account_id}, {diff:?})", self.transaction_id);
                match self.shard.read(&self.transaction_id, &account_id).await {
                    Ok(balance) => match self.shard.write(&self.transaction_id, account_id, balance + diff.0).await {
                        Ok(_) => ClientResponse::Ok,
                        Err(Abort::ObjectNotFound) => ClientResponse::AbortedNotFound,
                        Err(_) => ClientResponse::Aborted
                    },
                    Err(Abort::ObjectNotFound) => 
                        // if diff.0 < 0 {
                        //     ClientResponse::AbortedNotFound
                        // } else {
                        //     match self.shard.write(&self.transaction_id, account_id, diff.0).await {
                        //         Ok(_) => ClientResponse::Ok,
                        //         Err(Abort::ObjectNotFound) => ClientResponse::AbortedNotFound,
                        //         Err(_) => ClientResponse::Aborted
                        //     }
                        // }
                        match self.shard.write(&self.transaction_id, account_id, diff.0).await {
                            Ok(_) => ClientResponse::Ok,
                            Err(Abort::ObjectNotFound) => ClientResponse::AbortedNotFound,
                            Err(_) => ClientResponse::Aborted
                        }
                    Err(_) => ClientResponse::Aborted
                }
            },
            TargetShard::DoesNotExist => {
                trace!("Unable to handle client request on {}: BalanceChange({account_id}, {diff:?}) -- account does not exist", self.transaction_id);
                ClientResponse::AbortedNotFound
            }
        };

        trace!("Client request on {}: BalanceChange({account_id_fmt}, {diff:?}) => {resp:?}", self.transaction_id);
        let ret_val = if resp.is_err() {
            trace!("Aborting transaction {}...", self.transaction_id);
            self.do_abort().await;
            Err(())
        } else {
            Ok(())
        };

        if let Err(e) = self.stream.send(resp).await {
            error!("Failed to send response to the client: {e:?}");
        }

        ret_val
    }

    async fn handle_balance_request(&mut self, account_id: AccountId) -> Result<(), ()> {
        let account_id_fmt = format!("{account_id}");
        let resp: ClientResponse = match self.extract_shard(&account_id) {
            TargetShard::Remote(shard_id) => {
                trace!("Forwarding client request on {} to shard {shard_id}: Balance({account_id})", self.transaction_id);
                let state = ClientState::Forward(
                    ForwardTarget::Node(shard_id), 
                    self.transaction_id, 
                    ClientRequest::ReadBalance(account_id)
                );

                if let Err(_) = self.forward_snd.send(state) {
                    error!("Failed to pass message to the shard server...");
                }

                trace!("Blocking wait for shard {shard_id}'s response to client request on {}", self.transaction_id);
                self.forward_rcv.recv().await.unwrap()
            },
            TargetShard::Local => {
                trace!("Handling client request on {} locally: Balance({account_id})", self.transaction_id);
                match self.shard.read(&self.transaction_id, &account_id).await {
                    Ok(value) => ClientResponse::Value(account_id, value),
                    Err(Abort::ObjectNotFound) => ClientResponse::AbortedNotFound,
                    Err(_) => ClientResponse::Aborted
                }
            },
            TargetShard::DoesNotExist => {
                trace!("Unable to handle client request on {}: Balance({account_id}) -- account does not exist", self.transaction_id);
                ClientResponse::AbortedNotFound
            }
        };

        trace!("Client request on {}: Balance({account_id_fmt}) => {resp:?}", self.transaction_id);
        let ret_val = if resp.is_err() {
            trace!("Aborting transaction {}...", self.transaction_id);
            self.do_abort().await;
            Err(())
        } else {
            Ok(())
        };

        if let Err(e) = self.stream.send(resp).await {
            error!("Failed to send response to the client: {e:?}");
        }

        ret_val
    }

    async fn do_abort(&mut self) {
        self.shard.abort(&self.transaction_id).await.unwrap();
        let abort_req = ClientState::Forward(
            ForwardTarget::Broadcast, 
            self.transaction_id, 
            ClientRequest::Abort
        );

        if let Err(_) = self.forward_snd.send(abort_req) {
            error!("Unable to forward abort request to shard server")
        }

        let mut abort_count = 1; // we aborted on this shard already!
        while abort_count < self.shard_ids.len() {
            if let ClientResponse::Aborted = self.forward_rcv.recv().await.unwrap() {
                abort_count += 1;
            } else {
                error!("Did not receive an abort in response!")
            }
        }
    }

    async fn do_commit(&self) {
        match self.shard.commit(&self.transaction_id).await {
            Ok(result) => format_commit_result(result),
            Err(e) => error!("FATAL ERROR: Failed to commit {}: {e:?}", self.transaction_id)
        }
    }

    async fn handle_commit_request(&mut self) {
        if let Err(_) = self.shard.check_commit(&self.transaction_id).await {
            info!("Consistency check on local shard failed: aborting...");
            self.do_abort().await;
            if let Err(e) = self.stream.send(ClientResponse::Aborted).await {
                error!("Failed to send response to the client: {e:?}");
            }

            return;
        }

        let check_commit_req = ClientState::Forward(
            ForwardTarget::Broadcast, 
            self.transaction_id, 
            ClientRequest::Commit
        );

        if let Err(_) = self.forward_snd.send(check_commit_req) {
            error!("Unable to forward check_commit request to shard server")
        }

        let resp = self.forward_rcv.recv().await.unwrap();
        match &resp {
            ClientResponse::CommitOk => self.do_commit().await,
            ClientResponse::Aborted => self.do_abort().await,
            resp => error!("FATAL ERROR: waiting for CommitOk or Aborted - got {resp:?}")
        }

        if let Err(e) = self.stream.send(resp).await {
            error!("Failed to send response to the client: {e:?}");
        }
    }

    pub async fn handle(mut self) {
        while let Some(Ok(request)) = self.stream.recv::<ClientRequest>().await {
            info!("Client task for {} handling {request:?}", self.transaction_id);
            match request {
                ClientRequest::WriteBalance(account_id, diff) => {
                    if self.handle_balance_change_request(account_id, diff).await.is_err() {
                        break;
                    }
                },
                ClientRequest::ReadBalance(account_id) => {
                    if self.handle_balance_request(account_id).await.is_err() {
                        break;
                    }
                },
                ClientRequest::Commit => {
                    self.handle_commit_request().await;
                    break;
                },
                ClientRequest::Abort => {
                    self.do_abort().await;
                    if let Err(e) = self.stream.send(ClientResponse::Aborted).await {
                        error!("Failed to send response to the client: {e:?}");
                    }
                    break;
                }
            }
        }

        let finished = ClientState::Finished(self.transaction_id);
        if let Err(_) = self.forward_snd.send(finished) {
            error!("Failed to pass finished message to server task.")
        }
    }
}