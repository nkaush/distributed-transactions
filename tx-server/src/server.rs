use crate::{
    sharding::{Shard, Abort, IdGen, TransactionIdGenerator, TransactionId}, 
    pool::server::{ServerStateMessage, ServerStateMessageType}, BalanceDiff,
    pool::{ConnectionPoolBuilder, ServerGroup}
};
use tx_common::{
    Amount, ClientRequest, ClientResponse, AccountId,
    config::{NodeId, Config}, stream::MessageStream
};
use tokio::{
    sync::mpsc::*, sync::mpsc::error::SendError, 
    select, net::{TcpStream, TcpListener}
};
use std::{sync::Arc, collections::HashMap};
use serde::{Deserialize, Serialize};
use log::{error, info, debug};

type AtomicShard = Arc<Shard<String, Amount, BalanceDiff>>;

pub struct Server {
    node_id: NodeId,
    shard: AtomicShard,
    listener: TcpListener,
    server_pool: ServerGroup<Forwarded>,
    shard_ids: Vec<NodeId>,
    from_servers: UnboundedReceiver<ServerStateMessage<Forwarded>>,
    clients: HashMap<TransactionId, ClientHandle>,
    id_gen: TransactionIdGenerator,
    from_clients: UnboundedReceiver<ClientState>,
    client_state_snd: UnboundedSender<ClientState>,
    commit_status: HashMap<TransactionId, (usize, CommitStatus)>
}

struct ServerHandle {
    forwarding_handle: UnboundedSender<ClientState>,
    shard_ids: Vec<NodeId>,
    server_id: NodeId,
    shard: AtomicShard,
    tx_id: TransactionId
}

struct ClientHandle {
    forward_snd: UnboundedSender<ClientResponse>
}

/// This struct contains all the data that a client handler task uses to process
/// a transaction from a client. This struct contains data pertaining to the 
/// shard this server represents and channels for communicating with the client 
/// and with the main server task.
struct Client {
    server_id: NodeId,
    transaction_id: TransactionId,
    shard_ids: Vec<NodeId>,
    stream: MessageStream, 
    shard: AtomicShard,
    forward_snd: UnboundedSender<ClientState>,
    forward_rcv: UnboundedReceiver<ClientResponse>
}

/// This enum indicates to the server how to forward a message.
enum ForwardTarget {
    /// Nofity the server to forward a message to a single node.
    Node(NodeId),
    /// Nofity the server to broadcast a message to all nodes in the group.
    Broadcast
}

/// This enum is used for communication between the client handler and server
/// tasks. Client handlers must communicate with the server task when they need
/// the server to forward a message onto another shard to read or write some
/// data stored on a remote shard. Client handlers also notify the server task
/// when they are finished processing a transaction so the server may reap any
/// resources used to manage the client handler. 
enum ClientState {
    /// Notify the server to either forward a request from the client to another 
    /// shard since the data requested is located on that shard or broadcast the
    /// request if the client handler must abort or commit a transaction. 
    Forward(ForwardTarget, TransactionId, ClientRequest),
    /// Notify the server that the client handler is finished processing a 
    /// transaction so the server may reap resources associated with the client. 
    Finished(TransactionId)
}

/// Thus enum represents communication between shards forwarding requests on 
/// behalf of clients coordinating transactions with shards and returning a 
/// response to any received requests. This enum also represents the state
/// associated with a two-phase commit of a transaction.
#[derive(Clone, Debug, Deserialize, Serialize)]
enum Forwarded {
    /// Forwards a client request from a coordinator to a shard.
    Request(TransactionId, ClientRequest),
    /// Respond to a request from a coordinator upon processing a client request
    /// received from this coordinator. 
    Response(TransactionId, ClientResponse),
    /// Messages exchanged as part of a two-phase commit of a transaction.
    TwoPhaseCommitStatus(TransactionId, CommitStatus),
    /// Notifies a shard that all other shards are able to commit the 
    /// transaction, so the shard can proceed with the commit. 
    DoCommit(TransactionId)
}

/// Status exchanged between shards as part of the two-phase commit process.
#[derive(Clone, Debug, Deserialize, Serialize)]
enum CommitStatus {
    /// Indicates that the shard is ready to commit the transaction upon 
    /// checking that the transaction passes a consistency check. 
    ReadyToCommit,
    /// Indicates that the shard is unable to commit the transaction upon 
    /// checking that the transaction passes a consistency check.
    CannotCommit
}

/// This enum represents the result of attempting to identify the shard that an
/// object is located on. Objects are associated with the shard identified with 
/// the first character of the object's name. 
enum TargetShard {
    /// Indicates that the object is associated with a shard not located on the
    /// server that is coordinating the transaction requesting the object.
    Remote(NodeId),
    /// Indicates that the object is associated with the server that is 
    /// coordinating the transaction requesting the object.
    Local,
    /// Indicates that the first letter of the object is not a valid shard name. 
    DoesNotExist
}

fn format_commit_result(mut result: Vec<(String, i64)>) {
    result.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
    let mut output = String::new();
    for (k, v) in result.into_iter() {
        if v != 0 {
            output += &format!("{k} = {v} ");
        }
    }

    if !output.is_empty() {
        println!("{output}");
    }
}

impl Client {
    pub fn new(server_handle: ServerHandle, stream: TcpStream, forward_rcv: UnboundedReceiver<ClientResponse>) -> Self {
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

    async fn handle_balance_change(&mut self, account_id: AccountId, diff: BalanceDiff) -> Result<(), ()> {
        let account_id_fmt = format!("{account_id}");
        let resp: ClientResponse = match self.extract_shard(&account_id) {
            TargetShard::Remote(shard_id) => {
                debug!("Forwarding client request on {} to shard {shard_id}: BalanceChange({account_id}, {diff:?})", self.transaction_id);
                let state = ClientState::Forward(
                    ForwardTarget::Node(shard_id), 
                    self.transaction_id, 
                    ClientRequest::BalanceChange(account_id, diff)
                );

                if let Err(_) = self.forward_snd.send(state) {
                    error!("Failed to pass message to the shard server...");
                }

                debug!("Blocking wait for shard {shard_id}'s response to client request on {}", self.transaction_id);
                self.forward_rcv.recv().await.unwrap()
            },
            TargetShard::Local => {
                debug!("Handling client request on {} locally: BalanceChange({account_id}, {diff:?})", self.transaction_id);
                match self.shard.write(&self.transaction_id, account_id, diff).await {
                    Ok(_) => ClientResponse::Ok,
                    Err(Abort::ObjectNotFound) => ClientResponse::AbortedNotFound,
                    Err(_) => ClientResponse::Aborted
                }
            },
            TargetShard::DoesNotExist => {
                debug!("Unable to handle client request on {}: BalanceChange({account_id}, {diff:?}) -- account does not exist", self.transaction_id);
                ClientResponse::AbortedNotFound
            }
        };

        debug!("Client request on {}: BalanceChange({account_id_fmt}, {diff:?}) => {resp:?}", self.transaction_id);
        let ret_val = if resp.is_err() {
            debug!("Aborting transaction {}...", self.transaction_id);
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

    async fn handle_balance(&mut self, account_id: AccountId) -> Result<(), ()> {
        let account_id_fmt = format!("{account_id}");
        let resp: ClientResponse = match self.extract_shard(&account_id) {
            TargetShard::Remote(shard_id) => {
                debug!("Forwarding client request on {} to shard {shard_id}: Balance({account_id})", self.transaction_id);
                let state = ClientState::Forward(
                    ForwardTarget::Node(shard_id), 
                    self.transaction_id, 
                    ClientRequest::Balance(account_id)
                );

                if let Err(_) = self.forward_snd.send(state) {
                    error!("Failed to pass message to the shard server...");
                }

                debug!("Blocking wait for shard {shard_id}'s response to client request on {}", self.transaction_id);
                self.forward_rcv.recv().await.unwrap()
            },
            TargetShard::Local => {
                debug!("Handling client request on {} locally: Balance({account_id})", self.transaction_id);
                match self.shard.read(&self.transaction_id, &account_id).await {
                    Ok(value) => ClientResponse::Value(account_id, value),
                    Err(Abort::ObjectNotFound) => ClientResponse::AbortedNotFound,
                    Err(_) => ClientResponse::Aborted
                }
            },
            TargetShard::DoesNotExist => {
                debug!("Unable to handle client request on {}: Balance({account_id}) -- account does not exist", self.transaction_id);
                ClientResponse::AbortedNotFound
            }
        };

        debug!("Client request on {}: Balance({account_id_fmt}) => {resp:?}", self.transaction_id);
        let ret_val = if resp.is_err() {
            debug!("Aborting transaction {}...", self.transaction_id);
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

    async fn handle_commit(&mut self) {
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
            ClientResponse::CommitOk => {
                self.do_commit().await;
            },
            ClientResponse::Aborted => {
                self.do_abort().await;
            },
            resp => error!("FATAL ERROR: waiting for CommitOk or Aborted - got {resp:?}")
        }

        if let Err(e) = self.stream.send(resp).await {
            error!("Failed to send response to the client: {e:?}");
        }
    }
    
    async fn handle(mut self) {
        while let Some(Ok(request)) = self.stream.recv::<ClientRequest>().await {
            info!("Client({}) handling {request:?}", self.transaction_id);
            match request {
                ClientRequest::BalanceChange(account_id, diff) => {
                    if self.handle_balance_change(account_id, diff).await.is_err() {
                        break;
                    }
                },
                ClientRequest::Balance(account_id) => {
                    if self.handle_balance(account_id).await.is_err() {
                        break;
                    }
                },
                ClientRequest::Commit => {
                    self.handle_commit().await;
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

impl Server {
    pub async fn new(node_id: NodeId, config: Config, timeout: u64) -> Self {
        let shard_ids = config.keys().map(char::clone).collect();
        let (client_state_snd, from_clients) = unbounded_channel();
        let server_pool = ConnectionPoolBuilder::new(config, node_id)
            .await
            .unwrap_or_else(|e| {
                eprintln!("Unable to construct connection pool: {e}");
                std::process::exit(1);
            })
            .with_timeout(timeout)
            .connect()
            .await
            .unwrap_or_else(|_| {
                eprintln!("Failed to connect to all nodes within {}s... Stopping.", timeout);
                std::process::exit(1);
            });

        Self {
            node_id,
            shard: Arc::new(Shard::new(node_id)),
            id_gen: TransactionIdGenerator::new(node_id),
            server_pool: server_pool.group,
            from_servers: server_pool.from_members,
            listener: server_pool.listener,
            clients: HashMap::new(),
            commit_status: HashMap::new(),
            from_clients,
            client_state_snd,
            shard_ids
        }
    }

    fn pass_message(&self, target: NodeId, msg: Forwarded) -> Result<(), SendError<Forwarded>> {
        self.server_pool
            .get(&target)
            .unwrap()
            .pass_message(msg)
    }

    fn pass_to_client(&self, tx_id: &TransactionId, msg: ClientResponse) -> Result<(), SendError<ClientResponse>> {
        self.clients
            .get(&tx_id)
            .unwrap()
            .forward_snd
            .send(msg)
    }

    fn broadcast(&self, msg: Forwarded) -> Result<(), SendError<Forwarded>> {
        self.server_pool
            .values()
            .try_for_each(|target| target.pass_message(msg.clone()))
    }

    fn get_server_send(&self, node_id: NodeId) -> UnboundedSender<Forwarded> {
        self.server_pool.get(&node_id).unwrap().to_client.clone()
    }

    fn get_handle(&mut self) -> ServerHandle {        
        ServerHandle { 
            forwarding_handle: self.client_state_snd.clone(), 
            shard_ids: self.shard_ids.clone(),
            server_id: self.node_id,
            shard: self.shard.clone(),
            tx_id: self.id_gen.next()
        }
    }

    fn handle_client_state(&mut self, client_state: ClientState) {
        use ClientState::*;
        match client_state {
            Finished(client_id) => {
                self.clients.remove(&client_id);
            },
            Forward(ForwardTarget::Broadcast, tx_id, req) => {
                if let ClientRequest::Commit = req {
                    self.commit_status.insert(tx_id, (0, CommitStatus::ReadyToCommit));
                }

                let fwd_req: Forwarded = Forwarded::Request(tx_id, req);
                if let Err(e) = self.broadcast(fwd_req) {
                    error!("Unknown server disconnected: {e} ... exiting.");
                    std::process::exit(1);
                }
            },
            Forward(ForwardTarget::Node(node_id), tx_id, req) => {
                let fwd_req = Forwarded::Request(tx_id, req);
                if let Err(e) = self.pass_message(node_id, fwd_req) {
                    error!("Server {node_id} disconnected: {e} ... exiting.");
                    std::process::exit(1);
                }
            }
        };
    }

    fn handle_remote_request(&mut self, sender_id: NodeId, tx_id: TransactionId, request: ClientRequest) {
        use CommitStatus::*;
        use Forwarded::*;

        let resp_handle = self.get_server_send(sender_id);
        let shard = self.shard.clone();
        let shard_id = self.node_id;
        tokio::spawn(async move {
            let fwd_resp: Forwarded = match request {
                ClientRequest::BalanceChange(account_id, diff) => {
                    let resp = match shard.write(&tx_id, account_id, diff).await {
                        Ok(_) => ClientResponse::Ok,
                        Err(Abort::ObjectNotFound) => ClientResponse::AbortedNotFound,
                        Err(_) => ClientResponse::Aborted
                    };

                    Response(tx_id, resp)
                },
                ClientRequest::Balance(account_id) => {
                    let resp = match shard.read(&tx_id, &account_id).await {
                        Ok(value) => ClientResponse::Value(account_id, value),
                        Err(Abort::ObjectNotFound) => ClientResponse::AbortedNotFound,
                        Err(_) => ClientResponse::Aborted
                    };

                    Response(tx_id, resp)
                },
                ClientRequest::Commit => {
                    // Check that the commit is valid. This is the first stage 
                    // in the 2 phase commit process.
                    match shard.check_commit(&tx_id).await {
                        Ok(_) => TwoPhaseCommitStatus(tx_id, ReadyToCommit),
                        Err(e) => {
                            info!("Unable to commit {tx_id}: {e:?}");
                            TwoPhaseCommitStatus(tx_id, CannotCommit)
                        }
                    }
                },
                ClientRequest::Abort => {
                    shard.abort(&tx_id).await.unwrap();
                    info!("Abort {tx_id} completed on {shard_id}.");
                    Response(tx_id, ClientResponse::Aborted)
                }
            };

            if let Err(_) = resp_handle.send(fwd_resp) {
                error!("Server {} disconnected ... exiting.", sender_id)
            }
        });
    }

    fn handle_two_phase_commit(&mut self, tx_id: TransactionId, commit_status: CommitStatus) {
        let (count, curr_status) = self.commit_status.get_mut(&tx_id).unwrap();
        *count += 1;
        if let CommitStatus::CannotCommit = commit_status {
            *curr_status = commit_status;
        }

        debug!("Two-phase commit for {tx_id} received {}/{} responses", *count, self.server_pool.len());
        if *count == self.server_pool.len() {
            match *curr_status {
                CommitStatus::ReadyToCommit => {
                    debug!("All shards ready to commit.");
                    let fwd_req = Forwarded::DoCommit(tx_id);
                    if let Err(e) = self.pass_to_client(&tx_id, ClientResponse::CommitOk) {
                        error!("Client handler for {tx_id} crashed: {e}");
                        std::process::exit(1);
                    }

                    if let Err(e) = self.broadcast(fwd_req) {
                        error!("Unknown server disconnected: {e} ... exiting.");
                        std::process::exit(1);
                    }   
                },
                CommitStatus::CannotCommit => {
                    debug!("Not all shards can commit. Notifying client task to initiate abort.");
                    if let Err(e) = self.pass_to_client(&tx_id, ClientResponse::Aborted) {
                        error!("Client handler for {tx_id} crashed: {e}");
                        std::process::exit(1);
                    }
                }
            }

            self.commit_status.remove(&tx_id);
        }
    }

    fn handle_server_state(&mut self, state: ServerStateMessage<Forwarded>) {
        use ServerStateMessageType::*;
        use Forwarded::*;
        
        match state.msg {
            Message(Request(tx_id, request)) => {
                debug!("Handling remote request for {tx_id}: {request:?}");
                self.handle_remote_request(state.member_id, tx_id, request)
            },
            Message(Response(tx_id, resp)) => {
                debug!("Passing response from remote request for {tx_id} back to client: {resp:?}");
                if let Err(e) = self.pass_to_client(&tx_id, resp) {
                    error!("Client handler for {tx_id} crashed: {e}");
                    std::process::exit(1); // TODO maybe abort the transaction???
                }
            },
            Message(TwoPhaseCommitStatus(tx_id, commit_status)) => {
                debug!("Handling two-phase commit status for {tx_id}: {commit_status:?}");
                self.handle_two_phase_commit(tx_id, commit_status);
            },
            Message(DoCommit(tx_id)) => {
                debug!("Doing commit for {tx_id}...");
                let shard = self.shard.clone();
                tokio::spawn(async move {
                    match shard.commit(&tx_id).await {
                        Ok(result) => format_commit_result(result),
                        Err(e) => error!("FATAL ERROR: Failed to commit {tx_id}: {e:?}")
                    }
                });
            },
            Disconnected => {
                eprintln!("Server {} disconnected ... exiting.", state.member_id);
                std::process::exit(1);
            }
        }
    }

    pub async fn serve(&mut self) {
        loop {
            select! {
                client = self.listener.accept() => match client {
                    Ok((stream, _addr)) => {
                        let (forward_snd, rcv) = unbounded_channel();
                        
                        let handle = self.get_handle();
                        let tx_id = handle.tx_id;
                        let client = Client::new(handle, stream, rcv);
                        self.clients.insert(tx_id, ClientHandle { forward_snd });

                        info!("Connected to client at {_addr:?} -- id={tx_id}");
                        tokio::spawn(client.handle());
                    },
                    Err(e) => error!("failed to accept client: {e:?}")
                },
                Some(state) = self.from_clients.recv() => self.handle_client_state(state),
                Some(state) = self.from_servers.recv() => self.handle_server_state(state)
            }
        }
    }
}