use crate::{
    sharding::{shard::{Shard, Abort}, transaction_id::{IdGen, TransactionIdGenerator, TransactionId}}, 
    pool::{ConnectionPoolBuilder, ServerGroup, server::{ServerStateMessage, ServerStateMessageType}}, utils::MessageStream, BalanceDiff,
};
use serde::{Deserialize, Serialize};
use tx_common::{Amount, config::{NodeId, Config}, ClientRequest, ClientResponse, AccountId};
use tokio::{
    sync::mpsc::*, sync::mpsc::error::SendError, 
    select, net::{TcpStream, TcpListener}
};
use log::{error, info};
use std::{sync::Arc, collections::HashMap};

type AtomicShard = Arc<Shard<String, Amount, BalanceDiff>>;

pub struct Server {
    node_id: NodeId,
    shard: AtomicShard,
    listener: TcpListener,
    server_pool: ServerGroup<Forwarded>,
    from_servers: UnboundedReceiver<ServerStateMessage<Forwarded>>,
    clients: HashMap<TransactionId, ClientHandle>,
    id_gen: TransactionIdGenerator,
    from_clients: UnboundedReceiver<ClientState>,
    client_state_snd: UnboundedSender<ClientState>
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
    TwoPhaseCommit(TransactionId, CommitStatus)
}

/// Status exchanged between shards as part of the two-phase commit process.
#[derive(Clone, Debug, Deserialize, Serialize)]
enum CommitStatus {
    /// Indicates that the shard is ready to commit the transaction upon 
    /// checking that the transaction passes a consistency check. 
    ReadyToCommit,
    /// Indicates that the shard is unable to commit the transaction upon 
    /// checking that the transaction passes a consistency check.
    CannotCommit,
    /// Notifies a shard that all other shards are able to commit the 
    /// transaction, so the shard can proceed with the commit. 
    DoCommit
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

    async fn handle_balance_change(&mut self, account_id: AccountId, diff: BalanceDiff) {
        let resp: ClientResponse = match self.extract_shard(&account_id) {
            TargetShard::Remote(shard_id) => {
                let state = ClientState::Forward(
                    ForwardTarget::Node(shard_id), 
                    self.transaction_id, 
                    ClientRequest::BalanceChange(account_id, diff)
                );

                if let Err(_) = self.forward_snd.send(state) {
                    error!("Failed to pass message to the shard server...");
                }

                self.forward_rcv.blocking_recv().unwrap()
            },
            TargetShard::Local => {
                match self.shard.write(&self.transaction_id, account_id, diff).await {
                    Ok(_) => ClientResponse::Ok,
                    Err(Abort::ObjectNotFound) => ClientResponse::AbortedNotFound,
                    Err(_) => ClientResponse::Aborted
                }
            },
            TargetShard::DoesNotExist => ClientResponse::AbortedNotFound
        };

        if resp.is_err() {
            todo!() // TODO do abort
        }

        if let Err(e) = self.stream.send(resp).await {
            error!("Failed to send response to the client: {e:?}");
        }
    }

    async fn handle_balance(&mut self, account_id: AccountId) {
        let resp: ClientResponse = match self.extract_shard(&account_id) {
            TargetShard::Remote(shard_id) => {
                let state = ClientState::Forward(
                    ForwardTarget::Node(shard_id), 
                    self.transaction_id, 
                    ClientRequest::Balance(account_id)
                );

                if let Err(_) = self.forward_snd.send(state) {
                    error!("Failed to pass message to the shard server...");
                }

                self.forward_rcv.blocking_recv().unwrap()
            },
            TargetShard::Local => {
                match self.shard.read(&self.transaction_id, &account_id).await {
                    Ok(value) => ClientResponse::Value(account_id, value),
                    Err(Abort::ObjectNotFound) => ClientResponse::AbortedNotFound,
                    Err(_) => ClientResponse::Aborted
                }
            },
            TargetShard::DoesNotExist => ClientResponse::AbortedNotFound
        };

        if resp.is_err() {
            todo!() // TODO do abort
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
                    self.handle_balance_change(account_id, diff).await
                },
                ClientRequest::Balance(account_id) => {
                    self.handle_balance(account_id).await
                },
                ClientRequest::Commit => todo!(),
                ClientRequest::Abort => todo!()
            }
        }
    }
}

impl Server {
    pub async fn new(node_id: NodeId, config: Config, timeout: u64) -> Self {
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
            from_clients,
            client_state_snd
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
        let shard_ids = self.server_pool
            .keys()
            .map(NodeId::clone)
            .collect();
        
        ServerHandle { 
            forwarding_handle: self.client_state_snd.clone(), 
            shard_ids,
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
            Forward(target, tx_id, req) => {
                match target {
                    ForwardTarget::Broadcast => {
                        todo!()
                    },
                    ForwardTarget::Node(node_id) => {
                        let fwd_req = Forwarded::Request(tx_id, req);
                        if let Err(e) = self.pass_message(node_id, fwd_req) {
                            error!("Server {node_id} disconnected: {e} ... exiting.");
                            std::process::exit(1);
                        }
                    }
                }
            }
        };
    }

    fn handle_server_state(&mut self, state: ServerStateMessage<Forwarded>) {
        use ServerStateMessageType::*;
        use Forwarded::*;
        
        match state.msg {
            Message(Request(tx_id, request)) => {
                let resp_handle = self.get_server_send(state.member_id);
                let shard = self.shard.clone();
                tokio::spawn(async move {
                    let resp: ClientResponse = match request {
                        ClientRequest::BalanceChange(account_id, diff) => {
                            match shard.write(&tx_id, account_id, diff).await {
                                Ok(_) => ClientResponse::Ok,
                                Err(Abort::ObjectNotFound) => ClientResponse::AbortedNotFound,
                                Err(_) => ClientResponse::Aborted
                            }
                        },
                        ClientRequest::Balance(account_id) => {
                            match shard.read(&tx_id, &account_id).await {
                                Ok(value) => ClientResponse::Value(account_id, value),
                                Err(Abort::ObjectNotFound) => ClientResponse::AbortedNotFound,
                                Err(_) => ClientResponse::Aborted
                            }
                        },
                        ClientRequest::Commit => todo!(), // Check that the commit is valid. This is the first stage in the 2 phase commit.
                        ClientRequest::Abort => todo!()
                    };

                    let fwd_resp = Forwarded::Response(tx_id, resp);
                    if let Err(_) = resp_handle.send(fwd_resp) {
                        error!("Server {} disconnected ... exiting.", state.member_id)
                    }
                });
            },
            Message(TwoPhaseCommit(tx_id, commit_status)) => {

            },
            Message(Response(tx_id, resp)) => {
                if let Err(e) = self.pass_to_client(&tx_id, resp) {
                    error!("Client {tx_id} crashed: {e}");
                    std::process::exit(1);
                }
            }
            ServerStateMessageType::Disconnected => {
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
                        let (snd, rcv) = unbounded_channel();
                        
                        let handle = self.get_handle();
                        let tx_id = handle.tx_id;
                        let client = Client::new(handle, stream, rcv);
                        let client_handle = ClientHandle {
                            forward_snd: snd
                        };
                        self.clients.insert(tx_id, client_handle);

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