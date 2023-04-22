mod protocol;
mod client;

use crate::{
    sharding::{Shard, Abort, CommitSuccess, TransactionIdGenerator, TransactionId}, 
    pool::server::{ServerStateMessage, ServerStateMessageType},
    pool::{ConnectionPoolBuilder, ServerGroup}, BalanceDiff
};
use tx_common::{Amount, ClientRequest, ClientResponse, config::{NodeId, Config}};
use tokio::{sync::mpsc::*, select, net::TcpListener};
use std::{sync::Arc, collections::HashMap};
use log::{error, info, trace};
use client::Client;
use protocol::*;

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
    forward_snd: UnboundedSender<ClientResponse>,
    commit_count: usize,
    commit_status: CommitStatus
}

fn format_commit_result(result: CommitSuccess<Vec<(String, i64)>>) {
    use CommitSuccess::*;

    if let ValueChanged(mut result) = result {
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
}

impl Server {
    pub async fn start(node_id: NodeId, config: Config, timeout: u64) -> Self {
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
            from_clients,
            client_state_snd,
            shard_ids
        }
    }

    fn pass_message(&self, target: NodeId, msg: Forwarded) -> Result<(), error::SendError<Forwarded>> {
        self.server_pool
            .get(&target)
            .unwrap()
            .pass_message(msg)
    }

    fn pass_to_client(&self, tx_id: &TransactionId, msg: ClientResponse) -> Result<(), error::SendError<ClientResponse>> {
        self.clients
            .get(&tx_id)
            .unwrap()
            .forward_snd
            .send(msg)
    }

    fn broadcast(&self, msg: Forwarded) -> Result<(), error::SendError<Forwarded>> {
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
            Finished(tx_id) => {
                trace!("Reaping client connection for {tx_id}");
                self.clients.remove(&tx_id);
            },
            Forward(ForwardTarget::Broadcast, tx_id, req) => {
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
        info!("Handling remote request from {sender_id} for {tx_id}: {request:?}");
        tokio::spawn(async move {
            let fwd_resp: Forwarded = match request {
                ClientRequest::WriteBalance(account_id, diff) => {
                    let resp = match shard.write(&tx_id, account_id, diff).await {
                        Ok(_) => ClientResponse::Ok,
                        Err(Abort::ObjectNotFound) => ClientResponse::AbortedNotFound,
                        Err(_) => ClientResponse::Aborted
                    };

                    Response(tx_id, resp)
                },
                ClientRequest::ReadBalance(account_id) => {
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
        let client_handle = self.clients.get_mut(&tx_id).unwrap();
        client_handle.commit_count += 1;
        if let CommitStatus::CannotCommit = commit_status {
            client_handle.commit_status = commit_status;
        }

        trace!("Two-phase commit for {tx_id} received {}/{} responses", client_handle.commit_count, self.server_pool.len());
        if client_handle.commit_count == self.server_pool.len() {
            match client_handle.commit_status {
                CommitStatus::ReadyToCommit => {
                    trace!("All shards ready to commit.");
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
                    trace!("Not all shards can commit. Notifying client task to initiate abort.");
                    if let Err(e) = self.pass_to_client(&tx_id, ClientResponse::Aborted) {
                        error!("Client handler for {tx_id} crashed: {e}");
                        std::process::exit(1);
                    }
                }
            }
        }
    }

    fn handle_server_state(&mut self, state: ServerStateMessage<Forwarded>) {
        use ServerStateMessageType::*;
        use Forwarded::*;
        
        match state.msg {
            Message(Request(tx_id, request)) => {
                trace!("Handling remote request for {tx_id} on behalf of coordinator {}: {request:?}", state.member_id);
                self.handle_remote_request(state.member_id, tx_id, request)
            },
            Message(Response(tx_id, resp)) => {
                trace!("Passing response to remote request for {tx_id} from shard {} back to client: {resp:?}", state.member_id);
                if let Err(e) = self.pass_to_client(&tx_id, resp) {
                    error!("Client handler for {tx_id} crashed: {e}");
                    std::process::exit(1); // TODO maybe abort the transaction???
                }
            },
            Message(TwoPhaseCommitStatus(tx_id, commit_status)) => {
                trace!("Handling two-phase commit status for {tx_id} initiated at {}: {commit_status:?}", state.member_id);
                self.handle_two_phase_commit(tx_id, commit_status);
            },
            Message(DoCommit(tx_id)) => {
                trace!("Doing commit for {tx_id}...");
                let shard: AtomicShard = self.shard.clone();
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
                        self.clients.insert(tx_id, ClientHandle { 
                            forward_snd,
                            commit_count: 0,
                            commit_status: CommitStatus::ReadyToCommit
                        });

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
