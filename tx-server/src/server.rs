use crate::{
    sharding::{shard::Shard, transaction_id::{IdGen, TransactionIdGenerator, TransactionId}}, 
    pool::{ConnectionPoolBuilder, ServerGroup, server::{ServerStateMessage, ServerStateMessageType}}, utils::MessageStream, BalanceDiff,
};
use serde::{Deserialize, Serialize};
use tx_common::{Amount, config::{NodeId, Config}, ClientName, ClientRequest, ClientResponse, RequestType, AccountId};
use tokio::{
    sync::mpsc::*, sync::mpsc::error::SendError, 
    select, net::{TcpStream, TcpListener}
};
use log::{error, info};
use std::{sync::Arc, collections::HashMap};

type AtomicShard = Arc<Shard<String, Amount, BalanceDiff>>;
type ClientId = usize;

pub struct Server {
    node_id: NodeId,
    shard: AtomicShard,
    listener: TcpListener,
    server_pool: ServerGroup<Forwarded>,
    from_servers: UnboundedReceiver<ServerStateMessage<Forwarded>>,
    clients: HashMap<ClientId, ClientHandle>,
    id_gen: TransactionIdGenerator,
    next_id: ClientId,
    from_clients: UnboundedReceiver<ClientState>,
    client_state_snd: UnboundedSender<ClientState>
}

struct ServerHandle {
    forwarding_handle: UnboundedSender<ClientState>,
    shard_ids: Vec<NodeId>,
    client_id: ClientId,
    server_id: NodeId,
    shard: AtomicShard,
    tx_id: TransactionId
}

struct ClientHandle {
    forward_snd: UnboundedSender<()>
}

struct Client {
    server_id: NodeId,
    client_id: ClientId,
    transaction_id: TransactionId,
    shard_ids: Vec<NodeId>,
    stream: MessageStream, 
    shard: AtomicShard,
    forward_snd: UnboundedSender<ClientState>,
    forward_rcv: UnboundedReceiver<()>
}

enum ForwardTarget {
    Node(NodeId),
    Broadcast
}

enum ClientState {
    Forward(ForwardTarget, ClientId, ClientRequest),
    Finished(ClientId)
}

#[derive(Clone, Debug, Deserialize, Serialize)]
enum Forwarded {
    Request(ClientId, ClientRequest),
    Response(ClientId, ClientResponse)
}

enum TargetShard {
    Remote(NodeId),
    Local,
    DoesNotExist
}

impl Client {
    pub fn new(server_handle: ServerHandle, stream: TcpStream, forward_rcv: UnboundedReceiver<()>) -> Self {
        Client {
            shard: server_handle.shard,
            server_id: server_handle.server_id,
            transaction_id: server_handle.tx_id,
            shard_ids: server_handle.shard_ids,
            forward_snd: server_handle.forwarding_handle,
            stream: MessageStream::from_tcp_stream(stream),
            client_id: server_handle.client_id,
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
    
    async fn handle(mut self) {
        while let Some(Ok(request)) = self.stream.recv::<ClientRequest>().await {
            info!("Client({}) handling {request:?}", self.client_id);
            match request.rqty {
                RequestType::BalanceChange(acct, diff) => {
                    match self.extract_shard(&acct) {
                        TargetShard::Remote(shard_id) => todo!(),
                        TargetShard::Local => {
                            match self.shard.write(&self.transaction_id, acct, diff).await {
                                Ok(_) => todo!(),
                                Err(_) => todo!()
                            }
                        },
                        TargetShard::DoesNotExist => todo!()
                    }
                },
                RequestType::Balance(acct) => {
                    match self.extract_shard(&acct) {
                        TargetShard::Remote(shard_id) => todo!(),
                        TargetShard::Local => {
                            match self.shard.read(&self.transaction_id, acct).await {
                                Ok(_) => todo!(),
                                Err(_) => todo!()
                            }
                        },
                        TargetShard::DoesNotExist => todo!()
                    }
                },
                RequestType::Commit => todo!(),
                RequestType::Abort => todo!()
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
            client_state_snd,
            next_id: 0
        }
    }

    fn pass_message(&self, target: NodeId, msg: Forwarded) -> Result<(), SendError<Forwarded>> {
        self.server_pool
            .get(&target)
            .unwrap()
            .pass_message(msg)
    }

    fn broadcast(&self, msg: Forwarded) -> Result<(), SendError<Forwarded>> {
        self.server_pool
            .values()
            .try_for_each(|target| target.pass_message(msg.clone()))
    }

    fn get_handle(&mut self) -> ServerHandle {
        self.next_id += 1;
        let shard_ids = self.server_pool
            .keys()
            .map(NodeId::clone)
            .collect();
        
        ServerHandle { 
            forwarding_handle: self.client_state_snd.clone(), 
            shard_ids,
            client_id: self.next_id,
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
            Forward(target, client_id, req) => {
                match target {
                    ForwardTarget::Broadcast => {

                    },
                    ForwardTarget::Node(node_id) => {
                        let fwd_req = Forwarded::Request(client_id, req);

                        if let Err(e) = self.pass_message(node_id, fwd_req) {
                            error!("Server group member crashed: {e}");
                            std::process::exit(1);
                        }
                    }
                }
            }
        };
    }

    pub async fn serve(&mut self) {
        loop {
            select! {
                client = self.listener.accept() => match client {
                    Ok((stream, _addr)) => {
                        let (snd, rcv) = unbounded_channel();
                        
                        let handle = self.get_handle();
                        let client_id = handle.client_id;
                        let client = Client::new(handle, stream, rcv);
                        let client_handle = ClientHandle {
                            forward_snd: snd
                        };
                        self.clients.insert(client_id, client_handle);

                        info!("Connected to client at {_addr:?} -- id={client_id}");
                        tokio::spawn(client.handle());
                    },
                    Err(e) => error!("failed to accept client: {e:?}")
                },
                Some(state) = self.from_clients.recv() => self.handle_client_state(state),
                Some(state) = self.from_servers.recv() => match state.msg {
                    ServerStateMessageType::Message(msg) => match msg {
                        Forwarded::Request(client_id, req) => todo!(),
                        Forwarded::Response(client_id, resp) => todo!()
                    },
                    ServerStateMessageType::Disconnected => {
                        eprintln!("Server {} disconnected ... exiting.", state.member_id);
                        std::process::exit(1);
                    }
                }
            }
        }
    }
}