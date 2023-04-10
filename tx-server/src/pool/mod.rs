pub mod member;
pub mod stream;

use member::{member_loop, MulticastMemberData, MulticastMemberHandle, MemberStateMessage};
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel}, select,
    io, time::timeout, net::{TcpStream, TcpListener}
};
use std::{net::SocketAddr, fmt, time::Duration, collections::HashMap};
use serde::{Serialize, de::DeserializeOwned, Deserialize};
use tokio_retry::{Retry, strategy::FixedInterval};
use tx_common::config::{Config, NodeId};
use log::{trace, error};

use self::stream::MessageStream;

pub type MulticastGroup<M> = HashMap<NodeId, MulticastMemberHandle<M>>;

pub struct ConnectionPool<M> {
    pub listener: TcpListener, 
    pub group: MulticastGroup<M>,
    pub node_id: NodeId,
    pub from_members: UnboundedReceiver<MemberStateMessage<M>>,
    pub client_snd_handle: UnboundedSender<MemberStateMessage<M>>,
    timeout_secs: Option<u64>,
    config: Config
}

pub static CONNECTION_POOL_INIT_TIMEOUT_SECS: u64 = 60;
pub static CONNECTION_RETRY_DELAY_MS: u64 = 100;

#[derive(Debug, Deserialize, Serialize)]
struct Handshake(NodeId);

impl<M> ConnectionPool<M> {
    pub async fn new(config: Config, node_id: NodeId) -> Result<Self, io::Error> {
        let (client_snd_handle, from_clients) = unbounded_channel();
        let node_config = config.get(&node_id).unwrap();
        let bind_addr: SocketAddr = ([0, 0, 0, 0], node_config.port).into();
        let listener = TcpListener::bind(bind_addr).await?;

        Ok(Self {
            listener,
            group: Default::default(),
            node_id,
            from_members: from_clients,
            client_snd_handle,
            timeout_secs: None,
            config
        })
    }

    pub fn with_timeout(mut self, seconds: u64) -> Self {
        self.timeout_secs = Some(seconds);
        self
    }

    async fn connect_to_node(this_node: NodeId, node_id: NodeId, host: String, port: u16, stream_snd: UnboundedSender<(MessageStream<M>, NodeId)>) where M: fmt::Debug {
        let server_addr = format!("{host}:{port}");
        trace!("Connecting to {} at {}...", node_id, server_addr);

        let retry_strategy = FixedInterval::from_millis(CONNECTION_RETRY_DELAY_MS);
        match Retry::spawn(retry_strategy, || TcpStream::connect(&server_addr)).await {
            Ok(stream) => {
                trace!("Connected to {} at {}", node_id, server_addr);
                let mut stream = MessageStream::from_tcp_stream(stream);

                let handshake = Handshake(this_node);
                if let Err(e) = stream.unchecked_send(handshake).await {
                    error!("Failed to send handshake to Node {node_id}: {e:?}")
                }

                if let Err(e) = stream_snd.send((stream, node_id)) {
                    error!("Failed to finish handshake with Node {node_id}: {e:?}")
                }
            },
            Err(e) => {
                eprintln!("Failed to connect to {}: {:?}... Stopping.", server_addr, e);
                std::process::exit(1);
            }
        }
    }

    fn admit_member(&mut self, stream: MessageStream<M>, member_id: NodeId) where M: 'static + Send + Serialize + DeserializeOwned + fmt::Debug {
        let (to_client, from_engine) = unbounded_channel();
        let member_data = MulticastMemberData {
            stream,
            member_id,
            from_engine,
            to_engine: self.client_snd_handle.clone()
        };

        let handle = tokio::spawn(member_loop(member_data));
        self.group.insert(member_id, MulticastMemberHandle { 
            member_id,
            to_client,
            handle
        });
    }

    async fn connect_inner(mut self) -> Self where M: 'static + Send + Serialize + DeserializeOwned + fmt::Debug {
        let (stream_snd, mut stream_rcv) = unbounded_channel();
        let node_config = self.config.get(&self.node_id).unwrap();

        for node in node_config.connection_list.iter() {
            let connect_config = self.config.get(&node).unwrap();
            let snd_clone = stream_snd.clone();
            tokio::spawn(Self::connect_to_node(
                self.node_id, 
                *node, 
                connect_config.hostname.clone(), 
                connect_config.port, 
                snd_clone
            ));
        }
        drop(stream_snd);
        
        loop {
            select! {
                client = self.listener.accept() => match client {
                    Ok((stream, _addr)) => {
                        let mut stream = MessageStream::<M>::from_tcp_stream(stream);

                        match stream.unchecked_recv::<Handshake>().await {
                            Some(Ok(Handshake(node_id))) => self.admit_member(stream, node_id),
                            Some(Err(e)) => error!("Error on handshake from {_addr}: {e:?}"),
                            None => error!("Failed to receive handshake from {_addr}")
                        }

                        if self.group.len() == self.config.len() - 1 { break self; }
                    },
                    Err(e) => error!("Could not accept client: {:?}", e)
                },
                Some((stream, member_id)) = stream_rcv.recv() => {
                    self.admit_member(stream, member_id);
                    if self.group.len() == self.config.len() - 1 { break self; }
                }
            }
        } 
    }

    pub async fn connect(self) -> Self where M: 'static + Send + Serialize + DeserializeOwned + fmt::Debug {
        let time_limit = self.timeout_secs.unwrap_or(CONNECTION_POOL_INIT_TIMEOUT_SECS);
        let time_limit = Duration::from_secs(time_limit);
        
        match timeout(time_limit, self.connect_inner()).await {
            Ok(p) => p,
            Err(_) => {
                eprintln!("Failed to connect to all nodes within {}s... Stopping.", time_limit.as_secs());
                std::process::exit(1);
            }
        }
    }
}
