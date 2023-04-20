pub mod server;
mod builder;

pub use builder::ConnectionPoolBuilder;

use tokio::{sync::mpsc::{UnboundedReceiver, UnboundedSender}, net::TcpListener};
use server::{RemoteServerHandle, ServerStateMessage};
use std::{collections::HashMap};
use tx_common::config::NodeId;
use log::{trace, error};

pub type ServerGroup<M> = HashMap<NodeId, RemoteServerHandle<M>>;

pub struct ConnectionPool<M> {
    pub listener: TcpListener, 
    pub group: ServerGroup<M>,
    pub node_id: NodeId,
    pub from_members: UnboundedReceiver<ServerStateMessage<M>>,
    pub client_snd_handle: UnboundedSender<ServerStateMessage<M>>
}