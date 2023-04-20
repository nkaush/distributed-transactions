use tokio::{
    sync::mpsc::{UnboundedSender, UnboundedReceiver, error::SendError}, 
    task::JoinHandle, select
};
use serde::{de::DeserializeOwned, Serialize};
use crate::utils::MessageStream;
use super::NodeId;
use log::trace;
use std::fmt;

/// Represents any message types a member handler thread could send the transaction engine
#[derive(Debug)]
pub enum ServerStateMessageType<M> {
    Message(M),
    Disconnected
}

/// Represents any messages a member handler thread could send the multicast engine.
#[derive(Debug)]
pub struct ServerStateMessage<I> {
    pub msg: ServerStateMessageType<I>,
    pub member_id: NodeId
}

/// The handle that the multicast engine has for each member handler thread.
pub struct RemoteServerHandle<O> {
    pub member_id: NodeId,
    pub to_client: UnboundedSender<O>,
    pub handle: JoinHandle<()>
}

impl<M> RemoteServerHandle<M> {
    pub fn pass_message(&self, msg: M) -> Result<(), SendError<M>> {
        self.to_client.send(msg)
    }
}

impl<M> Drop for RemoteServerHandle<M> {
    fn drop(&mut self) {
        trace!("Aborting client thread for {}", self.member_id);
        self.handle.abort()
    }
}

pub(super) struct RemoteServerData<I, O> {
    pub member_id: NodeId,
    pub stream: MessageStream,
    pub to_engine: UnboundedSender<ServerStateMessage<I>>,
    pub from_engine: UnboundedReceiver<O>
}

impl<I, O> RemoteServerData<I, O> {
    fn generate_state_msg(&self, msg: ServerStateMessageType<I>) -> ServerStateMessage<I> {
        ServerStateMessage {
            msg,
            member_id: self.member_id
        }
    }
    
    fn notify_client_message(&mut self, msg: ServerStateMessageType<I>) -> Result<(), SendError<ServerStateMessage<I>>> {
        self.to_engine.send(self.generate_state_msg(msg))
    }

    fn notify_network_error(&mut self) -> Result<(), SendError<ServerStateMessage<I>>> {
        self.to_engine.send(self.generate_state_msg(ServerStateMessageType::Disconnected))
    }
}

pub(super) async fn member_loop<I, O>(mut member_data: RemoteServerData<I, O>) where I: DeserializeOwned + fmt::Debug, O: Serialize {
    loop {
        select! {
            Some(to_send) = member_data.from_engine.recv() => {
                if member_data.stream.send(to_send).await.is_err() {
                    member_data.notify_network_error().unwrap();
                    break;
                }
            },
            received = member_data.stream.recv() => match received {
                Some(Ok(msg)) => {
                    member_data.notify_client_message(ServerStateMessageType::Message(msg)).unwrap();
                },
                _ => {
                    member_data.notify_network_error().unwrap();
                    break;
                }
            }
        }
    }
}
