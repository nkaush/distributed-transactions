use tokio::{
    sync::mpsc::{UnboundedSender, UnboundedReceiver, error::SendError}, 
    task::JoinHandle, select
};
use serde::{de::DeserializeOwned, Serialize};
use super::{NodeId, stream::MessageStream};
use log::trace;
use std::fmt;

/// Represents any message types a member handler thread could send the transaction engine
#[derive(Debug)]
pub enum MemberStateMessageType<M> {
    Message(M),
    NetworkError
}

/// Represents any messages a member handler thread could send the multicast engine.
#[derive(Debug)]
pub struct MemberStateMessage<M> {
    pub msg: MemberStateMessageType<M>,
    pub member_id: NodeId
}

/// The handle that the multicast engine has for each member handler thread.
pub struct MulticastMemberHandle<M> {
    pub member_id: NodeId,
    pub to_client: UnboundedSender<M>,
    pub handle: JoinHandle<()>
}

impl<M> MulticastMemberHandle<M> {
    pub fn pass_message(&self, msg: M) -> Result<(), SendError<M>> {
        self.to_client.send(msg)
    }
}

impl<M> Drop for MulticastMemberHandle<M> {
    fn drop(&mut self) {
        trace!("Aborting client thread for {}", self.member_id);
        self.handle.abort()
    }
}

pub(super) struct MulticastMemberData<M> {
    pub member_id: NodeId,
    pub stream: MessageStream<M, M>,
    pub to_engine: UnboundedSender<MemberStateMessage<M>>,
    pub from_engine: UnboundedReceiver<M>
}

impl<M> MulticastMemberData<M> {
    fn generate_state_msg(&self, msg: MemberStateMessageType<M>) -> MemberStateMessage<M> {
        MemberStateMessage {
            msg,
            member_id: self.member_id
        }
    }
    
    fn notify_client_message(&mut self, msg: MemberStateMessageType<M>) -> Result<(), SendError<MemberStateMessage<M>>> {
        self.to_engine.send(self.generate_state_msg(msg))
    }

    fn notify_network_error(&mut self) -> Result<(), SendError<MemberStateMessage<M>>> {
        self.to_engine.send(self.generate_state_msg(MemberStateMessageType::NetworkError))
    }
}

pub(super) async fn member_loop<M>(mut member_data: MulticastMemberData<M>) where M: DeserializeOwned + Serialize + fmt::Debug {
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
                    member_data.notify_client_message(MemberStateMessageType::Message(msg)).unwrap();
                },
                _ => {
                    member_data.notify_network_error().unwrap();
                    break;
                }
            }
        }
    }
}
