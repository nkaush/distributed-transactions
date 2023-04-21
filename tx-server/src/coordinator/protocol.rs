use tx_common::{ClientRequest, ClientResponse, config::NodeId};
use serde::{Deserialize, Serialize};
use crate::sharding::TransactionId;

/// This enum indicates to the server how to forward a message.
pub enum ForwardTarget {
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
pub enum ClientState {
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
pub enum Forwarded {
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
pub enum CommitStatus {
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
pub enum TargetShard {
    /// Indicates that the object is associated with a shard not located on the
    /// server that is coordinating the transaction requesting the object.
    Remote(NodeId),
    /// Indicates that the object is associated with the server that is 
    /// coordinating the transaction requesting the object.
    Local,
    /// Indicates that the first letter of the object is not a valid shard name. 
    DoesNotExist
}