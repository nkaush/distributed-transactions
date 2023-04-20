pub mod config;

use serde::{Deserialize, Serialize};

pub type Amount = i64;
pub type ClientName = String;
pub type AccountId = String;

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct BalanceDiff(pub Amount);

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum RequestType {
    BalanceChange(AccountId, BalanceDiff),
    Balance(AccountId),
    Commit,
    Abort
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ClientRequest {
    pub rqty: RequestType,
    pub id: ClientName,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ClientResponse {
    Ok,
    CommitOk,
    Aborted,
    AbortedNotFound,
    Value(AccountId, Amount)
}
