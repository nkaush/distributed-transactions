pub mod config;
pub mod stream;

use serde::{Deserialize, Serialize};

pub type Amount = i64;
pub type ClientName = String;
pub type AccountId = String;

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct BalanceDiff(pub Amount);

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ClientRequest {
    WriteBalance(AccountId, BalanceDiff),
    ReadBalance(AccountId),
    Commit,
    Abort
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ClientResponse {
    Ok,
    CommitOk,
    Aborted,
    AbortedNotFound,
    Value(AccountId, Amount)
}

impl ClientResponse {
    pub fn is_err(&self) -> bool {
        matches!(self, Self::Aborted | Self::AbortedNotFound)
    }

    pub fn is_ok(&self) -> bool {
        !matches!(self, Self::Aborted | Self::AbortedNotFound)
    }

    pub fn is_final(&self) -> bool {
        matches!(self, Self::CommitOk | Self::Aborted | Self::AbortedNotFound)
    }

    pub fn format(&self) -> String {
        match self {
            Self::Ok => format!("OK"),
            Self::Value(account_id, balance) => format!("{account_id} = {balance}"),
            Self::CommitOk => format!("COMMIT OK"),
            Self::Aborted => format!("ABORTED"),
            Self::AbortedNotFound => format!("NOT FOUND, ABORTED")
        }
    }
}

#[cfg(test)]
mod test {
    use super::*; 

    #[test]
    fn test_client_response_is_err() {
        assert!(ClientResponse::Aborted.is_err());
        assert!(ClientResponse::AbortedNotFound.is_err());
        assert!(!ClientResponse::Ok.is_err());
        assert!(!ClientResponse::CommitOk.is_err());
        assert!(!ClientResponse::Value("test".into(), 10).is_err());
    }

    #[test]
    fn test_client_response_is_ok() {
        assert!(!ClientResponse::Aborted.is_ok());
        assert!(!ClientResponse::AbortedNotFound.is_ok());
        assert!(ClientResponse::Ok.is_ok());
        assert!(ClientResponse::CommitOk.is_ok());
        assert!(ClientResponse::Value("test".into(), 10).is_ok());
    }
}