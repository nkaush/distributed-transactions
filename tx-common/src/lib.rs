pub mod config;

use serde::{Deserialize, Serialize};

pub type Amount = i64;
pub type ClientId = String;
pub type AccountId = String;

#[derive(Debug, Deserialize, Serialize)]
pub enum BankTransactionType {
    Deposit,
    Withdraw
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BankTransaction {
    transaction: BankTransactionType,
    amount: Amount,
    client_id: ClientId,
    account_id: AccountId
}