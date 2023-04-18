pub mod config;

pub enum BankTransaction {
    Deposit(i64),
    Withdraw(i64)
}