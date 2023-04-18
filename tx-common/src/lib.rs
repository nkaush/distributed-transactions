pub mod config;

pub type Balance = i64;

pub enum BankTransaction {
    Deposit(Balance),
    Withdraw(Balance)
}