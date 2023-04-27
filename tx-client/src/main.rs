use tx_common::{
    ClientRequest::*, ClientResponse, BalanceDiff, stream::MessageStream,
    config::{Config, parse_config, NodeConfiguration}
};
use rand::seq::IteratorRandom;
use log::{error, trace};

#[tokio::main]
async fn main() {
    env_logger::init();
    let args: Vec<_> = std::env::args().collect();

    if args.len() != 3 {
        eprintln!("Usage: {} <client identifier> <path to config file>", args[0]);
        std::process::exit(1);
    }

    let config: Config = match parse_config(&args[2]) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{}: {}", &args[0], e);
            std::process::exit(1);
        }
    };

    let mut rng = rand::thread_rng();
    let coordinator_cfg: &NodeConfiguration = config.values().choose(&mut rng).unwrap();
    let shard_addr = format!("{}:{}", coordinator_cfg.hostname, coordinator_cfg.port);

    let mut buffer = String::new();
    while std::io::stdin().read_line(&mut buffer).is_ok() {
        let delimited: Vec <_> = buffer
            .trim()
            .split_ascii_whitespace()
            .collect();
        
        if let ["BEGIN"] = delimited[..] {
            println!("OK");
            buffer.clear();
            break
        } else {
            trace!("Transaction has not started. Ignoring input `{}`", buffer.trim());
        }

        buffer.clear();
    }

    trace!("Connecting to Node {}...", coordinator_cfg.node_id);
    let mut stream = match tokio::net::TcpStream::connect(&shard_addr).await {
        Ok(s) => MessageStream::from_tcp_stream(s),
        Err(e) => {
            eprintln!("Failed to connect to coordinator {} ({}): {}", coordinator_cfg.node_id, shard_addr, e.to_string());
            std::process::exit(1);
        }
    };

    while std::io::stdin().read_line(&mut buffer).is_ok() {
        let delimited: Vec <_> = buffer
            .trim()
            .split_ascii_whitespace()
            .collect();

        let request = match delimited[..] {
            ["BALANCE", account_id] => ReadBalance(account_id.into()),
            ["DEPOSIT", account_id, amount] => {
                match amount.parse::<i64>() {
                    Ok(amount) => WriteBalance(account_id.into(), BalanceDiff(amount)),
                    Err(e) => {
                        error!("ABORTING! Failed to parse amount: {e:?}");
                        Abort
                    }
                }
            },
            ["WITHDRAW", account_id, amount] => {
                match amount.parse::<i64>() {
                    Ok(amount) => WriteBalance(account_id.into(), BalanceDiff(amount * -1)),
                    Err(e) => {
                        error!("ABORTING! Failed to parse amount: {e:?}");
                        Abort
                    }
                }
            },
            ["COMMIT"] => Commit,
            ["ABORT"] => Abort,
            _ => {
                error!("ABORTING! Unknown command: `{}`", buffer.trim());
                Abort
            }
        };

        trace!("Sending command to coordinator: {request:?}");
        if let Err(e) = stream.send(request).await {
            error!("Failed to send message to coordinator: {e:?}");
            std::process::exit(1);
        }

        let response: ClientResponse = match stream.recv().await {
            Some(Ok(response)) => response,
            Some(Err(e)) => {
                error!("Error on receiving response: {e:?}");
                std::process::exit(1);
            },
            None => {
                error!("Error on receiving response: other half closed");
                std::process::exit(1);
            }
        };

        println!("{}", response.format());
        if response.is_final() {
            break;
        }
        
        buffer.clear();
    }
}
