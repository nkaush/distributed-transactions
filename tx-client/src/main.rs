use tx_common::{
    ClientRequest::*, ClientResponse, BalanceDiff, stream::MessageStream,
    config::{Config, parse_config, NodeConfiguration}
};
use rand::seq::IteratorRandom;
use log::{error, info};

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

    info!("Connecting to Node {}...", coordinator_cfg.node_id);

    let shard_addr = format!("{}:{}", coordinator_cfg.hostname, coordinator_cfg.port);
    let mut stream = match tokio::net::TcpStream::connect(&shard_addr).await {
        Ok(s) => MessageStream::from_tcp_stream(s),
        Err(e) => {
            eprintln!("Failed to connect to coordinator {} at {}: {e:?}", coordinator_cfg.node_id, shard_addr);
            std::process::exit(1);
        }
    };

    let mut buffer = String::new();
    let mut transaction_started = false;
    while let Ok(_) = std::io::stdin().read_line(&mut buffer) {
        let delimited: Vec <_> = buffer
            .trim()
            .split_ascii_whitespace()
            .collect();

        if !transaction_started {
            if let ["BEGIN"] = delimited[..] {
                transaction_started = true;
            }

            buffer.clear();
            continue
        }

        let request = match delimited[..] {
            ["BALANCE", account_id] => Balance(account_id.into()),
            ["DEPOSIT", account_id, amount] => {
                match amount.parse::<i64>() {
                    Ok(amount) => BalanceChange(account_id.into(), BalanceDiff(amount)),
                    Err(e) => {
                        error!("Failed to parse amount: {e:?}");
                        std::process::exit(1);
                    }
                }
            },
            ["WITHDRAW", account_id, amount] => {
                match amount.parse::<i64>() {
                    Ok(amount) => BalanceChange(account_id.into(), BalanceDiff(amount * -1)),
                    Err(e) => {
                        error!("Failed to parse amount: {e:?}");
                        std::process::exit(1);
                    }
                }
            },
            ["COMMIT"] => Commit,
            ["ABORT"] => Abort,
            _ => {
                error!("Unknown command: `{}`", buffer.trim());
                std::process::exit(1);
            }
        };

        if let Err(e) = stream.send(request).await {
            error!("Failed to send message to coordinator: {e:?}");
            std::process::exit(1);
        }

        let response = match stream.recv::<ClientResponse>().await {
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

        response.print();
        if response.is_final() {
            break;
        }
        
        buffer.clear();
    }
}
