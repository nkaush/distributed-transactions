use tx_common::config::{self, NodeId, Config};
use tx_server::pool::{ConnectionPool, pipe::unbounded_pipe, stream::MessageStream};
use log::error;

pub fn parse_config(path: &str, given_node_name: char) -> Result<Config, String> {
    match config::parse_config(path) {
        Ok(c) => {
            if c.contains_key(&given_node_name) {
                Ok(c)
            } else {
                return Err(format!("Bad config: node identifier is not listed in config file"));
            }
        },
        Err(e) => return Err(e)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: {} <node identifier> <path to config file>", args[0]);
        std::process::exit(1);
    } else if args[1].len() != 1 {
        eprintln!("{}: Node identifier must be a single character", args[0]);
        std::process::exit(1);
    }

    let node_id: NodeId = args[1].chars().nth(0).unwrap();
    let config: Config = match parse_config(&args[2], node_id) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("{}: {}", args[0], e);
            std::process::exit(1);
        }
    };
    let (send, rcv) = unbounded_pipe();
    let stream = MessageStream::from_local(send);

    let mut this = ConnectionPool::<i32>::new(config, node_id)
        .await
        .unwrap_or_else(|e| {
            error!("Unable to construct connection pool: {e}");
            std::process::exit(1);
        })
        .connect()
        .await;

    this.admit_member(stream, node_id);

    Ok(())
}
