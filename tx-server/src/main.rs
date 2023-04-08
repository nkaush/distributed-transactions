use server::pool::{config::{NodeId, Config, parse_config}, ConnectionPool};

#[tokio::main]
async fn main() {
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
    let pool = ConnectionPool::<i32>::new(node_id)
        .connect(&config)
        .await;
}
