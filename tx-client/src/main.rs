use tx_common::config::{Config, parse_config, NodeConfiguration};
use rand::seq::IteratorRandom;
use log::info;

fn main() {
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
}
