use tx_common::config::{Config, parse_config, NodeConfiguration};
use rand::{thread_rng, Rng};
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

    let coordinator_index = thread_rng().gen::<usize>() % config.len();
    let coordinator_id = config.keys().nth(coordinator_index).unwrap();
    let coordinator_cfg: &NodeConfiguration = config.get(coordinator_id).unwrap();

    info!("Connecting to Node {}...", coordinator_cfg.node_id);
}
