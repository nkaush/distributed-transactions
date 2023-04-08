use std::io::{BufRead, BufReader};
use std::collections::HashMap;
use std::fs::File;

pub type NodeId = char;

#[derive(Clone)]
pub struct NodeConfiguration {
    pub node_id: NodeId,
    pub hostname: String,
    pub port: u16,
    pub connection_list: Vec<NodeId>
}

impl NodeConfiguration {
    fn new(node_id: NodeId, hostname: String, port: u16, connection_list: Vec<NodeId>) -> Self {
        Self {
            node_id,
            hostname,
            port,
            connection_list
        }
    }
}

pub type Config = HashMap<NodeId, NodeConfiguration>;

pub fn parse_config(path: &str) -> Result<Config, String> {
    let mut config: HashMap<NodeId, NodeConfiguration> = Config::new();
    let mut rdr = match File::open(path) {
        Ok(f) => BufReader::new(f),
        Err(e) => return Err(e.to_string())
    };
    
    let mut buf = String::new();
    let mut nodes = Vec::new();

    while let Ok(n) = rdr.read_line(&mut buf) {
        if n == 0 { break }

        let delimited: Vec<_> = buf.split_ascii_whitespace().collect();
        match delimited[0..3] {
            [node_name, hostname, p] => match p.parse() {
                Ok(port) => {
                    let node_name = if node_name.len() == 1 {
                        node_name.chars().nth(0).unwrap()
                    } else {
                        return Err("Node name must be a character".into());
                    };

                    config.insert(node_name, NodeConfiguration::new(node_name, hostname.into(), port, nodes.clone()));
                    nodes.push(node_name);
                },
                Err(_) => return Err(format!("Bad config: could not parse port for node with id: {}", n))
            },
            _ => return Err("Bad config: too little arguments per line".into())
        };

        buf.clear();
    }

    Ok(config)
}
