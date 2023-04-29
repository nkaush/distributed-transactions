NODE_COUNT = 5

HEADER = """version: '3.3'

services:"""

FMT = """  shard-{nodeid_lower}:
    container_name: shard-{nodeid_lower}
    build: 
      context: .
      dockerfile: Dockerfile.server
    environment:
      - RUST_LOG=tx_server=trace
      - NODEID={nodeid_upper}
    volumes:
      - ./config/docker.config:/service/cfg/config
    ports:
      - {port}:{port}
    command: ./server {nodeid_upper} /service/cfg/config"""

print(HEADER)
for n in range(NODE_COUNT):
    nodeid_lower = chr(97 + n)
    nodeid_upper = chr(65 + n)
    port = 10000 + n
    print(
        FMT.format(
            nodeid_lower=nodeid_lower, 
            nodeid_upper=nodeid_upper, 
            port=port
        )
    )