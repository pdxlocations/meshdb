

# meshdb

A lightweight Python library for storing Meshtastic node, telemetry, and message data in per-node SQLite databases.

## Quick Start

```python
from meshdb import handle_packet, set_default_db_path

set_default_db_path("./data")

# example packet (decoded from Meshtastic interface)
packet = {
    "from": 12345678,
    "rxTime": 1700000000,
    "decoded": {"portnum": "NODEINFO_APP", "user": {"longName": "TestNode"}}
}

handle_packet(packet, owner_node_num=12345678)
```

Run `python -m meshdb` to view all known nodes and their latest telemetry in JSON format.