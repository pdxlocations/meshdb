# meshdb

A lightweight Python library for storing Meshtastic node, telemetry, and message data in per-node SQLite databases.

## Installation

You can install `meshdb` directly from PyPI using pip:

```bash
pip install meshdb
```

Or install from source within a virtual environment:

```bash
git clone https://github.com/pdxlocations/meshdb.git
cd meshdb
pip install -e .
```

Or install via Poetry:

```bash
poetry install
```

## Quick Usage Example

Set a default database path and handle incoming packets:

```python
from meshdb import handle_packet, set_default_db_path

set_default_db_path("./data")

packet = {
    "from": 12345678,
    "rxTime": 1700000000,
    "decoded": {
        "portnum": "NODEINFO_APP",
        "user": {"longName": "TestNode"}
    }
}

handle_packet(packet, node_database_number=12345678)
```

## Network Connection Options (Serial/TCP/Virtual Node)

`meshdb` now includes a connection helper that supports:
- `serial` (default)
- `tcp`
- `udp` virtual-node mode

```python
import meshdb

conn = meshdb.connect(
    transport="udp",
    virtual_node=meshdb.VirtualNodeConfig(
        node_id="!89abcdef",
        long_name="MeshDB Virtual Node",
        short_name="MDB",
        channel="LongFast",
        key="AQ==",
        mcast_group="224.0.0.69",
        mcast_port=4403,
    ),
)
print(conn.transport, conn.owner_node_num)
```

Optional environment-based shortcut is still available:

```bash
export MESHDB_TRANSPORT=udp
export MESHDB_VNODE_ID=!89abcdef
export MESHDB_VNODE_LONG_NAME="MeshDB Virtual Node"
export MESHDB_VNODE_SHORT_NAME=MDB
export MESHDB_MUDP_GROUP=224.0.0.69
export MESHDB_MUDP_PORT=4403
export MESHDB_MUDP_CHANNEL=LongFast
export MESHDB_MUDP_KEY=AQ==
```

Then call:

```python
conn = meshdb.connect_from_env()
```

When receiving packets from `udp`, use `meshdb.normalize_packet(packet, "udp")`
before passing to `meshdb.handle_packet(...)`.

## Viewing Stored Data

You can run:

```bash
python -m meshdb --db ./data
```

This prints a JSON summary of all known nodes and their latest telemetry, if available.

## Lookups in Code

```python
import meshdb

node = meshdb.get_node(12345678, owner_node_num=12345678)
battery = meshdb.get_node_metric("TestNode", "battery_level", owner_node_num=12345678)
```

## Project Status

Early development. Schema and API changes may occur.
