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