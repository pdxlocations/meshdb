import os
import time
import json
from typing import List
from pubsub import pub

import meshtastic.serial_interface
import meshdb

"""
Basic MeshDB integration demo
=============================

How to customize:
  1) Set DB_BASE to where you want your per-owner DB files written.
  2) Edit TARGETS to the nodes you want to query (id/hex suffix/short/long/numeric).
  3) Run the script while a Meshtastic device is attached over USB/serial.

What this demo shows:
  - Resolving node numbers from many identifier forms (hex suffix, names, numeric)
  - Pretty-printing a full node snapshot (nodeinfo + latest telemetry + latest position)
  - Fetching a single metric (e.g., battery_level) via a convenience helper
  - Live packet handling that persists NODEINFO/POSITION/TELEMETRY/TEXT_MESSAGE
  - Auto-syncing the device's NodeDB into your local SQL DB on connect
"""

# -----------------------------
# 1) Easy knobs
# -----------------------------
# Where to store DB files. Examples:
#   "~/meshdb_data"            → creates <owner>.db files in that directory
#   "./mesh.sqlite3"          → creates mesh.<owner>.sqlite3 alongside this script
#   None or ""                 → current working directory
DB_BASE = os.environ.get("MESHDB_BASE", "~/Meshtastic/github/pdxlocations/meshdb/")

# Nodes to query for examples. You can use any of:
#   - "!deadbeef" (full hex id)
#   - "deadbeef" or "1adc" (hex suffix)
#   - "FONE" (short name)
#   - "New Phone Who Dis?" (long name)
#   - 4062650989 (node number)
TARGETS: List[object] = [
    "Meshtastic 1adc",  # free text with hex suffix
    "1adc",  # hex suffix
    "SenseRAT",  # long name
    "FONE",  # short name
]

# A single metric to fetch for demonstration
DEMO_METRIC = os.environ.get("MESHDB_METRIC", "hw_model")  # e.g., "battery_level"

# -----------------------------
# 2) Configure MeshDB base path
# -----------------------------
meshdb.set_default_db_path(DB_BASE)
print(f"[meshdb] DB base set to: {DB_BASE}")

# -----------------------------
# 3) Connect to device (Serial)
# -----------------------------
# You can also use meshtastic.tcp_interface.TCPInterface(hostname="127.0.0.1:4403")
interface = meshtastic.serial_interface.SerialInterface()

# Resolve connected device node number AND sync its NodeDB into our local DB
CONNECTED_NODE_NUM = meshdb.get_connected_device_node_num(interface)
if CONNECTED_NODE_NUM is None:
    print("[meshdb] Warning: Could not resolve connected device node number; falling back to 0.")
    CONNECTED_NODE_NUM = 0
else:
    print(f"[meshdb] Connected device node number: {CONNECTED_NODE_NUM}")

# -----------------------------
# 4) Helper to pretty print JSON
# -----------------------------


def jprint(obj):
    print(json.dumps(obj, indent=2, sort_keys=True, ensure_ascii=False))


# -----------------------------
# 5) Show resolution examples
# -----------------------------
print("\n=== Node number resolution examples ===")
for ident in TARGETS:
    resolved = meshdb.get_node_num(ident, owner_node_num=CONNECTED_NODE_NUM)
    print(f"identifier= {ident!r} → node_num= {resolved}")

# -----------------------------
# 6) Show full snapshots for targets
# -----------------------------
print("\n=== Full node snapshots (nodeinfo + latest telemetry + latest position) ===")
for ident in TARGETS:
    snap = meshdb.get_node(ident, owner_node_num=CONNECTED_NODE_NUM)
    if snap is None:
        print(f"snapshot for {ident!r}: NOT FOUND")
    else:
        print(f"\n# snapshot for {ident!r}")
        jprint(snap)

# -----------------------------
# 7) Show a single metric value (first-found subtype priority)
# -----------------------------
print("\n=== Single metric lookup ===")
for ident in TARGETS:
    val = meshdb.get_node_metric(ident, DEMO_METRIC, owner_node_num=CONNECTED_NODE_NUM)
    print(f"{DEMO_METRIC} for {ident!r} → {val}")

# Bonus: show a NodeInfo field via metric helper

val = meshdb.get_node_metric("SenseRAT", "hw_model", owner_node_num=CONNECTED_NODE_NUM)
print(f"hw_model for 'SenseRAT' → {val}")

# -----------------------------
# 8) Live receive: store packets and optionally observe
# -----------------------------


def on_receive(packet=None, interface=None):
    """Store NODEINFO/POSITION/TELEMETRY/TEXT_MESSAGE into the DB automatically."""
    try:
        result = meshdb.handle_packet(packet, node_database_number=CONNECTED_NODE_NUM)

        # Example: derive readable sender names
        sender = packet.get("from")
        ln = meshdb.get_long_name(sender, node_database_number=CONNECTED_NODE_NUM)
        sn = meshdb.get_short_name(sender, node_database_number=CONNECTED_NODE_NUM)
        print(f"saved={result} from={sender} long='{ln}' short='{sn}' port={packet.get('decoded',{}).get('portnum')}")

    except Exception as e:
        print(f"on_receive error: {e}")


# Hook PubSub topic and idle forever
pub.subscribe(on_receive, "meshtastic.receive")
print("\n[meshdb] Listening for packets… (Ctrl+C to exit)")
while True:
    time.sleep(1)
