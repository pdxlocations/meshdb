"""Store and query one channel, including the connected radio's initial NodeDB.

Edit the settings below to match your radio. UDP mode is a passive collector;
use vnode when you need a transmitting Meshtastic 2.8 identity.
"""

import base64
import json
import os
import time

from pubsub import pub
from meshtastic.util import generate_channel_hash
import meshdb

DB_BASE = os.getcwd()
TRANSPORT = "serial"  # "serial", "tcp", or "udp"
SERIAL_PORT = None
TCP_HOST = "127.0.0.1:4403"  # IPv6 with a port: "[::1]:4403"
CHANNEL_NAME = "LongFast"
CHANNEL_KEY = "AQ=="
CHANNEL_INDEX = 0  # Radio channel index; distinct from its UDP wire hash.
TARGETS = ["Meshtastic 1adc", "1adc", "SenseRAT", "FONE"]
DEMO_METRIC = "hw_model"


def json_default(value):
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(value).decode("ascii")
    raise TypeError(f"Cannot encode {type(value).__name__}")


def run_demo(connection, db_base=DB_BASE, targets=TARGETS):
    owner = connection.owner_node_num
    scope = {
        "db_path": db_base,
        "channel": generate_channel_hash(CHANNEL_NAME, CHANNEL_KEY),
        "channel_name": CHANNEL_NAME,
    }
    # These are the same scope arguments used for both writes and lookups.
    if connection.transport != "udp":
        count = meshdb.sync_nodes_from_interface(owner, connection.interface, **scope)
        print(f"Imported {count} radio node records")
    for identifier in targets:
        number = meshdb.get_node_num(identifier, owner_node_num=owner, **scope)
        snapshot = meshdb.get_node(identifier, owner_node_num=owner, **scope)
        metric = meshdb.get_node_metric(
            identifier, DEMO_METRIC, owner_node_num=owner, **scope
        )
        print(f"{identifier!r}: number={number}, {DEMO_METRIC}={metric}")
        print(json.dumps(snapshot, indent=2, ensure_ascii=False, default=json_default))

    def on_receive(packet=None, interface=None, addr=None):
        if connection.transport != "udp" and interface is not connection.interface:
            return
        normalized = meshdb.normalize_packet(packet, connection.transport)
        packet_channel = normalized.get("channel", 0)
        expected_channel = (
            scope["channel"] if connection.transport == "udp" else CHANNEL_INDEX
        )
        if not normalized.get("pkiEncrypted") and packet_channel != expected_channel:
            return  # This example intentionally stores only the selected channel.
        result = meshdb.handle_packet(normalized, node_database_number=owner, **scope)
        sender = normalized.get("from")
        name = meshdb.get_long_name(sender, node_database_number=owner, **scope)
        print(f"saved={result} from={sender} long={name!r}")

    pub.subscribe(on_receive, connection.receive_topic)
    print(f"Listening on {connection.receive_topic}; Ctrl+C to exit")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        pub.unsubscribe(on_receive, connection.receive_topic)
        meshdb.close_connection(connection)


def main():
    os.makedirs(DB_BASE, exist_ok=True)
    connection = meshdb.connect(
        transport=TRANSPORT,
        serial_port=SERIAL_PORT,
        tcp_host=TCP_HOST,
        virtual_node=meshdb.VirtualNodeConfig(
            channel=CHANNEL_NAME,
            key=CHANNEL_KEY,
            mcast_group="239.0.0.69",
            mcast_port=4403,
        ),
    )
    run_demo(connection)


if __name__ == "__main__":
    main()
