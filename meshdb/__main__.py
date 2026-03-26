#!/usr/bin/env python3
"""CLI: listen for Meshtastic packets and persist them into meshdb.

Usage:
  python -m meshdb
  python -m meshdb --db ./data --transport tcp --tcp-host 127.0.0.1:4403
  python -m meshdb --transport udp --node-id !89abcdef --channel LongFast
"""
from __future__ import annotations

import argparse
import os
import time

from meshdb import (
    VirtualNodeConfig,
    close_connection,
    connect,
    get_long_name,
    get_short_name,
    handle_packet,
    normalize_packet,
    set_default_db_path,
)

DEFAULT_CHANNEL_NAME = "LongFast"
DEFAULT_CHANNEL_KEY = "AQ=="


def _configured_channel_hash(channel_name: str, key: str) -> int | None:
    try:
        from meshtastic.util import generate_channel_hash

        return int(generate_channel_hash(channel_name, key))
    except Exception:
        return None


def _interface_channels_with_hash(interface) -> list[dict]:
    try:
        local_node = getattr(interface, "localNode", None)
        get_channels_with_hash = getattr(local_node, "get_channels_with_hash", None)
        if not callable(get_channels_with_hash):
            return []
        return [channel for channel in (get_channels_with_hash() or []) if isinstance(channel, dict)]
    except Exception:
        return []


def _interface_channel_hashes(interface) -> dict[int, int]:
    mapping: dict[int, int] = {}
    for channel in _interface_channels_with_hash(interface):
        try:
            index = int(channel.get("index"))
            hash_value = int(channel.get("hash"))
        except (TypeError, ValueError):
            continue
        mapping[index] = hash_value
    return mapping


def _default_interface_channel_hash(interface, preferred_name: str | None = None) -> int | None:
    channels = _interface_channels_with_hash(interface)
    if not channels:
        return None

    if preferred_name:
        for channel in channels:
            try:
                hash_value = int(channel.get("hash"))
            except (TypeError, ValueError):
                continue
            if str(channel.get("name") or "").strip() == preferred_name:
                return hash_value

    for channel in channels:
        try:
            hash_value = int(channel.get("hash"))
        except (TypeError, ValueError):
            continue
        if str(channel.get("role") or "").upper() == "PRIMARY":
            return hash_value

    for channel in channels:
        try:
            return int(channel.get("hash"))
        except (TypeError, ValueError):
            continue
    return None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Listen for packets and persist them into meshdb.")
    parser.add_argument("--db", dest="db_base", default=os.getcwd(), help="DB base path. Defaults to the working directory.")
    parser.add_argument(
        "--transport",
        choices=["serial", "tcp", "udp"],
        default="serial",
        help="Connection transport. Defaults to serial.",
    )
    parser.add_argument("--serial-port", default=None, help="Serial device path for serial transport.")
    parser.add_argument("--tcp-host", default="127.0.0.1:4403", help="TCP host:port for tcp transport.")
    parser.add_argument("--owner", dest="owner_node_num", type=int, default=None, help="Override DB owner node number.")
    parser.add_argument("--node-id", default="!ffffffff", help="Virtual node id for udp transport.")
    parser.add_argument("--long-name", default="meshdb virtual node", help="Virtual node long name for udp transport.")
    parser.add_argument("--short-name", default="MDB", help="Virtual node short name for udp transport.")
    parser.add_argument("--hw-model", type=int, default=255, help="Virtual node hardware model for udp transport.")
    parser.add_argument("--channel", default=DEFAULT_CHANNEL_NAME, help="Virtual node channel name for udp transport.")
    parser.add_argument("--key", default=DEFAULT_CHANNEL_KEY, help="Virtual node channel key for udp transport.")
    parser.add_argument("--mcast-group", default="224.0.0.69", help="UDP multicast group for udp transport.")
    parser.add_argument("--mcast-port", type=int, default=4403, help="UDP multicast port for udp transport.")
    return parser.parse_args()


def start() -> None:
    from pubsub import pub

    args = _parse_args()
    set_default_db_path(args.db_base)

    connection = connect(
        transport=args.transport,
        serial_port=args.serial_port,
        tcp_host=args.tcp_host,
        virtual_node=VirtualNodeConfig(
            node_id=args.node_id,
            long_name=args.long_name,
            short_name=args.short_name,
            hw_model=args.hw_model,
            channel=args.channel,
            key=args.key,
            mcast_group=args.mcast_group,
            mcast_port=args.mcast_port,
        ),
    )
    owner_node_num = args.owner_node_num if args.owner_node_num is not None else connection.owner_node_num
    configured_channel_hash = _configured_channel_hash(args.channel, args.key)
    explicit_channel_config = args.channel != DEFAULT_CHANNEL_NAME or args.key != DEFAULT_CHANNEL_KEY
    interface_default_channel_hash = _default_interface_channel_hash(
        connection.interface,
        preferred_name=args.channel if explicit_channel_config else None,
    )

    def on_receive(packet=None, interface=None, addr=None) -> None:
        try:
            normalized = normalize_packet(packet, connection.transport)
            raw_channel = normalized.get("channel")
            decoded_channel = normalized.get("decoded", {}).get("channel")
            packet_channel = raw_channel if raw_channel is not None else decoded_channel

            storage_channel = None
            channel_hashes = _interface_channel_hashes(connection.interface)
            try:
                packet_channel_int = int(packet_channel) if packet_channel is not None else None
            except (TypeError, ValueError):
                packet_channel_int = None

            if packet_channel_int is not None:
                storage_channel = channel_hashes.get(packet_channel_int)
                if storage_channel is None and packet_channel_int > 7:
                    storage_channel = packet_channel_int

            if storage_channel is None:
                if connection.transport == "udp":
                    storage_channel = configured_channel_hash
                else:
                    storage_channel = interface_default_channel_hash or configured_channel_hash

            result = handle_packet(
                normalized,
                node_database_number=owner_node_num,
                channel=storage_channel,
                channel_name=args.channel,
            )
            sender = normalized.get("from")
            long_name = (
                get_long_name(sender, node_database_number=owner_node_num, channel=storage_channel)
                if sender is not None
                else "Unknown"
            )
            short_name = (
                get_short_name(sender, node_database_number=owner_node_num, channel=storage_channel)
                if sender is not None
                else "Unknown"
            )
            port = normalized.get("decoded", {}).get("portnum")
            print(
                f"saved={result} from={sender} channel={packet_channel} storage_channel={storage_channel} "
                f"long='{long_name}' short='{short_name}' port={port}"
            )
        except Exception as exc:
            print(f"on_receive error: {exc}")

    pub.subscribe(on_receive, connection.receive_topic)
    print(
        f"[meshdb] listening transport={connection.transport} owner={owner_node_num} "
        f"db_base={args.db_base} topic={connection.receive_topic} "
        f"configured_channel_hash={configured_channel_hash} interface_default_channel_hash={interface_default_channel_hash}"
    )

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        close_connection(connection)


if __name__ == "__main__":
    start()
