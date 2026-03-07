from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Optional, Union

@dataclass
class MeshConnection:
    transport: str
    interface: Any
    owner_node_num: int
    receive_topic: str


@dataclass
class VirtualNodeConfig:
    node_id: str = "!ffffffff"
    long_name: str = "meshdb virtual node"
    short_name: str = "MDB"
    hw_model: int = 255
    channel: str = "LongFast"
    key: str = "AQ=="
    mcast_group: str = "224.0.0.69"
    mcast_port: int = 4403


def _parse_node_id_to_num(node_id: Union[str, int]) -> int:
    if isinstance(node_id, int):
        return node_id
    text = str(node_id).strip()
    if text.startswith("!"):
        text = text[1:]
    return int(text, 16)


def _connect_serial(serial_port: Optional[str] = None) -> Any:
    from meshtastic.serial_interface import SerialInterface

    kwargs: Dict[str, Any] = {}
    if serial_port:
        kwargs["devPath"] = serial_port
    return SerialInterface(**kwargs)


def _connect_tcp(hostname: str) -> Any:
    from meshtastic.tcp_interface import TCPInterface

    return TCPInterface(hostname=hostname)


def _connect_udp(
    *,
    mcast_group: str,
    mcast_port: int,
    node_id: str,
    channel: str,
    key: str,
    long_name: str,
    short_name: str,
    hw_model: int,
) -> Any:
    from mudp import UDPPacketStream
    from mudp.singleton import node

    node.node_id = node_id if str(node_id).startswith("!") else f"!{node_id}"
    node.long_name = long_name
    node.short_name = short_name
    node.hw_model = int(hw_model)
    node.channel = channel
    node.key = key

    stream = UDPPacketStream(mcast_group, int(mcast_port), key=key, parse_payload=False)
    stream.start()
    return stream


def connect(
    *,
    transport: str = "serial",
    serial_port: Optional[str] = None,
    tcp_host: str = "127.0.0.1:4403",
    virtual_node: Optional[VirtualNodeConfig] = None,
) -> MeshConnection:
    transport = transport.strip().lower()

    if transport == "serial":
        interface = _connect_serial(serial_port)
        owner = _get_connected_num(interface)
        return MeshConnection(
            transport="serial",
            interface=interface,
            owner_node_num=owner,
            receive_topic="meshtastic.receive",
        )

    if transport == "tcp":
        interface = _connect_tcp(tcp_host)
        owner = _get_connected_num(interface)
        return MeshConnection(
            transport="tcp",
            interface=interface,
            owner_node_num=owner,
            receive_topic="meshtastic.receive",
        )

    if transport in ("udp", "mudp"):
        cfg = virtual_node or VirtualNodeConfig()
        node_id = cfg.node_id
        interface = _connect_udp(
            mcast_group=cfg.mcast_group,
            mcast_port=int(cfg.mcast_port),
            node_id=node_id,
            channel=cfg.channel,
            key=cfg.key,
            long_name=cfg.long_name,
            short_name=cfg.short_name,
            hw_model=int(cfg.hw_model),
        )
        owner = _parse_node_id_to_num(node_id)
        return MeshConnection(
            transport="udp",
            interface=interface,
            owner_node_num=owner,
            receive_topic="mesh.rx.packet",
        )

    raise ValueError(f"Unsupported transport={transport!r}. Use serial, tcp, or udp.")


def connect_from_env() -> MeshConnection:
    transport = os.environ.get("MESHDB_TRANSPORT", "serial").strip().lower()
    if transport in ("udp", "mudp"):
        return connect(
            transport="udp",
            virtual_node=VirtualNodeConfig(
                node_id=os.environ.get("MESHDB_VNODE_ID", "!ffffffff"),
                long_name=os.environ.get("MESHDB_VNODE_LONG_NAME", "meshdb virtual node"),
                short_name=os.environ.get("MESHDB_VNODE_SHORT_NAME", "MDB"),
                hw_model=int(os.environ.get("MESHDB_VNODE_HW_MODEL", "255")),
                channel=os.environ.get("MESHDB_MUDP_CHANNEL", "LongFast"),
                key=os.environ.get("MESHDB_MUDP_KEY", "AQ=="),
                mcast_group=os.environ.get("MESHDB_MUDP_GROUP", "224.0.0.69"),
                mcast_port=int(os.environ.get("MESHDB_MUDP_PORT", "4403")),
            ),
        )
    if transport == "tcp":
        return connect(transport="tcp", tcp_host=os.environ.get("MESHDB_TCP_HOST", "127.0.0.1:4403"))
    return connect(transport="serial", serial_port=os.environ.get("MESHDB_SERIAL_PORT"))


def close_connection(connection: MeshConnection) -> None:
    try:
        stop = getattr(connection.interface, "stop", None)
        if callable(stop):
            stop()
    except Exception:
        pass
    try:
        close = getattr(connection.interface, "close", None)
        if callable(close):
            close()
    except Exception:
        pass


def normalize_packet(packet: Any, transport: str) -> Dict[str, Any]:
    if transport not in ("udp", "mudp"):
        if isinstance(packet, dict):
            return packet
        raise TypeError(f"Expected dict packet for transport={transport}, got {type(packet).__name__}")

    from meshtastic import portnums_pb2, protocols
    from google.protobuf.json_format import MessageToDict

    out: Dict[str, Any] = {}
    from_value = getattr(packet, "from", None)
    if from_value is not None:
        out["from"] = int(from_value)
    rx_time = getattr(packet, "rx_time", None)
    if rx_time is not None:
        out["rxTime"] = int(rx_time)
    rx_snr = getattr(packet, "rx_snr", None)
    if rx_snr is not None:
        out["snr"] = float(rx_snr)

    decoded_dict: Dict[str, Any] = {}
    decoded_msg = getattr(packet, "decoded", None)
    if decoded_msg is not None:
        portnum = getattr(decoded_msg, "portnum", None)
        decoded_dict["portnum"] = int(portnum) if portnum is not None else None
        decoded_dict["channel"] = int(getattr(packet, "channel", 0))

        payload = getattr(decoded_msg, "payload", b"") or b""
        if isinstance(payload, str):
            payload = payload.encode("utf-8", errors="replace")

        if portnum == portnums_pb2.PortNum.TEXT_MESSAGE_APP:
            try:
                decoded_dict["text"] = bytes(payload).decode("utf-8", errors="replace")
            except Exception:
                decoded_dict["payload"] = payload
        else:
            handler = protocols.get(portnum) if portnum is not None else None
            factory = getattr(handler, "protobufFactory", None) if handler is not None else None
            if callable(factory):
                try:
                    msg = factory()
                    msg.ParseFromString(bytes(payload))
                    pb = MessageToDict(msg, preserving_proto_field_name=False, use_integers_for_enums=False)
                    if portnum == portnums_pb2.PortNum.NODEINFO_APP:
                        decoded_dict["user"] = pb
                    elif portnum == portnums_pb2.PortNum.POSITION_APP:
                        decoded_dict["position"] = pb
                    elif portnum == portnums_pb2.PortNum.TELEMETRY_APP:
                        decoded_dict["telemetry"] = pb
                    else:
                        decoded_dict["payload"] = pb
                except Exception:
                    decoded_dict["payload"] = bytes(payload)
            else:
                decoded_dict["payload"] = bytes(payload)

    out["decoded"] = decoded_dict
    return out


def _get_connected_num(interface: Any) -> int:
    from meshdb.db_handler import get_connected_device_node_num

    num = get_connected_device_node_num(interface)
    if isinstance(num, int):
        return num
    return 0
