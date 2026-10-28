import contextlib
import importlib.util
import io
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from pubsub import pub
from meshtastic.util import generate_channel_hash
import meshdb
from meshdb.network import _connect_tcp


def load_demo():
    path = Path(__file__).resolve().parents[1] / "examples/meshdb_demo.py"
    spec = importlib.util.spec_from_file_location("meshdb_demo_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TcpAddressTests(unittest.TestCase):
    def test_host_port_and_ipv6_are_passed_separately(self):
        cases = [
            ("radio.local", "radio.local", 4403),
            ("192.168.1.2:4404", "192.168.1.2", 4404),
            ("[::1]:4404", "::1", 4404),
            ("::1", "::1", 4403),
            ("[fe80::1%en0]:4403", "fe80::1%en0", 4403),
        ]
        for address, host, port in cases:
            with (
                self.subTest(address=address),
                patch("meshtastic.tcp_interface.TCPInterface") as interface,
            ):
                _connect_tcp(address)
                interface.assert_called_once_with(hostname=host, portNumber=port)

    def test_invalid_addresses_do_not_connect(self):
        for address in [
            "",
            "host:0",
            "host:65536",
            "host:abc",
            "host:",
            "[::1",
            "user@host:4403",
            "host/path",
        ]:
            with (
                self.subTest(address=address),
                patch("meshtastic.tcp_interface.TCPInterface") as interface,
            ):
                with self.assertRaises(ValueError):
                    _connect_tcp(address)
                interface.assert_not_called()


class DemoTests(unittest.TestCase):
    def test_sync_query_and_live_storage_share_channel_scope(self):
        demo = load_demo()
        peer = {
            "num": 123,
            "user": {"longName": "Peer", "shortName": "PEER", "hwModel": "HELTEC_V3"},
        }
        interface = SimpleNamespace(nodes={123: peer}, stop=Mock())
        connection = meshdb.MeshConnection(
            "serial", interface, 456, "meshdb.tests.demo"
        )
        packet = {
            "from": 123,
            "to": 0xFFFFFFFF,
            "channel": 0,
            "decoded": {"portnum": "TEXT_MESSAGE_APP", "text": "live message"},
        }

        def deliver_then_stop(_):
            pub.sendMessage(
                connection.receive_topic, packet=packet, interface=interface
            )
            raise KeyboardInterrupt

        with (
            tempfile.TemporaryDirectory() as root,
            contextlib.redirect_stdout(io.StringIO()) as output,
        ):
            with patch.object(demo.time, "sleep", side_effect=deliver_then_stop):
                demo.run_demo(connection, root, ["Peer"])
            channel = generate_channel_hash(demo.CHANNEL_NAME, demo.CHANNEL_KEY)
            row = meshdb.get_nodeinfo(
                "Peer", owner_node_num=456, db_path=root, channel=channel
            )
            self.assertEqual(row["node_num"], "123")
            from meshdb.db_handler import MessageDB

            db = MessageDB(456, root, channel=channel)
            self.assertEqual(
                db.load_channel_messages(channel)[0]["message_text"], "live message"
            )
            self.assertIn("Imported 1 radio node records", output.getvalue())
            self.assertIn("number=123", output.getvalue())
            self.assertIn("long='Peer'", output.getvalue())
        interface.stop.assert_called_once()

    def test_snapshot_binary_metadata_has_json_representation(self):
        import json

        demo = load_demo()
        self.assertEqual(
            json.dumps({"raw_payload": b"hello"}, default=demo.json_default),
            '{"raw_payload": "aGVsbG8="}',
        )
