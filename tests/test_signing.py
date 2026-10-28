import base64
import tempfile
import unittest

from meshtastic import mesh_pb2
from meshdb.network import normalize_packet, VirtualNodeConfig
from meshdb.db_handler import NodeDB, MessageDB, handle_packet


class SigningMetadataTests(unittest.TestCase):
    def test_udp_preserves_signature_and_envelope_without_trusting_flag(self):
        packet = mesh_pb2.MeshPacket(id=42, to=0xFFFFFFFF, channel=15)
        setattr(packet, "from", 123)
        data = packet.decoded
        data.portnum = 1
        data.payload = b"hello"
        data.request_id, data.reply_id, data.emoji = 9, 8, 1
        data.want_response = True
        data.bitfield = 0
        signature = bytes(range(64))
        data.MergeFromString(b"\x52\x40" + signature)
        if "xeddsa_signed" in packet.DESCRIPTOR.fields_by_name:
            packet.xeddsa_signed = True
        normalized = normalize_packet(packet, "udp")
        self.assertFalse(normalized["xeddsaSigned"])
        self.assertEqual(normalized["decoded"]["payload"], b"hello")
        self.assertEqual(
            normalized["decoded"]["xeddsaSignature"],
            base64.b64encode(signature).decode(),
        )
        self.assertEqual(normalized["decoded"]["requestId"], 9)
        self.assertEqual(normalized["decoded"]["replyId"], 8)
        self.assertEqual(normalized["decoded"]["bitfield"], 0)
        with tempfile.TemporaryDirectory() as root:
            handle_packet(normalized, node_database_number=456, db_path=root)
            db = MessageDB(456, root, channel=15)
            with db.connect() as con:
                row = con.execute(
                    "SELECT xeddsa_signature, xeddsa_signed, raw_payload, request_id, reply_id, bitfield FROM channel_messages"
                ).fetchone()
            self.assertEqual(
                row, (base64.b64encode(signature).decode(), 0, b"hello", 9, 8, 0)
            )

    def test_radio_status_latches_but_key_change_resets_it(self):
        with tempfile.TemporaryDirectory() as root:
            packet = {
                "from": 123,
                "channel": 15,
                "xeddsaSigned": True,
                "decoded": {"portnum": "NODEINFO_APP", "user": {"publicKey": "first"}},
            }
            handle_packet(packet, node_database_number=456, db_path=root)
            db = NodeDB(456, root, channel=15)

            def signed():
                with db.connect() as con:
                    return con.execute(
                        "SELECT has_xeddsa_signed FROM channel_nodedb WHERE node_num=123"
                    ).fetchone()[0]

            self.assertEqual(signed(), 1)
            packet["xeddsaSigned"] = False
            handle_packet(packet, node_database_number=456, db_path=root)
            self.assertEqual(signed(), 1)
            packet["decoded"]["user"]["publicKey"] = "second"
            handle_packet(packet, node_database_number=456, db_path=root)
            self.assertEqual(signed(), 0)

    def test_old_node_table_gains_status_column(self):
        with tempfile.TemporaryDirectory() as root:
            db = NodeDB(456, root)
            with db.connect() as con:
                con.execute(
                    'CREATE TABLE "456_nodedb" (node_num TEXT PRIMARY KEY, long_name TEXT, short_name TEXT, macaddr TEXT, hw_model TEXT, role TEXT, is_licensed INTEGER, public_key TEXT, is_unmessagable INTEGER, last_heard INTEGER, hops_away INTEGER, snr REAL)'
                )
                con.execute(
                    'INSERT INTO "456_nodedb" (node_num, long_name, public_key) VALUES (123,"Existing","key")'
                )
            db.upsert(123, has_xeddsa_signed=True)
            with db.connect() as con:
                row = con.execute(
                    'SELECT long_name, public_key, has_xeddsa_signed FROM "456_nodedb"'
                ).fetchone()
            self.assertEqual(row, ("Existing", "key", 1))

    def test_multicast_default(self):
        self.assertEqual(VirtualNodeConfig().mcast_group, "239.0.0.69")

    def test_scoped_channel_zero_dm_stays_in_peer_table(self):
        with tempfile.TemporaryDirectory() as root:
            packet = {
                "from": 456,
                "to": 123,
                "channel": 0,
                "pkiEncrypted": True,
                "decoded": {
                    "portnum": "TEXT_MESSAGE_APP",
                    "channel": 0,
                    "text": "private",
                },
            }
            stored = handle_packet(
                packet,
                node_database_number=456,
                db_path=root,
                channel=15,
                storage_name="456",
            )
            self.assertTrue(stored["message"])
            db = MessageDB(456, root, storage_name="456")
            with db.connect() as con:
                self.assertEqual(
                    con.execute("SELECT message_text FROM dm_123_messages").fetchone()[
                        0
                    ],
                    "private",
                )

    def test_radio_database_snapshot_preserves_signer_status(self):
        with tempfile.TemporaryDirectory() as root:
            db = NodeDB(456, root, channel=15)
            db.init_from_interface_nodes(
                [
                    {
                        "num": 123,
                        "hasXeddsaSigned": True,
                        "user": {"publicKey": "key", "longName": "Peer"},
                    }
                ]
            )
            with db.connect() as con:
                status = con.execute(
                    "SELECT has_xeddsa_signed FROM channel_nodedb"
                ).fetchone()[0]
            self.assertEqual(status, 1)
