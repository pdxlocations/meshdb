import time
from pubsub import pub

import meshtastic.serial_interface
import meshdb

# Set the path to your MeshDB database here
meshdb.set_default_db_path("~/Meshtastic/github/pdxlocations/meshdb/")

# Connect to a Meshtastic device (serial in this example)
interface = meshtastic.serial_interface.SerialInterface()

# Use library helper to resolve connected device node number
CONNECTED_NODE_NUM = meshdb.get_connected_device_node_num(interface)
if CONNECTED_NODE_NUM is None:
    print("[meshdb] Warning: Could not resolve connected device node number; falling back to 0.")
    CONNECTED_NODE_NUM = 0


def on_receive(packet, interface):
    try:
        # Handle and store the received packet in MeshDB
        result = meshdb.handle_packet(packet, owner_node_num=CONNECTED_NODE_NUM)

        # Demo: fetch long/short name for the sender (will fall back to hex if unknown)
        sender = packet.get("from")
        ln = meshdb.get_long_name(sender, owner_node_num=CONNECTED_NODE_NUM)
        sn = meshdb.get_short_name(sender, owner_node_num=CONNECTED_NODE_NUM)

        print(f"saved: {result} | sender long='{ln}' short='{sn}'")

    except Exception as e:
        print(f"on_receive error: {e}")


# Subscribe to receive packets
pub.subscribe(on_receive, "meshtastic.receive")

# Keep the script alive
while True:
    time.sleep(1)
