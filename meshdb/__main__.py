#!/usr/bin/env python3
"""No-arg CLI: print nodes in the DB with latest telemetry and position.

Usage:
  python -m meshdb

Environment (optional):
  MESHTASTIC_CONNECTED_NODE  Connected device node number (int). If absent, inferred.
  MESHTASTIC_DB              DB base path (dir or file pattern). If absent, uses CWD.
  (Back-compat: MESHTASTIC_OWNER/OWNER are also honored.)
"""
from __future__ import annotations

import os
import re
import json
from datetime import datetime
from typing import Optional, Dict, Any

from meshdb import (
    NodeDB,
    LocationDB,
    TelemetryDB,
    set_default_db_path,
)


def _fmt_ts(ts: Optional[int]) -> str:
    if not ts:
        return "-"
    try:
        return datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts)


def _latest_location(ldb: LocationDB, node_id: int) -> Optional[Dict[str, Any]]:
    with ldb.connect() as con:
        cur = con.cursor()
        cur.execute(
            f"SELECT timestamp, latitude, longitude, altitude, location_source FROM {ldb.table} "
            "WHERE node_id = ? ORDER BY timestamp DESC LIMIT 1",
            (node_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "timestamp": row[0],
            "latitude": row[1],
            "longitude": row[2],
            "altitude": row[3],
            "location_source": row[4],
        }


def _latest_device_telemetry(tdb: TelemetryDB, node_id: int) -> Optional[Dict[str, Any]]:
    with tdb.connect() as con:
        cur = con.cursor()
        cur.execute(
            f"SELECT timestamp, battery_level, voltage, channel_utilization, air_util_tx, uptime_seconds "
            f"FROM {tdb.table_device} WHERE node_id = ? ORDER BY timestamp DESC LIMIT 1",
            (node_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "timestamp": row[0],
            "battery": row[1],
            "voltage": row[2],
            "ch_util": row[3],
            "air_util": row[4],
            "uptime": row[5],
        }


def _latest_power_telemetry(tdb: TelemetryDB, node_id: int) -> Optional[Dict[str, Any]]:
    with tdb.connect() as con:
        cur = con.cursor()
        cur.execute(
            f"SELECT timestamp, ch1_voltage, ch1_current, ch2_voltage, ch2_current, ch3_voltage, ch3_current "
            f"FROM {tdb.table_power} WHERE node_id = ? ORDER BY timestamp DESC LIMIT 1",
            (node_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "timestamp": row[0],
            "ch1_v": row[1],
            "ch1_i": row[2],
            "ch2_v": row[3],
            "ch2_i": row[4],
            "ch3_v": row[5],
            "ch3_i": row[6],
        }


def _infer_owner_candidates(db_hint: str | None) -> list[int]:
    path = db_hint or os.getcwd()
    path = os.path.abspath(os.path.expanduser(path))
    if not os.path.exists(path):
        return []
    dirpath = path if os.path.isdir(path) else (os.path.dirname(path) or ".")
    out: set[int] = set()
    try:
        for fn in os.listdir(dirpath):
            # Patterns:
            #   12345678.db
            #   something.12345678.sqlite3
            #   something.12345678.db
            #   12345678.sqlite3
            m = re.search(r"(?:^|\.)((?:\d){4,})(?=\.(?:db|sqlite3)$)", fn)
            if m:
                try:
                    out.add(int(m.group(1)))
                except Exception:
                    pass
    except Exception:
        return []
    return sorted(out)


def main() -> None:
    db_base = os.environ.get("MESHTASTIC_DB")
    set_default_db_path(db_base)

    # Prefer new env var, fall back to legacy names
    connected_env = os.environ.get("MESHTASTIC_CONNECTED_NODE")
    connected = int(connected_env) if connected_env and connected_env.isdigit() else None

    if connected is None:
        candidates = _infer_owner_candidates(db_base)
        if len(candidates) == 1:
            connected = candidates[0]
        else:
            msg = [
                "Connected device node number not provided. Set MESHTASTIC_CONNECTED_NODE,",
                "or place a single per-node DB in the target directory.",
            ]
            if candidates:
                msg.append(f" Found candidate nodes: {', '.join(map(str, candidates))}")
            print("".join(msg))
            return

    ndb = NodeDB(connected, db_path=db_base)
    ldb = LocationDB(connected, db_path=db_base)
    tdb = TelemetryDB(connected, db_path=db_base)

    # Ensure tables exist so SELECTs don't fail on fresh DBs
    ndb.ensure_table()
    ldb.ensure_table()
    tdb.ensure_tables()

    # Fetch all nodes
    with ndb.connect() as con:
        cur = con.cursor()
        cur.execute(
            f"SELECT node_id, long_name, short_name, last_heard, hops_away, snr FROM {ndb.table} "
            "ORDER BY (last_heard IS NULL), last_heard DESC"
        )
        rows = cur.fetchall()

    if not rows:
        print("(no nodes in database)")
        return

    node_list = []
    for node_id, long_name, short_name, last_heard, hops_away, snr in rows:
        uid = int(node_id) if isinstance(node_id, (int, str)) else node_id
        name_long = long_name or ndb.get_name(uid, "long")
        name_short = short_name or ndb.get_name(uid, "short")
        loc = _latest_location(ldb, uid)
        dev = _latest_device_telemetry(tdb, uid)
        pwr = _latest_power_telemetry(tdb, uid)
        node_data = {
            "node_id": uid,
            "long_name": name_long,
            "short_name": name_short,
            "last_heard": last_heard,
            "hops_away": hops_away,
            "snr": snr,
        }
        if loc:
            node_data["location"] = loc
        if dev:
            node_data["telemetry_device"] = dev
        if pwr:
            node_data["telemetry_power"] = pwr

        with tdb.connect() as con:
            cur = con.cursor()
            # Environment telemetry
            cur.execute(
                f"SELECT * FROM {tdb.table_environment} WHERE node_id = ? ORDER BY timestamp DESC LIMIT 1", (uid,)
            )
            env_row = cur.fetchone()
            if env_row:
                columns = [d[0] for d in cur.description]
                node_data["telemetry_environment"] = dict(zip(columns, env_row))

            # Air quality telemetry
            cur.execute(
                f"SELECT * FROM {tdb.table_air_quality} WHERE node_id = ? ORDER BY timestamp DESC LIMIT 1", (uid,)
            )
            aq_row = cur.fetchone()
            if aq_row:
                columns = [d[0] for d in cur.description]
                node_data["telemetry_air_quality"] = dict(zip(columns, aq_row))

            # Health telemetry
            cur.execute(f"SELECT * FROM {tdb.table_health} WHERE node_id = ? ORDER BY timestamp DESC LIMIT 1", (uid,))
            health_row = cur.fetchone()
            if health_row:
                columns = [d[0] for d in cur.description]
                node_data["telemetry_health"] = dict(zip(columns, health_row))

            # Host telemetry
            cur.execute(f"SELECT * FROM {tdb.table_host} WHERE node_id = ? ORDER BY timestamp DESC LIMIT 1", (uid,))
            host_row = cur.fetchone()
            if host_row:
                columns = [d[0] for d in cur.description]
                node_data["telemetry_host"] = dict(zip(columns, host_row))

        node_list.append(node_data)
    print(json.dumps(node_list, indent=2))


if __name__ == "__main__":
    main()
