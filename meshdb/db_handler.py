import os
import re
from typing import Optional, Union, Dict, List, Tuple

# Optional package-wide default DB base path that users can set from their scripts.
# If set (via set_default_db_path), classes will use it when db_path=None.
DEFAULT_DB_BASE_PATH: Optional[str] = None


def set_default_db_path(path: Optional[str]) -> None:
    """Set a package-wide default base path for database files.

    Examples:
        set_default_db_path("~/db")
        set_default_db_path("./data/mesh.sqlite3")
        set_default_db_path(None)  # disable and use cwd
    """
    global DEFAULT_DB_BASE_PATH
    DEFAULT_DB_BASE_PATH = os.path.expanduser(path) if path else None


import sqlite3
import time
import logging
from datetime import datetime
from collections import OrderedDict

from meshdb.utils import decimal_to_hex


############################################################
# Library-style, class-based database handlers
# - Per-owned-node databases: one DB file per node_database_number
# - Separate classes for NodeInfo, Location, and Messages
# - No UI globals required; callers can use return values
############################################################


def _normalize_channel(channel: Optional[Union[int, str]]) -> Optional[int]:
    try:
        normalized = int(channel)
    except (TypeError, ValueError):
        return None
    return normalized if normalized > 0 else None


def _slugify_channel_name(channel_name: Optional[str]) -> Optional[str]:
    if not isinstance(channel_name, str):
        return None
    slug = re.sub(r"[^0-9A-Za-z]+", "", channel_name).strip().lower()
    return slug or None


def _storage_key(
    node_database_number: Union[int, str],
    channel: Optional[Union[int, str]] = None,
    *,
    storage_name: Optional[str] = None,
) -> str:
    normalized_channel = _normalize_channel(channel)
    slug = _slugify_channel_name(storage_name)
    if slug is not None:
        return slug
    if normalized_channel is not None:
        return f"channel_{normalized_channel}"
    return str(int(node_database_number))


def _scoped_table_name(
    node_database_number: Union[int, str],
    suffix: str,
    *,
    channel: Optional[Union[int, str]] = None,
) -> str:
    normalized_channel = _normalize_channel(channel)
    if normalized_channel is not None:
        return f'"channel_{suffix}"'
    return f'"{int(node_database_number)}_{suffix}"'


def _default_db_path(base_path: Optional[str], node_database_number: Union[int, str]) -> str:
    """Resolve a per-node database path.

    If base_path is a directory, create a file inside it named
    `node_<owner>.sqlite3`. If base_path is a file path, append
    `.<owner>` to its filename. If base_path is None, use current dir.
    """
    # If a per-call base_path was not provided, fall back to the package-wide default
    # that users may set via set_default_db_path().
    if base_path is None and DEFAULT_DB_BASE_PATH:
        base_path = DEFAULT_DB_BASE_PATH
    owner = str(node_database_number)
    if not base_path:
        return os.path.abspath(f"{owner}.db")

    base_path = os.path.abspath(os.path.expanduser(base_path))
    if os.path.isdir(base_path):
        return os.path.join(base_path, f"{owner}.db")

    root, ext = os.path.splitext(base_path)
    if ext:
        return f"{root}.{owner}{ext}"
    return f"{base_path}.{owner}.sqlite3"


PACKET_META_COLUMNS: List[Tuple[str, str]] = [
    ("to_node", "INTEGER"),
    ("packet_id", "INTEGER"),
    ("channel", "INTEGER"),
    ("rx_snr", "REAL"),
    ("rx_rssi", "INTEGER"),
    ("hop_limit", "INTEGER"),
    ("want_ack", "INTEGER"),
    ("priority", "TEXT"),
    ("delayed", "TEXT"),
    ("via_mqtt", "INTEGER"),
    ("hop_start", "INTEGER"),
    ("public_key", "TEXT"),
    ("pki_encrypted", "INTEGER"),
    ("next_hop", "INTEGER"),
    ("relay_node", "INTEGER"),
    ("tx_after", "INTEGER"),
    ("transport_mechanism", "TEXT"),
]
PACKET_META_COLUMN_NAMES = [name for name, _ in PACKET_META_COLUMNS]
PACKET_META_SCHEMA = ", ".join(f"{name} {typ}" for name, typ in PACKET_META_COLUMNS)


def _ensure_packet_meta_columns(cur, table: str) -> None:
    cur.execute(f"PRAGMA table_info({table})")
    cols = {r[1] for r in cur.fetchall()}
    for name, typ in PACKET_META_COLUMNS:
        if name not in cols:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {name} {typ}")


def _packet_meta_values(packet: Dict[str, object]) -> Tuple[object, ...]:
    def as_int_bool(value: object) -> Optional[int]:
        if value is None:
            return None
        return int(bool(value))

    return (
        packet.get("to"),
        packet.get("id"),
        packet.get("channel"),
        packet.get("snr"),
        packet.get("rxRssi"),
        packet.get("hopLimit"),
        as_int_bool(packet.get("wantAck")),
        packet.get("priority"),
        packet.get("delayed"),
        as_int_bool(packet.get("viaMqtt")),
        packet.get("hopStart"),
        packet.get("publicKey"),
        as_int_bool(packet.get("pkiEncrypted")),
        packet.get("nextHop"),
        packet.get("relayNode"),
        packet.get("txAfter"),
        packet.get("transportMechanism"),
    )


def _upsert_node_scoped_packet(
    cur,
    table: str,
    node_num: object,
    timestamp: int,
    field_values: "OrderedDict[str, object]",
    packet: Dict[str, object],
) -> None:
    columns = ["node_num", "timestamp", *field_values.keys(), *PACKET_META_COLUMN_NAMES]
    placeholders = ", ".join("?" for _ in columns)
    update_clause = ", ".join(f"{column}=excluded.{column}" for column in columns if column != "node_num")
    values = (node_num, timestamp, *field_values.values(), *_packet_meta_values(packet))
    cur.execute(
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) "
        f"ON CONFLICT(node_num) DO UPDATE SET {update_clause}",
        values,
    )


class _DB:
    """Lightweight connection helper that ensures tables and provides cursors."""

    def __init__(
        self,
        node_database_number: Union[int, str],
        db_path: Optional[str] = None,
        channel: Optional[Union[int, str]] = None,
        channel_name: Optional[str] = None,
        storage_name: Optional[str] = None,
    ):
        self.node_database_number = int(node_database_number)
        self.channel = _normalize_channel(channel)
        self.channel_name = channel_name
        self.storage_name = _slugify_channel_name(storage_name)
        storage_key = _storage_key(self.node_database_number, self.channel, storage_name=self.storage_name)
        self.db_path = _default_db_path(db_path, storage_key)
        # Ensure parent directory exists if a directory is implied
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        self._migrate_legacy_channel_db(db_path)

    def connect(self):
        return sqlite3.connect(self.db_path)

    @property
    def owner(self) -> int:
        return self.node_database_number

    @property
    def scope(self) -> str:
        return _storage_key(self.node_database_number, self.channel, storage_name=self.storage_name)

    def _migrate_legacy_channel_db(self, db_path: Optional[str]) -> None:
        if self.channel is None or self.storage_name is None:
            return
        legacy_path = _default_db_path(db_path, f"channel_{self.channel}")
        if legacy_path == self.db_path:
            return
        if os.path.exists(self.db_path) or not os.path.exists(legacy_path):
            return
        try:
            os.replace(legacy_path, self.db_path)
        except OSError:
            pass


class NodeDB(_DB):
    """CRUD utilities for the per-owner node database table (…_nodedb)."""

    @property
    def table(self) -> str:
        return _scoped_table_name(self.owner, "nodedb", channel=self.channel)

    def ensure_table(self) -> None:
        schema = (
            "node_num TEXT PRIMARY KEY,"
            "long_name TEXT,"
            "short_name TEXT,"
            "macaddr TEXT,"
            "hw_model TEXT,"
            "role TEXT,"
            "is_licensed INTEGER,"
            "public_key TEXT,"
            "is_unmessagable INTEGER,"
            "last_heard INTEGER,"
            "hops_away INTEGER,"
            "snr REAL"
        )
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(f"CREATE TABLE IF NOT EXISTS {self.table} ({schema})")
            # Forward-compat: add new columns if upgrading from older schema
            cur.execute(f"PRAGMA table_info({self.table})")
            cols = {r[1] for r in cur.fetchall()}
            for name, typ in [
                ("macaddr", "TEXT"),
                ("is_unmessagable", "INTEGER"),
                ("last_heard", "INTEGER"),
                ("hops_away", "INTEGER"),
                ("snr", "REAL"),
                ("is_licensed", "INTEGER"),
            ]:
                if name not in cols:
                    cur.execute(f"ALTER TABLE {self.table} ADD COLUMN {name} {typ}")
            con.commit()

    def upsert(
        self,
        node_num: Union[int, str],
        long_name: Optional[str] = None,
        short_name: Optional[str] = None,
        hw_model: Optional[Union[str, int]] = None,
        role: Optional[Union[str, int]] = None,
        is_licensed: Optional[Union[bool, int]] = None,
        public_key: Optional[str] = None,
        macaddr: Optional[str] = None,
        is_unmessagable: Optional[Union[bool, int]] = None,
        last_heard: Optional[int] = None,
        hops_away: Optional[int] = None,
        snr: Optional[float] = None,
    ) -> None:
        """Insert or update a node record, preserving unspecified fields."""
        self.ensure_table()

        def normalized_text(value: Optional[object]) -> Optional[str]:
            if value is None:
                return None
            text = str(value).strip()
            return text or None

        with self.connect() as con:
            cur = con.cursor()
            cur.execute(f"SELECT * FROM {self.table} WHERE node_num = ?", (node_num,))
            existing = cur.fetchone()

            if existing:
                (
                    _node_num,
                    ex_long,
                    ex_short,
                    ex_mac,
                    ex_hw,
                    ex_role,
                    ex_lic,
                    ex_pub,
                    ex_unmsg,
                    ex_last,
                    ex_hops,
                    ex_snr,
                ) = existing
            else:
                ex_long = ex_short = ex_mac = ex_hw = ex_role = ex_lic = ex_pub = ex_unmsg = ex_last = ex_hops = (
                    ex_snr
                ) = None

            long_name = normalized_text(long_name) if long_name is not None else ex_long
            short_name = normalized_text(short_name) if short_name is not None else ex_short
            macaddr = macaddr if macaddr is not None else (ex_mac if ex_mac is not None else "")
            hw_model = str(hw_model) if hw_model is not None else (ex_hw if ex_hw is not None else "UNSET")
            role = str(role) if role is not None else (ex_role if ex_role is not None else "CLIENT")
            is_licensed = int(is_licensed) if is_licensed is not None else (ex_lic if ex_lic is not None else 0)
            public_key = public_key if public_key is not None else (ex_pub if ex_pub is not None else "")
            is_unmessagable = (
                int(is_unmessagable) if is_unmessagable is not None else (ex_unmsg if ex_unmsg is not None else 0)
            )
            last_heard = last_heard if last_heard is not None else (ex_last if ex_last is not None else None)
            hops_away = hops_away if hops_away is not None else (ex_hops if ex_hops is not None else None)
            snr = snr if snr is not None else (ex_snr if ex_snr is not None else None)

            upsert_sql = f"""
                INSERT INTO {self.table}
                    (node_num, long_name, short_name, macaddr, hw_model, role, is_licensed, public_key, is_unmessagable, last_heard, hops_away, snr)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(node_num) DO UPDATE SET
                    long_name=excluded.long_name,
                    short_name=excluded.short_name,
                    macaddr=excluded.macaddr,
                    hw_model=excluded.hw_model,
                    role=excluded.role,
                    is_licensed=excluded.is_licensed,
                    public_key=excluded.public_key,
                    is_unmessagable=excluded.is_unmessagable,
                    last_heard=excluded.last_heard,
                    hops_away=excluded.hops_away,
                    snr=excluded.snr
            """
            cur.execute(
                upsert_sql,
                (
                    node_num,
                    long_name,
                    short_name,
                    macaddr,
                    hw_model,
                    role,
                    is_licensed,
                    public_key,
                    is_unmessagable,
                    last_heard,
                    hops_away,
                    snr,
                ),
            )
            con.commit()

    def get_name(self, node_num: int, kind: str = "long") -> str:
        """Return long or short name; fallback to hex string when missing."""
        self.ensure_table()
        col = "long_name" if kind == "long" else "short_name"
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(f"SELECT {col} FROM {self.table} WHERE node_num = ?", (node_num,))
            row = cur.fetchone()
            return row[0] if row and row[0] else decimal_to_hex(node_num)

    def init_from_interface_nodes(self, nodes: List[Dict[str, object]]) -> None:
        """Initialize/populate the node table from an iterable of node dicts."""
        for node in list(nodes):
            self.upsert(
                node_num=node.get("num"),
                long_name=node.get("user", {}).get("longName", ""),
                short_name=node.get("user", {}).get("shortName", ""),
                macaddr=node.get("user", {}).get("macaddr", ""),
                hw_model=node.get("user", {}).get("hwModel", ""),
                role=node.get("user", {}).get("role", "CLIENT"),
                is_licensed=(
                    node.get("user", {}).get("isLicensed") if isinstance(node.get("user", {}), dict) else None
                )
                or node.get("user", {}).get("is_licensed"),
                public_key=node.get("user", {}).get("publicKey", ""),
                is_unmessagable=node.get("user", {}).get("isUnmessagable", 0),
                last_heard=node.get("lastHeard"),
                hops_away=node.get("hopsAway"),
                snr=node.get("snr"),
            )


class LocationDB(_DB):
    """Storage and retrieval for location packets."""

    @property
    def table(self) -> str:
        return _scoped_table_name(self.owner, "location", channel=self.channel)

    def ensure_table(self) -> None:
        schema = (
            "node_num TEXT,"
            "timestamp INTEGER,"  # packet rxTime fallback
            "latitude REAL,"
            "longitude REAL,"
            "latitude_i INTEGER,"
            "longitude_i INTEGER,"
            "altitude REAL,"
            "location_source TEXT,"
            "altitude_source TEXT,"
            "pos_time INTEGER,"  # field 4
            "pos_timestamp INTEGER,"  # field 7
            "pos_timestamp_ms_adjust INTEGER,"  # field 8
            "altitude_hae INTEGER,"  # field 9
            "altitude_geoidal_separation INTEGER,"  # field 10
            "pdop INTEGER,"  # field 11
            "hdop INTEGER,"  # field 12
            "vdop INTEGER,"  # field 13
            "gps_accuracy INTEGER,"  # field 14
            "ground_speed INTEGER,"  # field 15
            "ground_track INTEGER,"  # field 16
            "fix_quality INTEGER,"  # field 17
            "fix_type INTEGER,"  # field 18
            "sats_in_view INTEGER,"  # field 19
            "sensor_id INTEGER,"  # field 20
            "next_update INTEGER,"  # field 21
            "seq_number INTEGER,"  # field 22
            "precision_bits INTEGER,"  # field 23
            "precision INTEGER,"  # legacy compatibility
            f"{PACKET_META_SCHEMA}"
        )
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(f"CREATE TABLE IF NOT EXISTS {self.table} ({schema})")
            # Index to speed up history queries
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.scope}_loc_user_time ON {self.table} (node_num, timestamp)")
            cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS uniq_{self.scope}_loc_user ON {self.table} (node_num)")
            cur.execute(f"PRAGMA table_info({self.table})")
            lcols = {r[1] for r in cur.fetchall()}
            for name, typ in [
                ("latitude_i", "INTEGER"),
                ("longitude_i", "INTEGER"),
                ("location_source", "TEXT"),
                ("altitude_source", "TEXT"),
                ("pos_time", "INTEGER"),
                ("pos_timestamp", "INTEGER"),
                ("pos_timestamp_ms_adjust", "INTEGER"),
                ("altitude_hae", "INTEGER"),
                ("altitude_geoidal_separation", "INTEGER"),
                ("pdop", "INTEGER"),
                ("hdop", "INTEGER"),
                ("vdop", "INTEGER"),
                ("gps_accuracy", "INTEGER"),
                ("ground_speed", "INTEGER"),
                ("ground_track", "INTEGER"),
                ("fix_quality", "INTEGER"),
                ("fix_type", "INTEGER"),
                ("sats_in_view", "INTEGER"),
                ("sensor_id", "INTEGER"),
                ("next_update", "INTEGER"),
                ("seq_number", "INTEGER"),
                ("precision_bits", "INTEGER"),
            ]:
                if name not in lcols:
                    cur.execute(f"ALTER TABLE {self.table} ADD COLUMN {name} {typ}")
            _ensure_packet_meta_columns(cur, self.table)
            con.commit()

    def save_packet(self, packet: Dict[str, object]) -> int:
        """Save a location packet. Expects a Meshtastic-like decoded dict.

        Returns the stored timestamp.
        """
        self.ensure_table()
        node_num = packet.get("from")
        decoded = packet.get("decoded", {})
        pos = decoded.get("position", {})
        timestamp = int(packet.get("rxTime", int(time.time())))

        # Unified getters to accept both camelCase and snake_case from various decoders
        def g(obj, *names, default=None):
            for n in names:
                if n in obj and obj.get(n) is not None:
                    return obj.get(n)
            return default

        lat = g(pos, "latitude", "lat")
        lon = g(pos, "longitude", "lon")
        lat_i = g(pos, "latitudeI", "latitude_i")
        lon_i = g(pos, "longitudeI", "longitude_i")
        alt = g(pos, "altitude", "alt")
        loc_src = g(pos, "locationSource", "location_source")
        alt_src = g(pos, "altitudeSource", "altitude_source")
        pos_time = g(pos, "time", "pos_time")
        pos_ts = g(pos, "timestamp", "pos_timestamp")
        pos_ts_adj = g(pos, "timestampMillisAdjust", "timestamp_millis_adjust", "pos_timestamp_ms_adjust")
        alt_hae = g(pos, "altitudeHae", "altitude_hae")
        alt_geo_sep = g(pos, "altitudeGeoidalSeparation", "altitude_geoidal_separation")
        pdop = g(pos, "PDOP", "pdop")
        hdop = g(pos, "HDOP", "hdop")
        vdop = g(pos, "VDOP", "vdop")
        gps_acc = g(pos, "gpsAccuracy", "gps_accuracy")
        gspd = g(pos, "groundSpeed", "ground_speed")
        gtrk = g(pos, "groundTrack", "ground_track")
        fix_q = g(pos, "fixQuality", "fix_quality")
        fix_t = g(pos, "fixType", "fix_type")
        sats = g(pos, "satsInView", "sats_in_view")
        sensor_id = g(pos, "sensorId", "sensor_id")
        next_upd = g(pos, "nextUpdate", "next_update")
        seq_no = g(pos, "seqNumber", "seq_number")
        prec_bits = g(pos, "precisionBits", "precision_bits")
        with self.connect() as con:
            cur = con.cursor()
            _upsert_node_scoped_packet(
                cur,
                self.table,
                node_num,
                timestamp,
                OrderedDict(
                    [
                        ("latitude", lat),
                        ("longitude", lon),
                        ("latitude_i", lat_i),
                        ("longitude_i", lon_i),
                        ("altitude", alt),
                        ("location_source", loc_src),
                        ("altitude_source", alt_src),
                        ("pos_time", pos_time),
                        ("pos_timestamp", pos_ts),
                        ("pos_timestamp_ms_adjust", pos_ts_adj),
                        ("altitude_hae", alt_hae),
                        ("altitude_geoidal_separation", alt_geo_sep),
                        ("pdop", pdop),
                        ("hdop", hdop),
                        ("vdop", vdop),
                        ("gps_accuracy", gps_acc),
                        ("ground_speed", gspd),
                        ("ground_track", gtrk),
                        ("fix_quality", fix_q),
                        ("fix_type", fix_t),
                        ("sats_in_view", sats),
                        ("sensor_id", sensor_id),
                        ("next_update", next_upd),
                        ("seq_number", seq_no),
                        ("precision_bits", prec_bits),
                        ("precision", prec_bits),
                    ]
                ),
                packet,
            )
            con.commit()
        return timestamp

    def latest_for_user(self, node_num: Union[int, str]) -> Optional[Tuple[int, float, float]]:
        self.ensure_table()
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                f"SELECT timestamp, latitude, longitude FROM {self.table} WHERE node_num = ? ORDER BY timestamp DESC LIMIT 1",
                (node_num,),
            )
            row = cur.fetchone()
            return (row[0], row[1], row[2]) if row else None

    def history_for_user(
        self, node_num: Union[int, str], since_ts: Optional[int] = None, limit: int = 1000
    ) -> List[Tuple[int, float, float]]:
        self.ensure_table()
        with self.connect() as con:
            cur = con.cursor()
            if since_ts:
                cur.execute(
                    f"SELECT timestamp, latitude, longitude FROM {self.table} WHERE node_num = ? AND timestamp >= ? ORDER BY timestamp ASC LIMIT ?",
                    (node_num, since_ts, limit),
                )
            else:
                cur.execute(
                    f"SELECT timestamp, latitude, longitude FROM {self.table} WHERE node_num = ? ORDER BY timestamp ASC LIMIT ?",
                    (node_num, limit),
                )
            return [(r[0], r[1], r[2]) for r in cur.fetchall()]


class TelemetryDB(_DB):
    """Storage for Meshtastic telemetry metrics.

    Creates typed tables for each common metrics variant:
      - <owner>_telemetry_device(node_num, timestamp, battery_level, voltage, channel_utilization, air_util_tx, uptime_seconds)
      - <owner>_telemetry_power(node_num, timestamp, ch1_voltage, ch1_current, ch2_voltage, ch2_current, ch3_voltage, ch3_current, ch4_voltage, ch4_current, ch5_voltage, ch5_current, ch6_voltage, ch6_current, ch7_voltage, ch7_current, ch8_voltage, ch8_current)
      - <owner>_telemetry_environment(node_num, timestamp, temperature, relative_humidity, barometric_pressure, gas_resistance, voltage, current, iaq, distance, lux, white_lux, ir_lux, uv_lux, wind_direction, wind_speed, weight, wind_gust, wind_lull, radiation, rainfall_1h, rainfall_24h, soil_moisture, soil_temperature)
      - <owner>_telemetry_air_quality(node_num, timestamp, pm10_standard, pm25_standard, pm100_standard, pm10_environmental, pm25_environmental, pm100_environmental, particles_03um, particles_05um, particles_10um, particles_25um, particles_50um, particles_100um, co2, co2_temperature, co2_humidity, form_formaldehyde, form_humidity, form_temperature, pm40_standard, particles_40um, pm_temperature, pm_humidity, pm_voc_idx, pm_nox_idx, particles_tps)
      - <owner>_telemetry_local_stats(node_num, timestamp, uptime_seconds, channel_utilization, air_util_tx, num_packets_tx, num_packets_rx, num_packets_rx_bad, num_online_nodes, num_total_nodes, num_rx_dupe, num_tx_relay, num_tx_relay_canceled, heap_total_bytes, heap_free_bytes, num_tx_dropped)
      - <owner>_telemetry_health(node_num, timestamp, heart_bpm, spO2, temperature)
      - <owner>_telemetry_host(node_num, timestamp, uptime_seconds, freemem_bytes, diskfree1_bytes, diskfree2_bytes, diskfree3_bytes, load1, load5, load15, user_string)
    """

    @property
    def table_device(self) -> str:
        return _scoped_table_name(self.owner, "telemetry_device", channel=self.channel)

    @property
    def table_power(self) -> str:
        return _scoped_table_name(self.owner, "telemetry_power", channel=self.channel)

    @property
    def table_environment(self) -> str:
        return _scoped_table_name(self.owner, "telemetry_environment", channel=self.channel)

    @property
    def table_air_quality(self) -> str:
        return _scoped_table_name(self.owner, "telemetry_air_quality", channel=self.channel)

    @property
    def table_local_stats(self) -> str:
        return _scoped_table_name(self.owner, "telemetry_local_stats", channel=self.channel)

    @property
    def table_health(self) -> str:
        return _scoped_table_name(self.owner, "telemetry_health", channel=self.channel)

    @property
    def table_host(self) -> str:
        return _scoped_table_name(self.owner, "telemetry_host", channel=self.channel)

    def ensure_tables(self) -> None:
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {self.table_device} ("
                "node_num TEXT,"
                "timestamp INTEGER,"
                "battery_level REAL,"
                "voltage REAL,"
                "channel_utilization REAL,"
                "air_util_tx REAL,"
                "uptime_seconds INTEGER,"
                f"{PACKET_META_SCHEMA}"
                ")"
            )
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {self.table_power} ("
                "node_num TEXT,"
                "timestamp INTEGER,"
                "ch1_voltage REAL,"
                "ch1_current REAL,"
                "ch2_voltage REAL,"
                "ch2_current REAL,"
                "ch3_voltage REAL,"
                "ch3_current REAL,"
                "ch4_voltage REAL,"
                "ch4_current REAL,"
                "ch5_voltage REAL,"
                "ch5_current REAL,"
                "ch6_voltage REAL,"
                "ch6_current REAL,"
                "ch7_voltage REAL,"
                "ch7_current REAL,"
                "ch8_voltage REAL,"
                "ch8_current REAL,"
                f"{PACKET_META_SCHEMA}"
                ")"
            )
            # Environment
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {self.table_environment} ("
                "node_num TEXT,"
                "timestamp INTEGER,"
                "temperature REAL,"
                "relative_humidity REAL,"
                "barometric_pressure REAL,"
                "gas_resistance REAL,"
                "voltage REAL,"
                "current REAL,"
                "iaq INTEGER,"
                "distance REAL,"
                "lux REAL,"
                "white_lux REAL,"
                "ir_lux REAL,"
                "uv_lux REAL,"
                "wind_direction INTEGER,"
                "wind_speed REAL,"
                "weight REAL,"
                "wind_gust REAL,"
                "wind_lull REAL,"
                "radiation REAL,"
                "rainfall_1h REAL,"
                "rainfall_24h REAL,"
                "soil_moisture INTEGER,"
                "soil_temperature REAL,"
                f"{PACKET_META_SCHEMA}"
                ")"
            )

            # Air Quality
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {self.table_air_quality} ("
                "node_num TEXT,"
                "timestamp INTEGER,"
                "pm10_standard INTEGER,"
                "pm25_standard INTEGER,"
                "pm100_standard INTEGER,"
                "pm10_environmental INTEGER,"
                "pm25_environmental INTEGER,"
                "pm100_environmental INTEGER,"
                "particles_03um INTEGER,"
                "particles_05um INTEGER,"
                "particles_10um INTEGER,"
                "particles_25um INTEGER,"
                "particles_50um INTEGER,"
                "particles_100um INTEGER,"
                "co2 INTEGER,"
                "co2_temperature REAL,"
                "co2_humidity REAL,"
                "form_formaldehyde REAL,"
                "form_humidity REAL,"
                "form_temperature REAL,"
                "pm40_standard INTEGER,"
                "particles_40um INTEGER,"
                "pm_temperature REAL,"
                "pm_humidity REAL,"
                "pm_voc_idx REAL,"
                "pm_nox_idx REAL,"
                "particles_tps REAL,"
                f"{PACKET_META_SCHEMA}"
                ")"
            )

            # Local Stats
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {self.table_local_stats} ("
                "node_num TEXT,"
                "timestamp INTEGER,"
                "uptime_seconds INTEGER,"
                "channel_utilization REAL,"
                "air_util_tx REAL,"
                "num_packets_tx INTEGER,"
                "num_packets_rx INTEGER,"
                "num_packets_rx_bad INTEGER,"
                "num_online_nodes INTEGER,"
                "num_total_nodes INTEGER,"
                "num_rx_dupe INTEGER,"
                "num_tx_relay INTEGER,"
                "num_tx_relay_canceled INTEGER,"
                "heap_total_bytes INTEGER,"
                "heap_free_bytes INTEGER,"
                "num_tx_dropped INTEGER,"
                f"{PACKET_META_SCHEMA}"
                ")"
            )

            # Health
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {self.table_health} ("
                "node_num TEXT,"
                "timestamp INTEGER,"
                "heart_bpm INTEGER,"
                "spO2 INTEGER,"
                "temperature REAL,"
                f"{PACKET_META_SCHEMA}"
                ")"
            )

            # Host
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {self.table_host} ("
                "node_num TEXT,"
                "timestamp INTEGER,"
                "uptime_seconds INTEGER,"
                "freemem_bytes INTEGER,"
                "diskfree1_bytes INTEGER,"
                "diskfree2_bytes INTEGER,"
                "diskfree3_bytes INTEGER,"
                "load1 INTEGER,"
                "load5 INTEGER,"
                "load15 INTEGER,"
                "user_string TEXT,"
                f"{PACKET_META_SCHEMA}"
                ")"
            )
            # Helpful indices
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.scope}_td_user_time ON {self.table_device} (node_num, timestamp)")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.scope}_tp_user_time ON {self.table_power} (node_num, timestamp)")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.scope}_tenv_user_time ON {self.table_environment} (node_num, timestamp)")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.scope}_taq_user_time ON {self.table_air_quality} (node_num, timestamp)")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.scope}_tls_user_time ON {self.table_local_stats} (node_num, timestamp)")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.scope}_th_user_time ON {self.table_health} (node_num, timestamp)")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.scope}_thost_user_time ON {self.table_host} (node_num, timestamp)")
            # Add unique indices for overwrite-on-insert (upsert) per node_num
            cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS uniq_{self.scope}_td_user ON {self.table_device} (node_num)")
            cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS uniq_{self.scope}_tp_user ON {self.table_power} (node_num)")
            cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS uniq_{self.scope}_tenv_user ON {self.table_environment} (node_num)")
            cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS uniq_{self.scope}_taq_user ON {self.table_air_quality} (node_num)")
            cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS uniq_{self.scope}_tls_user ON {self.table_local_stats} (node_num)")
            cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS uniq_{self.scope}_th_user ON {self.table_health} (node_num)")
            cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS uniq_{self.scope}_thost_user ON {self.table_host} (node_num)")
            for table in (
                self.table_device,
                self.table_power,
                self.table_environment,
                self.table_air_quality,
                self.table_local_stats,
                self.table_health,
                self.table_host,
            ):
                _ensure_packet_meta_columns(cur, table)
            con.commit()

    def save_packet(self, packet: Dict[str, object]) -> int:
        """Persist any telemetry metrics present in a decoded packet.

        Returns the stored timestamp.
        """
        self.ensure_tables()
        node_num = packet.get("from")
        decoded = packet.get("decoded", {})
        telem = decoded.get("telemetry", {})
        ts = int(telem.get("time") or packet.get("rxTime") or time.time())

        device = telem.get("deviceMetrics")
        power = telem.get("powerMetrics")
        env = telem.get("environmentMetrics") or telem.get("environment_metrics")
        aq = telem.get("airQualityMetrics") or telem.get("air_quality_metrics")
        ls = telem.get("localStats") or telem.get("local_stats")
        health = telem.get("healthMetrics") or telem.get("health_metrics")
        host = telem.get("hostMetrics") or telem.get("host_metrics")

        with self.connect() as con:
            cur = con.cursor()

            if isinstance(device, dict):
                _upsert_node_scoped_packet(
                    cur,
                    self.table_device,
                    node_num,
                    ts,
                    OrderedDict(
                        [
                            ("battery_level", device.get("batteryLevel")),
                            ("voltage", device.get("voltage")),
                            ("channel_utilization", device.get("channelUtilization")),
                            ("air_util_tx", device.get("airUtilTx")),
                            ("uptime_seconds", device.get("uptimeSeconds")),
                        ]
                    ),
                    packet,
                )

            if isinstance(power, dict):
                _upsert_node_scoped_packet(
                    cur,
                    self.table_power,
                    node_num,
                    ts,
                    OrderedDict(
                        [
                            ("ch1_voltage", power.get("ch1Voltage")),
                            ("ch1_current", power.get("ch1Current")),
                            ("ch2_voltage", power.get("ch2Voltage")),
                            ("ch2_current", power.get("ch2Current")),
                            ("ch3_voltage", power.get("ch3Voltage")),
                            ("ch3_current", power.get("ch3Current")),
                            ("ch4_voltage", power.get("ch4Voltage")),
                            ("ch4_current", power.get("ch4Current")),
                            ("ch5_voltage", power.get("ch5Voltage")),
                            ("ch5_current", power.get("ch5Current")),
                            ("ch6_voltage", power.get("ch6Voltage")),
                            ("ch6_current", power.get("ch6Current")),
                            ("ch7_voltage", power.get("ch7Voltage")),
                            ("ch7_current", power.get("ch7Current")),
                            ("ch8_voltage", power.get("ch8Voltage")),
                            ("ch8_current", power.get("ch8Current")),
                        ]
                    ),
                    packet,
                )

            if isinstance(env, dict):
                _upsert_node_scoped_packet(
                    cur,
                    self.table_environment,
                    node_num,
                    ts,
                    OrderedDict(
                        [
                            ("temperature", env.get("temperature")),
                            ("relative_humidity", env.get("relativeHumidity") if "relativeHumidity" in env else env.get("relative_humidity")),
                            ("barometric_pressure", env.get("barometricPressure") if "barometricPressure" in env else env.get("barometric_pressure")),
                            ("gas_resistance", env.get("gasResistance") if "gasResistance" in env else env.get("gas_resistance")),
                            ("voltage", env.get("voltage")),
                            ("current", env.get("current")),
                            ("iaq", env.get("iaq")),
                            ("distance", env.get("distance")),
                            ("lux", env.get("lux")),
                            ("white_lux", env.get("whiteLux") if "whiteLux" in env else env.get("white_lux")),
                            ("ir_lux", env.get("irLux") if "irLux" in env else env.get("ir_lux")),
                            ("uv_lux", env.get("uvLux") if "uvLux" in env else env.get("uv_lux")),
                            ("wind_direction", env.get("windDirection") if "windDirection" in env else env.get("wind_direction")),
                            ("wind_speed", env.get("windSpeed") if "windSpeed" in env else env.get("wind_speed")),
                            ("weight", env.get("weight")),
                            ("wind_gust", env.get("windGust") if "windGust" in env else env.get("wind_gust")),
                            ("wind_lull", env.get("windLull") if "windLull" in env else env.get("wind_lull")),
                            ("radiation", env.get("radiation")),
                            ("rainfall_1h", env.get("rainfall1h") if "rainfall1h" in env else env.get("rainfall_1h")),
                            ("rainfall_24h", env.get("rainfall24h") if "rainfall24h" in env else env.get("rainfall_24h")),
                            ("soil_moisture", env.get("soilMoisture") if "soilMoisture" in env else env.get("soil_moisture")),
                            ("soil_temperature", env.get("soilTemperature") if "soilTemperature" in env else env.get("soil_temperature")),
                        ]
                    ),
                    packet,
                )

            if isinstance(aq, dict):
                _upsert_node_scoped_packet(
                    cur,
                    self.table_air_quality,
                    node_num,
                    ts,
                    OrderedDict(
                        [
                            ("pm10_standard", aq.get("pm10Standard") if "pm10Standard" in aq else aq.get("pm10_standard")),
                            ("pm25_standard", aq.get("pm25Standard") if "pm25Standard" in aq else aq.get("pm25_standard")),
                            ("pm100_standard", aq.get("pm100Standard") if "pm100Standard" in aq else aq.get("pm100_standard")),
                            ("pm10_environmental", aq.get("pm10Environmental") if "pm10Environmental" in aq else aq.get("pm10_environmental")),
                            ("pm25_environmental", aq.get("pm25Environmental") if "pm25Environmental" in aq else aq.get("pm25_environmental")),
                            ("pm100_environmental", aq.get("pm100Environmental") if "pm100Environmental" in aq else aq.get("pm100_environmental")),
                            ("particles_03um", aq.get("particles03um") if "particles03um" in aq else aq.get("particles_03um")),
                            ("particles_05um", aq.get("particles05um") if "particles05um" in aq else aq.get("particles_05um")),
                            ("particles_10um", aq.get("particles10um") if "particles10um" in aq else aq.get("particles_10um")),
                            ("particles_25um", aq.get("particles25um") if "particles25um" in aq else aq.get("particles_25um")),
                            ("particles_50um", aq.get("particles50um") if "particles50um" in aq else aq.get("particles_50um")),
                            ("particles_100um", aq.get("particles100um") if "particles100um" in aq else aq.get("particles_100um")),
                            ("co2", aq.get("co2")),
                            ("co2_temperature", aq.get("co2Temperature") if "co2Temperature" in aq else aq.get("co2_temperature")),
                            ("co2_humidity", aq.get("co2Humidity") if "co2Humidity" in aq else aq.get("co2_humidity")),
                            ("form_formaldehyde", aq.get("formFormaldehyde") if "formFormaldehyde" in aq else aq.get("form_formaldehyde")),
                            ("form_humidity", aq.get("formHumidity") if "formHumidity" in aq else aq.get("form_humidity")),
                            ("form_temperature", aq.get("formTemperature") if "formTemperature" in aq else aq.get("form_temperature")),
                            ("pm40_standard", aq.get("pm40Standard") if "pm40Standard" in aq else aq.get("pm40_standard")),
                            ("particles_40um", aq.get("particles40um") if "particles40um" in aq else aq.get("particles_40um")),
                            ("pm_temperature", aq.get("pmTemperature") if "pmTemperature" in aq else aq.get("pm_temperature")),
                            ("pm_humidity", aq.get("pmHumidity") if "pmHumidity" in aq else aq.get("pm_humidity")),
                            ("pm_voc_idx", aq.get("pmVocIdx") if "pmVocIdx" in aq else aq.get("pm_voc_idx")),
                            ("pm_nox_idx", aq.get("pmNoxIdx") if "pmNoxIdx" in aq else aq.get("pm_nox_idx")),
                            ("particles_tps", aq.get("particlesTps") if "particlesTps" in aq else aq.get("particles_tps")),
                        ]
                    ),
                    packet,
                )

            if isinstance(ls, dict):
                _upsert_node_scoped_packet(
                    cur,
                    self.table_local_stats,
                    node_num,
                    ts,
                    OrderedDict(
                        [
                            ("uptime_seconds", ls.get("uptimeSeconds") if "uptimeSeconds" in ls else ls.get("uptime_seconds")),
                            ("channel_utilization", ls.get("channelUtilization") if "channelUtilization" in ls else ls.get("channel_utilization")),
                            ("air_util_tx", ls.get("airUtilTx") if "airUtilTx" in ls else ls.get("air_util_tx")),
                            ("num_packets_tx", ls.get("numPacketsTx") if "numPacketsTx" in ls else ls.get("num_packets_tx")),
                            ("num_packets_rx", ls.get("numPacketsRx") if "numPacketsRx" in ls else ls.get("num_packets_rx")),
                            ("num_packets_rx_bad", ls.get("numPacketsRxBad") if "numPacketsRxBad" in ls else ls.get("num_packets_rx_bad")),
                            ("num_online_nodes", ls.get("numOnlineNodes") if "numOnlineNodes" in ls else ls.get("num_online_nodes")),
                            ("num_total_nodes", ls.get("numTotalNodes") if "numTotalNodes" in ls else ls.get("num_total_nodes")),
                            ("num_rx_dupe", ls.get("numRxDupe") if "numRxDupe" in ls else ls.get("num_rx_dupe")),
                            ("num_tx_relay", ls.get("numTxRelay") if "numTxRelay" in ls else ls.get("num_tx_relay")),
                            ("num_tx_relay_canceled", ls.get("numTxRelayCanceled") if "numTxRelayCanceled" in ls else ls.get("num_tx_relay_canceled")),
                            ("heap_total_bytes", ls.get("heapTotalBytes") if "heapTotalBytes" in ls else ls.get("heap_total_bytes")),
                            ("heap_free_bytes", ls.get("heapFreeBytes") if "heapFreeBytes" in ls else ls.get("heap_free_bytes")),
                            ("num_tx_dropped", ls.get("numTxDropped") if "numTxDropped" in ls else ls.get("num_tx_dropped")),
                        ]
                    ),
                    packet,
                )

            if isinstance(health, dict):
                _upsert_node_scoped_packet(
                    cur,
                    self.table_health,
                    node_num,
                    ts,
                    OrderedDict(
                        [
                            ("heart_bpm", health.get("heartBpm") if "heartBpm" in health else health.get("heart_bpm")),
                            ("spO2", health.get("spO2") if "spO2" in health else health.get("spO2")),
                            ("temperature", health.get("temperature")),
                        ]
                    ),
                    packet,
                )

            if isinstance(host, dict):
                _upsert_node_scoped_packet(
                    cur,
                    self.table_host,
                    node_num,
                    ts,
                    OrderedDict(
                        [
                            ("uptime_seconds", host.get("uptimeSeconds") if "uptimeSeconds" in host else host.get("uptime_seconds")),
                            ("freemem_bytes", host.get("freememBytes") if "freememBytes" in host else host.get("freemem_bytes")),
                            ("diskfree1_bytes", host.get("diskfree1Bytes") if "diskfree1Bytes" in host else host.get("diskfree1_bytes")),
                            ("diskfree2_bytes", host.get("diskfree2Bytes") if "diskfree2Bytes" in host else host.get("diskfree2_bytes")),
                            ("diskfree3_bytes", host.get("diskfree3Bytes") if "diskfree3Bytes" in host else host.get("diskfree3_bytes")),
                            ("load1", host.get("load1")),
                            ("load5", host.get("load5")),
                            ("load15", host.get("load15")),
                            ("user_string", host.get("userString") if "userString" in host else host.get("user_string")),
                        ]
                    ),
                    packet,
                )

            con.commit()
        return ts


class MessageDB(_DB):
    """Per-channel message storage. Each owner has many channel tables."""

    def _table_for_channel(self, channel: Union[int, str]) -> str:
        if self.channel is not None:
            table_name = "channel_messages"
        else:
            table_name = f"{self.owner}_{channel}_messages"
        return f'"{table_name}"'

    def ensure_channel_table(self, channel: Union[int, str]) -> None:
        schema = "node_num TEXT," "message_text TEXT," "timestamp INTEGER," f"{PACKET_META_SCHEMA}"
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(f"CREATE TABLE IF NOT EXISTS {self._table_for_channel(channel)} ({schema})")
            _ensure_packet_meta_columns(cur, self._table_for_channel(channel))
            con.commit()

    def _table_for_dm_peer(self, peer_node_num: Union[int, str]) -> str:
        return f'"dm_{int(peer_node_num)}_messages"'

    def ensure_dm_table(self, peer_node_num: Union[int, str]) -> None:
        schema = "node_num TEXT," "message_text TEXT," "timestamp INTEGER," f"{PACKET_META_SCHEMA}"
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(f"CREATE TABLE IF NOT EXISTS {self._table_for_dm_peer(peer_node_num)} ({schema})")
            _ensure_packet_meta_columns(cur, self._table_for_dm_peer(peer_node_num))
            con.commit()

    def save_message(
        self,
        channel: Union[int, str],
        node_num: Union[int, str],
        message_text: str,
        *,
        timestamp: Optional[int] = None,
        packet: Optional[Dict[str, object]] = None,
    ) -> int:
        self.ensure_channel_table(channel)
        ts = int(timestamp or time.time())
        with self.connect() as con:
            cur = con.cursor()
            columns = ["node_num", "message_text", "timestamp", *PACKET_META_COLUMN_NAMES]
            placeholders = ", ".join("?" for _ in columns)
            cur.execute(
                f"INSERT INTO {self._table_for_channel(channel)} ({', '.join(columns)}) VALUES ({placeholders})",
                (str(node_num), message_text, ts, *_packet_meta_values(packet or {})),
            )
            con.commit()
        return ts

    def save_dm_message(
        self,
        peer_node_num: Union[int, str],
        node_num: Union[int, str],
        message_text: str,
        *,
        timestamp: Optional[int] = None,
        packet: Optional[Dict[str, object]] = None,
    ) -> int:
        self.ensure_dm_table(peer_node_num)
        ts = int(timestamp or time.time())
        with self.connect() as con:
            cur = con.cursor()
            columns = ["node_num", "message_text", "timestamp", *PACKET_META_COLUMN_NAMES]
            placeholders = ", ".join("?" for _ in columns)
            cur.execute(
                f"INSERT INTO {self._table_for_dm_peer(peer_node_num)} ({', '.join(columns)}) VALUES ({placeholders})",
                (str(node_num), message_text, ts, *_packet_meta_values(packet or {})),
            )
            con.commit()
        return ts

    def load_channel_messages(self, channel: Union[int, str], limit: Optional[int] = None) -> List[Dict[str, object]]:
        self.ensure_channel_table(channel)
        query = (
            f"SELECT node_num, message_text, timestamp, {', '.join(PACKET_META_COLUMN_NAMES)} "
            f"FROM {self._table_for_channel(channel)} ORDER BY timestamp DESC"
        )
        params: List[object] = []
        if limit is not None:
            query += " LIMIT ?"
            params.append(int(limit))
        with self.connect() as con:
            con.row_factory = sqlite3.Row
            rows = con.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def load_dm_messages(self, peer_node_num: Union[int, str], limit: Optional[int] = None) -> List[Dict[str, object]]:
        self.ensure_dm_table(peer_node_num)
        query = (
            f"SELECT node_num, message_text, timestamp, {', '.join(PACKET_META_COLUMN_NAMES)} "
            f"FROM {self._table_for_dm_peer(peer_node_num)} ORDER BY timestamp DESC"
        )
        params: List[object] = []
        if limit is not None:
            query += " LIMIT ?"
            params.append(int(limit))
        with self.connect() as con:
            con.row_factory = sqlite3.Row
            rows = con.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def update_ack_nak(
        self, channel: Union[int, str], timestamp: int, node_num: Union[int, str], message: str, ack: str
    ) -> None:
        # Deprecated: ack_type column has been removed; keep as no-op for backward compatibility.
        logging.debug("update_ack_nak called but ack_type support is removed; ignoring.")
        return

    def load_messages(self) -> Dict[Union[int, str], List[Tuple[str, str]]]:
        """Return all messages grouped by channel as a dict[channel] -> list[(prefix, text)].
        The hour separators are included as entries with empty text.
        Ack/Nak is no longer stored; this function ignores an existing ack_type column if present.
        """
        out: Dict[Union[int, str], List[Tuple[str, str]]] = {}
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ?",
                (f"{self.owner}_%_messages",),
            )
            tables = [r[0] for r in cur.fetchall()]

            for table_name in tables:
                quoted = f'"{table_name}"'

                # Detect columns for this table
                cur.execute(f"PRAGMA table_info({quoted})")
                cols = {r[1] for r in cur.fetchall()}
                has_ack = "ack_type" in cols

                # Build a SELECT based on available columns
                if has_ack:
                    cur.execute(f"SELECT node_num, message_text, timestamp FROM {quoted}")
                else:
                    cur.execute(f"SELECT node_num, message_text, timestamp FROM {quoted}")
                rows = cur.fetchall()

                # Infer channel name
                try:
                    channel = table_name.split("_")[1]
                    channel = int(channel) if channel.isdigit() else channel
                except Exception:
                    channel = table_name

                # Group hourly
                hourly: Dict[str, List[Tuple[str, str]]] = {}
                for uid, msg, ts in rows:
                    if uid is None or msg is None or ts is None:
                        logging.warning(f"Skipping row with NULL field(s): {(uid, msg, ts)}")
                        continue

                    hour = datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:00")
                    hourly.setdefault(hour, [])
                    # No ack prefix anymore
                    hourly[hour].append(("", msg.replace("\x00", "")))

                out.setdefault(channel, [])
                for hour, msgs in sorted(hourly.items()):
                    out[channel].append((f"-- {hour} --", ""))
                    out[channel].extend(msgs)
        return out


# ------------------------------
# Backwards-compat wrappers
# ------------------------------


def save_message_to_db(
    channel: str,
    node_num: str,
    message_text: str,
    *,
    node_database_number: Union[int, str],
    db_path: Optional[str] = None,
    storage_channel: Optional[Union[int, str]] = None,
    channel_name: Optional[str] = None,
    storage_name: Optional[str] = None,
    timestamp: Optional[int] = None,
    packet: Optional[Dict[str, object]] = None,
) -> Optional[int]:
    try:
        return MessageDB(
            node_database_number,
            db_path,
            channel=storage_channel,
            channel_name=channel_name,
            storage_name=storage_name,
        ).save_message(
            channel,
            node_num,
            message_text,
            timestamp=timestamp,
            packet=packet,
        )
    except sqlite3.Error as e:
        logging.error(f"SQLite error in save_message_to_db: {e}")
    except Exception as e:
        logging.error(f"Unexpected error in save_message_to_db: {e}")
    return None


def update_ack_nak(
    channel: str,
    timestamp: int,
    message: str,
    ack: str,
    *,
    node_database_number: Union[int, str],
    node_num: Union[int, str],
    db_path: Optional[str] = None,
) -> None:
    try:
        logging.debug("update_ack_nak wrapper called but ack_type support is removed; ignoring.")
        return
    except Exception:
        return


def get_name_from_database(
    node_num: int,
    kind: str = "long",
    *,
    node_database_number: Union[int, str],
    db_path: Optional[str] = None,
    channel: Optional[Union[int, str]] = None,
    channel_name: Optional[str] = None,
    storage_name: Optional[str] = None,
) -> str:
    try:
        return NodeDB(
            node_database_number,
            db_path,
            channel=channel,
            channel_name=channel_name,
            storage_name=storage_name,
        ).get_name(node_num, kind)
    except sqlite3.Error as e:
        logging.error(f"SQLite error in get_name_from_database: {e}")
        return "Unknown"
    except Exception as e:
        logging.error(f"Unexpected error in get_name_from_database: {e}")
        return "Unknown"


def maybe_store_nodeinfo_in_db(
    packet: Dict[str, object],
    *,
    node_database_number: Union[int, str],
    db_path: Optional[str] = None,
    channel: Optional[Union[int, str]] = None,
    channel_name: Optional[str] = None,
    storage_name: Optional[str] = None,
) -> None:
    try:
        node_num = packet["from"]
        user = packet["decoded"]["user"]
        storage_channel = _normalize_channel(channel)
        if storage_channel is None:
            storage_channel = _normalize_channel(packet.get("channel"))
        if storage_channel is None:
            storage_channel = _normalize_channel(packet.get("decoded", {}).get("channel"))
        NodeDB(
            node_database_number,
            db_path,
            channel=storage_channel,
            channel_name=channel_name,
            storage_name=storage_name,
        ).upsert(
            node_num=node_num,
            long_name=user.get("longName", ""),
            short_name=user.get("shortName", ""),
            macaddr=user.get("macaddr", ""),
            hw_model=str(user.get("hwModel", "")),
            role=user.get("role", "CLIENT"),
            is_licensed=user.get("isLicensed") if isinstance(user, dict) else None,
            public_key=user.get("publicKey", ""),
            is_unmessagable=user.get("isUnmessagable", 0),
        )
    except sqlite3.Error as e:
        logging.error(f"SQLite error in maybe_store_nodeinfo_in_db: {e}")
    except Exception as e:
        logging.error(f"Unexpected error in maybe_store_nodeinfo_in_db: {e}")


def sync_owner_nodeinfo(
    node_database_number: Union[int, str],
    info: Dict[str, object],
    db_path: Optional[str] = None,
    channel: Optional[Union[int, str]] = None,
    channel_name: Optional[str] = None,
    storage_name: Optional[str] = None,
) -> None:
    """Merge the connected node's own info into the DB.

    Meshtastic interfaces often expose fresher owner metadata via `getMyNodeInfo()`
    than what is present in the downloaded NodeDB snapshot.
    """
    if not isinstance(info, dict):
        return

    user = info.get("user") if isinstance(info.get("user"), dict) else {}
    num = info.get("num")
    if num is None:
        return

    NodeDB(
        node_database_number,
        db_path,
        channel=channel,
        channel_name=channel_name,
        storage_name=storage_name,
    ).upsert(
        node_num=num,
        long_name=user.get("longName"),
        short_name=user.get("shortName"),
        macaddr=user.get("macaddr"),
        hw_model=user.get("hwModel"),
        role=user.get("role"),
        is_licensed=user.get("isLicensed") if isinstance(user, dict) else None,
        public_key=user.get("publicKey"),
        is_unmessagable=user.get("isUnmessagable"),
        last_heard=info.get("lastHeard"),
        hops_away=info.get("hopsAway"),
        snr=info.get("snr"),
    )


def store_location_packet(
    packet: Dict[str, object],
    *,
    node_database_number: Union[int, str],
    db_path: Optional[str] = None,
    channel: Optional[Union[int, str]] = None,
    channel_name: Optional[str] = None,
    storage_name: Optional[str] = None,
) -> Optional[int]:
    try:
        storage_channel = _normalize_channel(channel)
        if storage_channel is None:
            storage_channel = _normalize_channel(packet.get("channel"))
        if storage_channel is None:
            storage_channel = _normalize_channel(packet.get("decoded", {}).get("channel"))
        return LocationDB(
            node_database_number,
            db_path,
            channel=storage_channel,
            channel_name=channel_name,
            storage_name=storage_name,
        ).save_packet(packet)
    except sqlite3.Error as e:
        logging.error(f"SQLite error in store_location_packet: {e}")
    except Exception as e:
        logging.error(f"Unexpected error in store_location_packet: {e}")
    return None


def store_telemetry_packet(
    packet: Dict[str, object],
    *,
    node_database_number: Union[int, str],
    db_path: Optional[str] = None,
    channel: Optional[Union[int, str]] = None,
    channel_name: Optional[str] = None,
    storage_name: Optional[str] = None,
) -> Optional[int]:
    """Persist TELEMETRY_APP packets (deviceMetrics, powerMetrics, etc.)."""
    try:
        storage_channel = _normalize_channel(channel)
        if storage_channel is None:
            storage_channel = _normalize_channel(packet.get("channel"))
        if storage_channel is None:
            storage_channel = _normalize_channel(packet.get("decoded", {}).get("channel"))
        return TelemetryDB(
            node_database_number,
            db_path,
            channel=storage_channel,
            channel_name=channel_name,
            storage_name=storage_name,
        ).save_packet(packet)
    except sqlite3.Error as e:
        logging.error(f"SQLite error in store_telemetry_packet: {e}")
    except Exception as e:
        logging.error(f"Unexpected error in store_telemetry_packet: {e}")
    return None


def _dm_peer_node_num(node_database_number: Union[int, str], packet: Dict[str, object]) -> Optional[int]:
    try:
        owner_num = int(node_database_number)
    except (TypeError, ValueError):
        return None

    try:
        from_num = int(packet.get("from"))
    except (TypeError, ValueError):
        from_num = None

    try:
        to_num = int(packet.get("to"))
    except (TypeError, ValueError):
        to_num = None

    if to_num in (None, 0, 0xFFFFFFFF):
        return None
    if from_num == owner_num:
        return to_num
    if to_num == owner_num:
        return from_num
    if from_num not in (None, owner_num):
        return from_num
    return to_num


# Store TEXT_MESSAGE_APP packets into the per-channel message tables.
def store_text_message_packet(
    packet: Dict[str, object],
    *,
    node_database_number: Union[int, str],
    db_path: Optional[str] = None,
    channel: Optional[Union[int, str]] = None,
    channel_name: Optional[str] = None,
    storage_name: Optional[str] = None,
) -> Optional[int]:
    """Persist TEXT_MESSAGE_APP packets into the per-channel message tables.

    We try a few common fields for channel and text to be compatible across lib versions.
    Returns the stored timestamp, or None if not stored.
    """
    try:
        decoded = packet.get("decoded", {}) or {}
        storage_channel = _normalize_channel(channel)
        # Derive channel (fallback to 0 if not present)
        message_channel = decoded.get("channel")
        if message_channel is None:
            message_channel = packet.get("channel")
        message_channel = _normalize_channel(message_channel)
        if storage_channel is None:
            storage_channel = message_channel
        if message_channel is None:
            message_channel = storage_channel
        if message_channel is None:
            message_channel = 0

        # Derive text (typical field is 'text')
        text = decoded.get("text")
        if not text:
            # Some decoders may only expose the raw payload; make a best-effort decode
            payload = decoded.get("payload")
            if isinstance(payload, (bytes, bytearray)):
                try:
                    text = bytes(payload).decode("utf-8", errors="replace")
                except Exception:
                    text = None
            elif isinstance(payload, str):
                text = payload

        if not text:
            return None

        node_num = packet.get("from")
        peer_node_num = _dm_peer_node_num(node_database_number, packet)
        if message_channel in (None, 0) and peer_node_num is not None:
            return MessageDB(
                node_database_number,
                db_path,
                channel=None,
                channel_name=channel_name,
                storage_name=storage_name,
            ).save_dm_message(
                peer_node_num,
                node_num,
                text,
                timestamp=packet.get("rxTime"),
                packet=packet,
            )
        return MessageDB(
            node_database_number,
            db_path,
            channel=storage_channel,
            channel_name=channel_name,
            storage_name=storage_name,
        ).save_message(
            message_channel,
            node_num,
            text,
            timestamp=packet.get("rxTime"),
            packet=packet,
        )
    except sqlite3.Error as e:
        logging.error(f"SQLite error in store_text_message_packet: {e}")
    except Exception as e:
        logging.error(f"Unexpected error in store_text_message_packet: {e}")
    return None


# ------------------------------
# Owner node helpers
# ------------------------------


def get_connected_device_node_num(iface) -> Optional[int]:
    """Return the connected device's own node number (if available) and
    **also** sync the device's NodeDB into the local SQL DB as a side effect.

    The sync uses the library's current default DB base path set via
    `set_default_db_path(...)`. If none is set, it falls back to the CWD.
    """
    try:
        info = iface.getMyNodeInfo()
        if isinstance(info, dict):
            num = info.get("num")
            if isinstance(num, int):
                try:
                    sync_owner_nodeinfo(num, info, db_path=None)
                except Exception as e:
                    logging.debug(f"sync_owner_nodeinfo skipped: {e}")
                # Best-effort: pull the device NodeDB and merge locally
                try:
                    sync_nodes_from_interface(num, iface, db_path=None)
                except Exception as e:
                    logging.debug(f"sync_nodes_from_interface skipped: {e}")
                return num
        return None
    except Exception:
        return None


# ------------------------------
# Sync node database from a connected Meshtastic interface
# ------------------------------


def _extract_nodes_from_interface(iface) -> List[Dict[str, object]]:
    """Best-effort extraction of device-style node snapshots from a Meshtastic interface.

    Supports multiple library versions:
      - iface.getNodeDB() -> list[dict] or dict[num]->dict
      - iface.nodes (dict or list)
    Returns a list of node dicts shaped like the examples Ben provided.
    """
    nodes: List[Dict[str, object]] = []

    # 1) Preferred API: getNodeDB()
    try:
        get_db = getattr(iface, "getNodeDB", None)
        if callable(get_db):
            data = get_db()
            if isinstance(data, list):
                nodes = [n for n in data if isinstance(n, dict)]
            elif isinstance(data, dict):
                nodes = [n for n in data.values() if isinstance(n, dict)]
            if nodes:
                return nodes
    except Exception:
        pass

    # 2) Common attribute: nodes
    try:
        attr = getattr(iface, "nodes", None)
        if isinstance(attr, dict):
            return [n for n in attr.values() if isinstance(n, dict)]
        if isinstance(attr, list):
            return [n for n in attr if isinstance(n, dict)]
    except Exception:
        pass

    return nodes


def sync_nodes_from_interface(
    node_database_number: Union[int, str],
    iface,
    db_path: Optional[str] = None,
    channel: Optional[Union[int, str]] = None,
    channel_name: Optional[str] = None,
    storage_name: Optional[str] = None,
) -> int:
    """Download the connected device's NodeDB and merge it into the local DB.

    Returns the number of node entries ingested.
    """
    nodes = _extract_nodes_from_interface(iface)
    if not nodes:
        return 0
    NodeDB(
        node_database_number,
        db_path,
        channel=channel,
        channel_name=channel_name,
        storage_name=storage_name,
    ).init_from_interface_nodes(nodes)
    return len(nodes)


# ------------------------------
# High-level packet handler (library takes care of routing)
# ------------------------------


def _port_matches(port: object, *candidates: object) -> bool:
    """Return True if decoded.portnum matches any known candidate.
    Accepts names (str) or raw ints from older/newer libs.
    """
    return port in candidates


def handle_packet(
    packet: Dict[str, object],
    *,
    node_database_number: Union[int, str],
    db_path: Optional[str] = None,
    channel: Optional[Union[int, str]] = None,
    channel_name: Optional[str] = None,
    storage_name: Optional[str] = None,
) -> Dict[str, bool]:
    """Persist known Meshtastic packet types into the owner's DB.

    Returns a dict of what was stored, e.g. {"nodeinfo": True, "position": False, "telemetry": True}.
    """
    stored = {"nodeinfo": False, "position": False, "telemetry": False, "message": False, "touched_last_heard": False}

    try:
        decoded = packet.get("decoded", {}) or {}
        port = decoded.get("portnum")
        storage_channel = _normalize_channel(channel)
        if storage_channel is None:
            storage_channel = _normalize_channel(packet.get("channel"))
        if storage_channel is None:
            storage_channel = _normalize_channel(decoded.get("channel"))

        # Always update last_heard (and SNR if provided)
        try:
            NodeDB(
                node_database_number,
                db_path,
                channel=storage_channel,
                channel_name=channel_name,
                storage_name=storage_name,
            ).upsert(
                node_num=packet.get("from"),
                last_heard=packet.get("rxTime"),
                snr=packet.get("snr"),
            )
            stored["touched_last_heard"] = True
        except Exception:
            pass

        # NODEINFO
        if _port_matches(port, "NODEINFO_APP", 4):
            maybe_store_nodeinfo_in_db(
                packet,
                node_database_number=node_database_number,
                db_path=db_path,
                channel=storage_channel,
                channel_name=channel_name,
                storage_name=storage_name,
            )
            stored["nodeinfo"] = True

        # POSITION
        if _port_matches(port, "POSITION_APP", 3) or ("position" in decoded):
            if (
                store_location_packet(
                    packet,
                    node_database_number=node_database_number,
                    db_path=db_path,
                    channel=storage_channel,
                    channel_name=channel_name,
                    storage_name=storage_name,
                )
                is not None
            ):
                stored["position"] = True

        # TELEMETRY
        if _port_matches(port, "TELEMETRY_APP", 67) or ("telemetry" in decoded):
            if (
                store_telemetry_packet(
                    packet,
                    node_database_number=node_database_number,
                    db_path=db_path,
                    channel=storage_channel,
                    channel_name=channel_name,
                    storage_name=storage_name,
                )
                is not None
            ):
                stored["telemetry"] = True

        # TEXT MESSAGE
        if _port_matches(port, "TEXT_MESSAGE_APP", "1") or ("text" in decoded):
            if (
                store_text_message_packet(
                    packet,
                    node_database_number=node_database_number,
                    db_path=db_path,
                    channel=storage_channel,
                    channel_name=channel_name,
                    storage_name=storage_name,
                )
                is not None
            ):
                stored["message"] = True

    except Exception as e:
        logging.error(f"handle_packet error: {e}")

    return stored


# ------------------------------
# Convenience accessors for names
# ------------------------------


def get_long_name(
    node_num: Union[int, str],
    *,
    node_database_number: Union[int, str],
    db_path: Optional[str] = None,
    channel: Optional[Union[int, str]] = None,
    channel_name: Optional[str] = None,
    storage_name: Optional[str] = None,
) -> str:
    return NodeDB(
        node_database_number,
        db_path,
        channel=channel,
        channel_name=channel_name,
        storage_name=storage_name,
    ).get_name(
        int(node_num), kind="long"
    )


def get_short_name(
    node_num: Union[int, str],
    *,
    node_database_number: Union[int, str],
    db_path: Optional[str] = None,
    channel: Optional[Union[int, str]] = None,
    channel_name: Optional[str] = None,
    storage_name: Optional[str] = None,
) -> str:
    return NodeDB(
        node_database_number,
        db_path,
        channel=channel,
        channel_name=channel_name,
        storage_name=storage_name,
    ).get_name(
        int(node_num), kind="short"
    )
