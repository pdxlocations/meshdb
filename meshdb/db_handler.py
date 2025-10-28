import os
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

from meshdb.utils import decimal_to_hex


############################################################
# Library-style, class-based database handlers
# - Per-owned-node databases: one DB file per owner_node_num
# - Separate classes for NodeInfo, Location, and Messages
# - No UI globals required; callers can use return values
############################################################


def _default_db_path(base_path: Optional[str], owner_node_num: Union[int, str]) -> str:
    """Resolve a per-node database path.

    If base_path is a directory, create a file inside it named
    `node_<owner>.sqlite3`. If base_path is a file path, append
    `.<owner>` to its filename. If base_path is None, use current dir.
    """
    # If a per-call base_path was not provided, fall back to the package-wide default
    # that users may set via set_default_db_path().
    if base_path is None and DEFAULT_DB_BASE_PATH:
        base_path = DEFAULT_DB_BASE_PATH
    owner = str(owner_node_num)
    if not base_path:
        return os.path.abspath(f"{owner}.db")

    base_path = os.path.abspath(os.path.expanduser(base_path))
    if os.path.isdir(base_path):
        return os.path.join(base_path, f"{owner}.db")

    root, ext = os.path.splitext(base_path)
    if ext:
        return f"{root}.{owner}{ext}"
    return f"{base_path}.{owner}.sqlite3"


class _DB:
    """Lightweight connection helper that ensures tables and provides cursors."""

    def __init__(self, owner_node_num: Union[int, str], db_path: Optional[str] = None):
        self.owner_node_num = int(owner_node_num)
        self.db_path = _default_db_path(db_path, self.owner_node_num)
        # Ensure parent directory exists if a directory is implied
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)

    def connect(self):
        return sqlite3.connect(self.db_path)

    @property
    def owner(self) -> int:
        return self.owner_node_num


class NodeDB(_DB):
    """CRUD utilities for the per-owner node database table (…_nodedb)."""

    @property
    def table(self) -> str:
        return f'"{self.owner}_nodedb"'

    def ensure_table(self) -> None:
        schema = (
            "node_id TEXT PRIMARY KEY,"
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
        node_id: Union[int, str],
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
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(f"SELECT * FROM {self.table} WHERE node_id = ?", (node_id,))
            existing = cur.fetchone()

            if existing:
                (
                    _node_id,
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

            long_name = (
                long_name
                if long_name is not None
                else (ex_long if ex_long is not None else "Meshtastic " + str(decimal_to_hex(node_id)[-4:]))
            )
            short_name = (
                short_name
                if short_name is not None
                else (ex_short if ex_short is not None else str(decimal_to_hex(node_id)[-4:]))
            )
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
                    (node_id, long_name, short_name, macaddr, hw_model, role, is_licensed, public_key, is_unmessagable, last_heard, hops_away, snr)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(node_id) DO UPDATE SET
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
                    node_id,
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

    def get_name(self, node_id: int, kind: str = "long") -> str:
        """Return long or short name; fallback to hex string when missing."""
        self.ensure_table()
        col = "long_name" if kind == "long" else "short_name"
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(f"SELECT {col} FROM {self.table} WHERE node_id = ?", (node_id,))
            row = cur.fetchone()
            return row[0] if row and row[0] else decimal_to_hex(node_id)

    def init_from_interface_nodes(self, nodes: List[Dict[str, object]]) -> None:
        """Initialize/populate the node table from an iterable of node dicts."""
        for node in list(nodes):
            self.upsert(
                node_id=node.get("num"),
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
        return f'"{self.owner}_location"'

    def ensure_table(self) -> None:
        schema = (
            "node_id TEXT,"
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
            "precision INTEGER"  # legacy compatibility
        )
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(f"CREATE TABLE IF NOT EXISTS {self.table} ({schema})")
            # Index to speed up history queries
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.owner}_loc_user_time ON {self.table} (node_id, timestamp)"
            )
            cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS uniq_{self.owner}_loc_user ON {self.table} (node_id)")
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
            con.commit()

    def save_packet(self, packet: Dict[str, object]) -> int:
        """Save a location packet. Expects a Meshtastic-like decoded dict.

        Returns the stored timestamp.
        """
        self.ensure_table()
        node_id = packet.get("from")
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
            cur.execute(
                f"INSERT INTO {self.table} ("
                "node_id, timestamp, latitude, longitude, latitude_i, longitude_i, altitude, location_source, altitude_source, "
                "pos_time, pos_timestamp, pos_timestamp_ms_adjust, altitude_hae, altitude_geoidal_separation, pdop, hdop, vdop, gps_accuracy, "
                "ground_speed, ground_track, fix_quality, fix_type, sats_in_view, sensor_id, next_update, seq_number, precision_bits, precision"
                ") VALUES (?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?, ?) "
                "ON CONFLICT(node_id) DO UPDATE SET "
                "timestamp=excluded.timestamp, latitude=excluded.latitude, longitude=excluded.longitude, "
                "latitude_i=excluded.latitude_i, longitude_i=excluded.longitude_i, altitude=excluded.altitude, "
                "location_source=excluded.location_source, altitude_source=excluded.altitude_source, "
                "pos_time=excluded.pos_time, pos_timestamp=excluded.pos_timestamp, pos_timestamp_ms_adjust=excluded.pos_timestamp_ms_adjust, "
                "altitude_hae=excluded.altitude_hae, altitude_geoidal_separation=excluded.altitude_geoidal_separation, "
                "pdop=excluded.pdop, hdop=excluded.hdop, vdop=excluded.vdop, gps_accuracy=excluded.gps_accuracy, "
                "ground_speed=excluded.ground_speed, ground_track=excluded.ground_track, fix_quality=excluded.fix_quality, "
                "fix_type=excluded.fix_type, sats_in_view=excluded.sats_in_view, sensor_id=excluded.sensor_id, next_update=excluded.next_update, "
                "seq_number=excluded.seq_number, precision_bits=excluded.precision_bits, precision=excluded.precision",
                (
                    node_id,
                    timestamp,
                    lat,
                    lon,
                    lat_i,
                    lon_i,
                    alt,
                    loc_src,
                    alt_src,
                    pos_time,
                    pos_ts,
                    pos_ts_adj,
                    alt_hae,
                    alt_geo_sep,
                    pdop,
                    hdop,
                    vdop,
                    gps_acc,
                    gspd,
                    gtrk,
                    fix_q,
                    fix_t,
                    sats,
                    sensor_id,
                    next_upd,
                    seq_no,
                    prec_bits,
                    prec_bits,
                ),
            )
            con.commit()
        return timestamp

    def latest_for_user(self, node_id: Union[int, str]) -> Optional[Tuple[int, float, float]]:
        self.ensure_table()
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                f"SELECT timestamp, latitude, longitude FROM {self.table} WHERE node_id = ? ORDER BY timestamp DESC LIMIT 1",
                (node_id,),
            )
            row = cur.fetchone()
            return (row[0], row[1], row[2]) if row else None

    def history_for_user(
        self, node_id: Union[int, str], since_ts: Optional[int] = None, limit: int = 1000
    ) -> List[Tuple[int, float, float]]:
        self.ensure_table()
        with self.connect() as con:
            cur = con.cursor()
            if since_ts:
                cur.execute(
                    f"SELECT timestamp, latitude, longitude FROM {self.table} WHERE node_id = ? AND timestamp >= ? ORDER BY timestamp ASC LIMIT ?",
                    (node_id, since_ts, limit),
                )
            else:
                cur.execute(
                    f"SELECT timestamp, latitude, longitude FROM {self.table} WHERE node_id = ? ORDER BY timestamp ASC LIMIT ?",
                    (node_id, limit),
                )
            return [(r[0], r[1], r[2]) for r in cur.fetchall()]


class TelemetryDB(_DB):
    """Storage for Meshtastic telemetry metrics.

    Creates typed tables for each common metrics variant:
      - <owner>_telemetry_device(node_id, timestamp, battery_level, voltage, channel_utilization, air_util_tx, uptime_seconds)
      - <owner>_telemetry_power(node_id, timestamp, ch1_voltage, ch1_current, ch2_voltage, ch2_current, ch3_voltage, ch3_current, ch4_voltage, ch4_current, ch5_voltage, ch5_current, ch6_voltage, ch6_current, ch7_voltage, ch7_current, ch8_voltage, ch8_current)
      - <owner>_telemetry_environment(node_id, timestamp, temperature, relative_humidity, barometric_pressure, gas_resistance, voltage, current, iaq, distance, lux, white_lux, ir_lux, uv_lux, wind_direction, wind_speed, weight, wind_gust, wind_lull, radiation, rainfall_1h, rainfall_24h, soil_moisture, soil_temperature)
      - <owner>_telemetry_air_quality(node_id, timestamp, pm10_standard, pm25_standard, pm100_standard, pm10_environmental, pm25_environmental, pm100_environmental, particles_03um, particles_05um, particles_10um, particles_25um, particles_50um, particles_100um, co2, co2_temperature, co2_humidity, form_formaldehyde, form_humidity, form_temperature, pm40_standard, particles_40um, pm_temperature, pm_humidity, pm_voc_idx, pm_nox_idx, particles_tps)
      - <owner>_telemetry_local_stats(node_id, timestamp, uptime_seconds, channel_utilization, air_util_tx, num_packets_tx, num_packets_rx, num_packets_rx_bad, num_online_nodes, num_total_nodes, num_rx_dupe, num_tx_relay, num_tx_relay_canceled, heap_total_bytes, heap_free_bytes, num_tx_dropped)
      - <owner>_telemetry_health(node_id, timestamp, heart_bpm, spO2, temperature)
      - <owner>_telemetry_host(node_id, timestamp, uptime_seconds, freemem_bytes, diskfree1_bytes, diskfree2_bytes, diskfree3_bytes, load1, load5, load15, user_string)
    """

    @property
    def table_device(self) -> str:
        return f'"{self.owner}_telemetry_device"'

    @property
    def table_power(self) -> str:
        return f'"{self.owner}_telemetry_power"'

    @property
    def table_environment(self) -> str:
        return f'"{self.owner}_telemetry_environment"'

    @property
    def table_air_quality(self) -> str:
        return f'"{self.owner}_telemetry_air_quality"'

    @property
    def table_local_stats(self) -> str:
        return f'"{self.owner}_telemetry_local_stats"'

    @property
    def table_health(self) -> str:
        return f'"{self.owner}_telemetry_health"'

    @property
    def table_host(self) -> str:
        return f'"{self.owner}_telemetry_host"'

    def ensure_tables(self) -> None:
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {self.table_device} ("
                "node_id TEXT,"
                "timestamp INTEGER,"
                "battery_level REAL,"
                "voltage REAL,"
                "channel_utilization REAL,"
                "air_util_tx REAL,"
                "uptime_seconds INTEGER"
                ")"
            )
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {self.table_power} ("
                "node_id TEXT,"
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
                "ch8_current REAL"
                ")"
            )
            # Environment
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {self.table_environment} ("
                "node_id TEXT,"
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
                "soil_temperature REAL"
                ")"
            )

            # Air Quality
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {self.table_air_quality} ("
                "node_id TEXT,"
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
                "particles_tps REAL"
                ")"
            )

            # Local Stats
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {self.table_local_stats} ("
                "node_id TEXT,"
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
                "num_tx_dropped INTEGER"
                ")"
            )

            # Health
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {self.table_health} ("
                "node_id TEXT,"
                "timestamp INTEGER,"
                "heart_bpm INTEGER,"
                "spO2 INTEGER,"
                "temperature REAL"
                ")"
            )

            # Host
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {self.table_host} ("
                "node_id TEXT,"
                "timestamp INTEGER,"
                "uptime_seconds INTEGER,"
                "freemem_bytes INTEGER,"
                "diskfree1_bytes INTEGER,"
                "diskfree2_bytes INTEGER,"
                "diskfree3_bytes INTEGER,"
                "load1 INTEGER,"
                "load5 INTEGER,"
                "load15 INTEGER,"
                "user_string TEXT"
                ")"
            )
            # Helpful indices
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.owner}_td_user_time ON {self.table_device} (node_id, timestamp)"
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.owner}_tp_user_time ON {self.table_power} (node_id, timestamp)"
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.owner}_tenv_user_time ON {self.table_environment} (node_id, timestamp)"
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.owner}_taq_user_time ON {self.table_air_quality} (node_id, timestamp)"
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.owner}_tls_user_time ON {self.table_local_stats} (node_id, timestamp)"
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.owner}_th_user_time ON {self.table_health} (node_id, timestamp)"
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.owner}_thost_user_time ON {self.table_host} (node_id, timestamp)"
            )
            # Add unique indices for overwrite-on-insert (upsert) per node_id
            cur.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS uniq_{self.owner}_td_user ON {self.table_device} (node_id)"
            )
            cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS uniq_{self.owner}_tp_user ON {self.table_power} (node_id)")
            cur.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS uniq_{self.owner}_tenv_user ON {self.table_environment} (node_id)"
            )
            cur.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS uniq_{self.owner}_taq_user ON {self.table_air_quality} (node_id)"
            )
            cur.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS uniq_{self.owner}_tls_user ON {self.table_local_stats} (node_id)"
            )
            cur.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS uniq_{self.owner}_th_user ON {self.table_health} (node_id)"
            )
            cur.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS uniq_{self.owner}_thost_user ON {self.table_host} (node_id)"
            )
            con.commit()

    def save_packet(self, packet: Dict[str, object]) -> int:
        """Persist any telemetry metrics present in a decoded packet.

        Returns the stored timestamp.
        """
        self.ensure_tables()
        node_id = packet.get("from")
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
                cur.execute(
                    f"INSERT INTO {self.table_device} (node_id, timestamp, battery_level, voltage, channel_utilization, air_util_tx, uptime_seconds) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(node_id) DO UPDATE SET "
                    "timestamp=excluded.timestamp, battery_level=excluded.battery_level, voltage=excluded.voltage, "
                    "channel_utilization=excluded.channel_utilization, air_util_tx=excluded.air_util_tx, uptime_seconds=excluded.uptime_seconds",
                    (
                        node_id,
                        ts,
                        device.get("batteryLevel"),
                        device.get("voltage"),
                        device.get("channelUtilization"),
                        device.get("airUtilTx"),
                        device.get("uptimeSeconds"),
                    ),
                )

            if isinstance(power, dict):
                cur.execute(
                    f"INSERT INTO {self.table_power} (node_id, timestamp, ch1_voltage, ch1_current, ch2_voltage, ch2_current, ch3_voltage, ch3_current, ch4_voltage, ch4_current, ch5_voltage, ch5_current, ch6_voltage, ch6_current, ch7_voltage, ch7_current, ch8_voltage, ch8_current) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(node_id) DO UPDATE SET "
                    "timestamp=excluded.timestamp, ch1_voltage=excluded.ch1_voltage, ch1_current=excluded.ch1_current, "
                    "ch2_voltage=excluded.ch2_voltage, ch2_current=excluded.ch2_current, ch3_voltage=excluded.ch3_voltage, ch3_current=excluded.ch3_current, "
                    "ch4_voltage=excluded.ch4_voltage, ch4_current=excluded.ch4_current, ch5_voltage=excluded.ch5_voltage, ch5_current=excluded.ch5_current, "
                    "ch6_voltage=excluded.ch6_voltage, ch6_current=excluded.ch6_current, ch7_voltage=excluded.ch7_voltage, ch7_current=excluded.ch7_current, "
                    "ch8_voltage=excluded.ch8_voltage, ch8_current=excluded.ch8_current",
                    (
                        node_id,
                        ts,
                        power.get("ch1Voltage"),
                        power.get("ch1Current"),
                        power.get("ch2Voltage"),
                        power.get("ch2Current"),
                        power.get("ch3Voltage"),
                        power.get("ch3Current"),
                        power.get("ch4Voltage"),
                        power.get("ch4Current"),
                        power.get("ch5Voltage"),
                        power.get("ch5Current"),
                        power.get("ch6Voltage"),
                        power.get("ch6Current"),
                        power.get("ch7Voltage"),
                        power.get("ch7Current"),
                        power.get("ch8Voltage"),
                        power.get("ch8Current"),
                    ),
                )

            if isinstance(env, dict):
                cur.execute(
                    f"INSERT INTO {self.table_environment} (node_id, timestamp, temperature, relative_humidity, barometric_pressure, gas_resistance, voltage, current, iaq, distance, lux, white_lux, ir_lux, uv_lux, wind_direction, wind_speed, weight, wind_gust, wind_lull, radiation, rainfall_1h, rainfall_24h, soil_moisture, soil_temperature) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(node_id) DO UPDATE SET "
                    "timestamp=excluded.timestamp, temperature=excluded.temperature, relative_humidity=excluded.relative_humidity, "
                    "barometric_pressure=excluded.barometric_pressure, gas_resistance=excluded.gas_resistance, voltage=excluded.voltage, current=excluded.current, "
                    "iaq=excluded.iaq, distance=excluded.distance, lux=excluded.lux, white_lux=excluded.white_lux, ir_lux=excluded.ir_lux, uv_lux=excluded.uv_lux, "
                    "wind_direction=excluded.wind_direction, wind_speed=excluded.wind_speed, weight=excluded.weight, wind_gust=excluded.wind_gust, wind_lull=excluded.wind_lull, "
                    "radiation=excluded.radiation, rainfall_1h=excluded.rainfall_1h, rainfall_24h=excluded.rainfall_24h, soil_moisture=excluded.soil_moisture, soil_temperature=excluded.soil_temperature",
                    (
                        node_id,
                        ts,
                        env.get("temperature"),
                        env.get("relativeHumidity") if "relativeHumidity" in env else env.get("relative_humidity"),
                        (
                            env.get("barometricPressure")
                            if "barometricPressure" in env
                            else env.get("barometric_pressure")
                        ),
                        env.get("gasResistance") if "gasResistance" in env else env.get("gas_resistance"),
                        env.get("voltage"),
                        env.get("current"),
                        env.get("iaq"),
                        env.get("distance"),
                        env.get("lux"),
                        env.get("whiteLux") if "whiteLux" in env else env.get("white_lux"),
                        env.get("irLux") if "irLux" in env else env.get("ir_lux"),
                        env.get("uvLux") if "uvLux" in env else env.get("uv_lux"),
                        env.get("windDirection") if "windDirection" in env else env.get("wind_direction"),
                        env.get("windSpeed") if "windSpeed" in env else env.get("wind_speed"),
                        env.get("weight"),
                        env.get("windGust") if "windGust" in env else env.get("wind_gust"),
                        env.get("windLull") if "windLull" in env else env.get("wind_lull"),
                        env.get("radiation"),
                        env.get("rainfall1h") if "rainfall1h" in env else env.get("rainfall_1h"),
                        env.get("rainfall24h") if "rainfall24h" in env else env.get("rainfall_24h"),
                        env.get("soilMoisture") if "soilMoisture" in env else env.get("soil_moisture"),
                        env.get("soilTemperature") if "soilTemperature" in env else env.get("soil_temperature"),
                    ),
                )

            if isinstance(aq, dict):
                cur.execute(
                    f"INSERT INTO {self.table_air_quality} (node_id, timestamp, pm10_standard, pm25_standard, pm100_standard, pm10_environmental, pm25_environmental, pm100_environmental, particles_03um, particles_05um, particles_10um, particles_25um, particles_50um, particles_100um, co2, co2_temperature, co2_humidity, form_formaldehyde, form_humidity, form_temperature, pm40_standard, particles_40um, pm_temperature, pm_humidity, pm_voc_idx, pm_nox_idx, particles_tps) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(node_id) DO UPDATE SET "
                    "timestamp=excluded.timestamp, pm10_standard=excluded.pm10_standard, pm25_standard=excluded.pm25_standard, pm100_standard=excluded.pm100_standard, "
                    "pm10_environmental=excluded.pm10_environmental, pm25_environmental=excluded.pm25_environmental, pm100_environmental=excluded.pm100_environmental, "
                    "particles_03um=excluded.particles_03um, particles_05um=excluded.particles_05um, particles_10um=excluded.particles_10um, particles_25um=excluded.particles_25um, "
                    "particles_50um=excluded.particles_50um, particles_100um=excluded.particles_100um, co2=excluded.co2, co2_temperature=excluded.co2_temperature, co2_humidity=excluded.co2_humidity, "
                    "form_formaldehyde=excluded.form_formaldehyde, form_humidity=excluded.form_humidity, form_temperature=excluded.form_temperature, pm40_standard=excluded.pm40_standard, "
                    "particles_40um=excluded.particles_40um, pm_temperature=excluded.pm_temperature, pm_humidity=excluded.pm_humidity, pm_voc_idx=excluded.pm_voc_idx, pm_nox_idx=excluded.pm_nox_idx, particles_tps=excluded.particles_tps",
                    (
                        node_id,
                        ts,
                        aq.get("pm10Standard") if "pm10Standard" in aq else aq.get("pm10_standard"),
                        aq.get("pm25Standard") if "pm25Standard" in aq else aq.get("pm25_standard"),
                        aq.get("pm100Standard") if "pm100Standard" in aq else aq.get("pm100_standard"),
                        aq.get("pm10Environmental") if "pm10Environmental" in aq else aq.get("pm10_environmental"),
                        aq.get("pm25Environmental") if "pm25Environmental" in aq else aq.get("pm25_environmental"),
                        aq.get("pm100Environmental") if "pm100Environmental" in aq else aq.get("pm100_environmental"),
                        aq.get("particles03um") if "particles03um" in aq else aq.get("particles_03um"),
                        aq.get("particles05um") if "particles05um" in aq else aq.get("particles_05um"),
                        aq.get("particles10um") if "particles10um" in aq else aq.get("particles_10um"),
                        aq.get("particles25um") if "particles25um" in aq else aq.get("particles_25um"),
                        aq.get("particles50um") if "particles50um" in aq else aq.get("particles_50um"),
                        aq.get("particles100um") if "particles100um" in aq else aq.get("particles_100um"),
                        aq.get("co2"),
                        aq.get("co2Temperature") if "co2Temperature" in aq else aq.get("co2_temperature"),
                        aq.get("co2Humidity") if "co2Humidity" in aq else aq.get("co2_humidity"),
                        aq.get("formFormaldehyde") if "formFormaldehyde" in aq else aq.get("form_formaldehyde"),
                        aq.get("formHumidity") if "formHumidity" in aq else aq.get("form_humidity"),
                        aq.get("formTemperature") if "formTemperature" in aq else aq.get("form_temperature"),
                        aq.get("pm40Standard") if "pm40Standard" in aq else aq.get("pm40_standard"),
                        aq.get("particles40um") if "particles40um" in aq else aq.get("particles_40um"),
                        aq.get("pmTemperature") if "pmTemperature" in aq else aq.get("pm_temperature"),
                        aq.get("pmHumidity") if "pmHumidity" in aq else aq.get("pm_humidity"),
                        aq.get("pmVocIdx") if "pmVocIdx" in aq else aq.get("pm_voc_idx"),
                        aq.get("pmNoxIdx") if "pmNoxIdx" in aq else aq.get("pm_nox_idx"),
                        aq.get("particlesTps") if "particlesTps" in aq else aq.get("particles_tps"),
                    ),
                )

            if isinstance(ls, dict):
                cur.execute(
                    f"INSERT INTO {self.table_local_stats} (node_id, timestamp, uptime_seconds, channel_utilization, air_util_tx, num_packets_tx, num_packets_rx, num_packets_rx_bad, num_online_nodes, num_total_nodes, num_rx_dupe, num_tx_relay, num_tx_relay_canceled, heap_total_bytes, heap_free_bytes, num_tx_dropped) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(node_id) DO UPDATE SET "
                    "timestamp=excluded.timestamp, uptime_seconds=excluded.uptime_seconds, channel_utilization=excluded.channel_utilization, "
                    "air_util_tx=excluded.air_util_tx, num_packets_tx=excluded.num_packets_tx, num_packets_rx=excluded.num_packets_rx, "
                    "num_packets_rx_bad=excluded.num_packets_rx_bad, num_online_nodes=excluded.num_online_nodes, num_total_nodes=excluded.num_total_nodes, "
                    "num_rx_dupe=excluded.num_rx_dupe, num_tx_relay=excluded.num_tx_relay, num_tx_relay_canceled=excluded.num_tx_relay_canceled, "
                    "heap_total_bytes=excluded.heap_total_bytes, heap_free_bytes=excluded.heap_free_bytes, num_tx_dropped=excluded.num_tx_dropped",
                    (
                        node_id,
                        ts,
                        ls.get("uptimeSeconds") if "uptimeSeconds" in ls else ls.get("uptime_seconds"),
                        ls.get("channelUtilization") if "channelUtilization" in ls else ls.get("channel_utilization"),
                        ls.get("airUtilTx") if "airUtilTx" in ls else ls.get("air_util_tx"),
                        ls.get("numPacketsTx") if "numPacketsTx" in ls else ls.get("num_packets_tx"),
                        ls.get("numPacketsRx") if "numPacketsRx" in ls else ls.get("num_packets_rx"),
                        ls.get("numPacketsRxBad") if "numPacketsRxBad" in ls else ls.get("num_packets_rx_bad"),
                        ls.get("numOnlineNodes") if "numOnlineNodes" in ls else ls.get("num_online_nodes"),
                        ls.get("numTotalNodes") if "numTotalNodes" in ls else ls.get("num_total_nodes"),
                        ls.get("numRxDupe") if "numRxDupe" in ls else ls.get("num_rx_dupe"),
                        ls.get("numTxRelay") if "numTxRelay" in ls else ls.get("num_tx_relay"),
                        (
                            ls.get("numTxRelayCanceled")
                            if "numTxRelayCanceled" in ls
                            else ls.get("num_tx_relay_canceled")
                        ),
                        ls.get("heapTotalBytes") if "heapTotalBytes" in ls else ls.get("heap_total_bytes"),
                        ls.get("heapFreeBytes") if "heapFreeBytes" in ls else ls.get("heap_free_bytes"),
                        ls.get("numTxDropped") if "numTxDropped" in ls else ls.get("num_tx_dropped"),
                    ),
                )

            if isinstance(health, dict):
                cur.execute(
                    f"INSERT INTO {self.table_health} (node_id, timestamp, heart_bpm, spO2, temperature) "
                    "VALUES (?,?,?,?,?) "
                    "ON CONFLICT(node_id) DO UPDATE SET "
                    "timestamp=excluded.timestamp, heart_bpm=excluded.heart_bpm, spO2=excluded.spO2, temperature=excluded.temperature",
                    (
                        node_id,
                        ts,
                        health.get("heartBpm") if "heartBpm" in health else health.get("heart_bpm"),
                        health.get("spO2") if "spO2" in health else health.get("spO2"),
                        health.get("temperature"),
                    ),
                )

            if isinstance(host, dict):
                cur.execute(
                    f"INSERT INTO {self.table_host} (node_id, timestamp, uptime_seconds, freemem_bytes, diskfree1_bytes, diskfree2_bytes, diskfree3_bytes, load1, load5, load15, user_string) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(node_id) DO UPDATE SET "
                    "timestamp=excluded.timestamp, uptime_seconds=excluded.uptime_seconds, freemem_bytes=excluded.freemem_bytes, "
                    "diskfree1_bytes=excluded.diskfree1_bytes, diskfree2_bytes=excluded.diskfree2_bytes, diskfree3_bytes=excluded.diskfree3_bytes, "
                    "load1=excluded.load1, load5=excluded.load5, load15=excluded.load15, user_string=excluded.user_string",
                    (
                        node_id,
                        ts,
                        host.get("uptimeSeconds") if "uptimeSeconds" in host else host.get("uptime_seconds"),
                        host.get("freememBytes") if "freememBytes" in host else host.get("freemem_bytes"),
                        host.get("diskfree1Bytes") if "diskfree1Bytes" in host else host.get("diskfree1_bytes"),
                        host.get("diskfree2Bytes") if "diskfree2Bytes" in host else host.get("diskfree2_bytes"),
                        host.get("diskfree3Bytes") if "diskfree3Bytes" in host else host.get("diskfree3_bytes"),
                        host.get("load1"),
                        host.get("load5"),
                        host.get("load15"),
                        host.get("userString") if "userString" in host else host.get("user_string"),
                    ),
                )

            con.commit()
        return ts


class MessageDB(_DB):
    """Per-channel message storage. Each owner has many channel tables."""

    def _table_for_channel(self, channel: Union[int, str]) -> str:
        table_name = f"{self.owner}_{channel}_messages"
        return f'"{table_name}"'

    def ensure_channel_table(self, channel: Union[int, str]) -> None:
        schema = "node_id TEXT," "message_text TEXT," "timestamp INTEGER"
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(f"CREATE TABLE IF NOT EXISTS {self._table_for_channel(channel)} ({schema})")
            con.commit()

    def save_message(self, channel: Union[int, str], node_id: Union[int, str], message_text: str) -> int:
        self.ensure_channel_table(channel)
        ts = int(time.time())
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                f"INSERT INTO {self._table_for_channel(channel)} (node_id, message_text, timestamp) VALUES (?, ?, ?)",
                (str(node_id), message_text, ts),
            )
            con.commit()
        return ts

    def update_ack_nak(
        self, channel: Union[int, str], timestamp: int, node_id: Union[int, str], message: str, ack: str
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
                    cur.execute(f"SELECT node_id, message_text, timestamp FROM {quoted}")
                else:
                    cur.execute(f"SELECT node_id, message_text, timestamp FROM {quoted}")
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
    channel: str, node_id: str, message_text: str, *, owner_node_num: Union[int, str], db_path: Optional[str] = None
) -> Optional[int]:
    try:
        return MessageDB(owner_node_num, db_path).save_message(channel, node_id, message_text)
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
    owner_node_num: Union[int, str],
    node_id: Union[int, str],
    db_path: Optional[str] = None,
) -> None:
    try:
        logging.debug("update_ack_nak wrapper called but ack_type support is removed; ignoring.")
        return
    except Exception:
        return


def get_name_from_database(
    node_id: int, kind: str = "long", *, owner_node_num: Union[int, str], db_path: Optional[str] = None
) -> str:
    try:
        return NodeDB(owner_node_num, db_path).get_name(node_id, kind)
    except sqlite3.Error as e:
        logging.error(f"SQLite error in get_name_from_database: {e}")
        return "Unknown"
    except Exception as e:
        logging.error(f"Unexpected error in get_name_from_database: {e}")
        return "Unknown"


def maybe_store_nodeinfo_in_db(
    packet: Dict[str, object], *, owner_node_num: Union[int, str], db_path: Optional[str] = None
) -> None:
    try:
        node_id = packet["from"]
        user = packet["decoded"]["user"]
        NodeDB(owner_node_num, db_path).upsert(
            node_id=node_id,
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


def store_location_packet(
    packet: Dict[str, object], *, owner_node_num: Union[int, str], db_path: Optional[str] = None
) -> Optional[int]:
    try:
        return LocationDB(owner_node_num, db_path).save_packet(packet)
    except sqlite3.Error as e:
        logging.error(f"SQLite error in store_location_packet: {e}")
    except Exception as e:
        logging.error(f"Unexpected error in store_location_packet: {e}")
    return None


def store_telemetry_packet(
    packet: Dict[str, object], *, owner_node_num: Union[int, str], db_path: Optional[str] = None
) -> Optional[int]:
    """Persist TELEMETRY_APP packets (deviceMetrics, powerMetrics, etc.)."""
    try:
        return TelemetryDB(owner_node_num, db_path).save_packet(packet)
    except sqlite3.Error as e:
        logging.error(f"SQLite error in store_telemetry_packet: {e}")
    except Exception as e:
        logging.error(f"Unexpected error in store_telemetry_packet: {e}")
    return None


# Store TEXT_MESSAGE_APP packets into the per-channel message tables.
def store_text_message_packet(
    packet: Dict[str, object], *, owner_node_num: Union[int, str], db_path: Optional[str] = None
) -> Optional[int]:
    """Persist TEXT_MESSAGE_APP packets into the per-channel message tables.

    We try a few common fields for channel and text to be compatible across lib versions.
    Returns the stored timestamp, or None if not stored.
    """
    try:
        decoded = packet.get("decoded", {}) or {}
        # Derive channel (fallback to 0 if not present)
        channel = decoded.get("channel")
        if channel is None:
            channel = packet.get("channel")
        if channel is None:
            channel = 0

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

        node_id = packet.get("from")
        return MessageDB(owner_node_num, db_path).save_message(channel, node_id, text)
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


def sync_nodes_from_interface(owner_node_num: Union[int, str], iface, db_path: Optional[str] = None) -> int:
    """Download the connected device's NodeDB and merge it into the local DB.

    Returns the number of node entries ingested.
    """
    nodes = _extract_nodes_from_interface(iface)
    if not nodes:
        return 0
    NodeDB(owner_node_num, db_path).init_from_interface_nodes(nodes)
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
    packet: Dict[str, object], *, owner_node_num: Union[int, str], db_path: Optional[str] = None
) -> Dict[str, bool]:
    """Persist known Meshtastic packet types into the owner's DB.

    Returns a dict of what was stored, e.g. {"nodeinfo": True, "position": False, "telemetry": True}.
    """
    stored = {"nodeinfo": False, "position": False, "telemetry": False, "message": False, "touched_last_heard": False}

    try:
        decoded = packet.get("decoded", {}) or {}
        port = decoded.get("portnum")

        # Always update last_heard (and SNR if provided)
        try:
            NodeDB(owner_node_num, db_path).upsert(
                node_id=packet.get("from"),
                last_heard=packet.get("rxTime"),
                snr=packet.get("snr"),
            )
            stored["touched_last_heard"] = True
        except Exception:
            pass

        # NODEINFO
        if _port_matches(port, "NODEINFO_APP", 4):
            maybe_store_nodeinfo_in_db(packet, owner_node_num=owner_node_num, db_path=db_path)
            stored["nodeinfo"] = True

        # POSITION
        if _port_matches(port, "POSITION_APP", 3) or ("position" in decoded):
            if store_location_packet(packet, owner_node_num=owner_node_num, db_path=db_path) is not None:
                stored["position"] = True

        # TELEMETRY
        if _port_matches(port, "TELEMETRY_APP", 67) or ("telemetry" in decoded):
            if store_telemetry_packet(packet, owner_node_num=owner_node_num, db_path=db_path) is not None:
                stored["telemetry"] = True

        # TEXT MESSAGE
        if _port_matches(port, "TEXT_MESSAGE_APP", "1") or ("text" in decoded):
            if store_text_message_packet(packet, owner_node_num=owner_node_num, db_path=db_path) is not None:
                stored["message"] = True

    except Exception as e:
        logging.error(f"handle_packet error: {e}")

    return stored


# ------------------------------
# Convenience accessors for names
# ------------------------------


def get_long_name(node_id: Union[int, str], *, owner_node_num: Union[int, str], db_path: Optional[str] = None) -> str:
    return NodeDB(owner_node_num, db_path).get_name(int(node_id), kind="long")


def get_short_name(node_id: Union[int, str], *, owner_node_num: Union[int, str], db_path: Optional[str] = None) -> str:
    return NodeDB(owner_node_num, db_path).get_name(int(node_id), kind="short")
