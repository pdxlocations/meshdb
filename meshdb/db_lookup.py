import re
from typing import List, Optional, Union

from .db_handler import NodeDB, LocationDB, TelemetryDB
from .utils import decimal_to_hex, hex_to_decimal

Identifier = Union[int, str]
ReturnType = Union[int, List[int], None]

_HEX_RE = re.compile(r"^[!]?([0-9a-fA-F]{3,16})$")
_HEX_SUFFIX_RE = re.compile(r"([0-9a-fA-F]{3,8})$")


def _is_int(value: Identifier) -> bool:
    return isinstance(value, int)


def _maybe_hex_chunk(text: str) -> Optional[str]:
    """Return a hex-like chunk if the input looks like hex (with or without '!')."""
    m = _HEX_RE.match(text.strip())
    if m:
        return m.group(1).lower()
    m = _HEX_SUFFIX_RE.search(text.strip())
    return m.group(1).lower() if m else None


def _query_all_node_nums(ndb: NodeDB) -> List[int]:
    ndb.ensure_table()
    with ndb.connect() as con:
        cur = con.cursor()
        cur.execute(f"SELECT node_num FROM {ndb.table}")
        out: List[int] = []
        for (val,) in cur.fetchall():
            try:
                out.append(int(val))
            except Exception:
                try:
                    out.append(int(str(val)))
                except Exception:
                    continue
        return out


def _match_by_hex_suffix(candidates: List[int], suffix_hex: str) -> List[int]:
    suffix_hex = suffix_hex.lower()
    hits: List[int] = []
    for num in candidates:
        hx = decimal_to_hex(num)[1:].lower()  # strip leading '!'
        if hx.endswith(suffix_hex):
            hits.append(num)
    return hits


def _query_by_name(ndb: NodeDB, name: str) -> List[int]:
    ndb.ensure_table()
    with ndb.connect() as con:
        cur = con.cursor()
        # Exact (case-insensitive) on short or long name
        cur.execute(
            f"SELECT node_num FROM {ndb.table} " "WHERE short_name = ? COLLATE NOCASE OR long_name = ? COLLATE NOCASE",
            (name, name),
        )
        rows = [int(r[0]) for r in cur.fetchall()]
        if rows:
            return rows
        # Fallback: substring match (case-insensitive)
        like = f"%{name}%"
        cur.execute(
            f"SELECT node_num FROM {ndb.table} "
            "WHERE short_name LIKE ? ESCAPE '\\' COLLATE NOCASE "
            "OR long_name LIKE ? ESCAPE '\\' COLLATE NOCASE",
            (like, like),
        )
        return [int(r[0]) for r in cur.fetchall()]


def get_node_num(
    identifier: Identifier, *, owner_node_num: Union[int, str], db_path: Optional[str] = None
) -> ReturnType:
    """
    Return the canonical node number for any identifier.

    Accepted forms:
      - int node number → returned as-is
      - '!deadbeef' or 'deadbeef' → hex → exact/partial suffix match on node_num's hex
      - 'FTS' (short name) or 'Futel - arbor SOL' (long name)
      - Free text with a trailing hex-like chunk, e.g. 'Meshtastic 1adc'

    Returns:
      - int if exactly one match
      - list[int] if multiple matches
      - None if no matches
    """
    ndb = NodeDB(owner_node_num, db_path)

    # 1) Already numeric
    if _is_int(identifier):
        return int(identifier)

    text = str(identifier).strip()

    # 2) Exact match by names (short/long)
    name_hits = _query_by_name(ndb, text)
    if len(name_hits) == 1:
        return name_hits[0]
    if len(name_hits) > 1:
        return name_hits

    # 3) Hex-like (exact or suffix)
    chunk = _maybe_hex_chunk(text)
    if chunk:
        # Try exact hex → decimal first
        try:
            exact_num = hex_to_decimal("!" + chunk)
            # confirm it exists
            ndb.ensure_table()
            with ndb.connect() as con:
                cur = con.cursor()
                cur.execute(f"SELECT 1 FROM {ndb.table} WHERE node_num = ? LIMIT 1", (exact_num,))
                if cur.fetchone():
                    return exact_num
        except Exception:
            pass

        # Suffix search over all known nodes
        nums = _query_all_node_nums(ndb)
        suffix_hits = _match_by_hex_suffix(nums, chunk)
        if len(suffix_hits) == 1:
            return suffix_hits[0]
        if len(suffix_hits) > 1:
            return suffix_hits

    # 4) No matches
    return None


# ------------------------------
# Snapshot helpers
# ------------------------------

from typing import Dict, Any


def _fetch_one_as_dict(con, query: str, params: tuple) -> Optional[Dict[str, Any]]:
    cur = con.cursor()
    cur.execute(query, params)
    row = cur.fetchone()
    if not row:
        return None
    cols = [d[0] for d in cur.description]
    return {c: row[i] for i, c in enumerate(cols)}


def _resolve_to_list(identifier: Identifier, *, owner_node_num: Union[int, str], db_path: Optional[str]) -> List[int]:
    """Resolve any identifier to a list of node_nums (possibly empty)."""
    hit = get_node_num(identifier, owner_node_num=owner_node_num, db_path=db_path)
    if hit is None:
        return []
    if isinstance(hit, list):
        return hit
    return [int(hit)]


def get_nodeinfo(
    identifier: Identifier, *, owner_node_num: Union[int, str], db_path: Optional[str] = None
) -> Union[Dict[str, Any], List[Dict[str, Any]], None]:
    """Return nodeinfo row(s) for the identifier. If ambiguous returns a list, if none returns None."""
    ndb = NodeDB(owner_node_num, db_path)
    ndb.ensure_table()
    nums = _resolve_to_list(identifier, owner_node_num=owner_node_num, db_path=db_path)
    if not nums:
        return None
    out: List[Dict[str, Any]] = []
    with ndb.connect() as con:
        for num in nums:
            row = _fetch_one_as_dict(con, f"SELECT * FROM {ndb.table} WHERE node_num = ?", (num,))
            if row:
                out.append(row)
    if not out:
        return None
    return out[0] if len(out) == 1 else out


def _latest_location_dict(
    owner_node_num: Union[int, str], num: int, db_path: Optional[str]
) -> Optional[Dict[str, Any]]:
    ldb = LocationDB(owner_node_num, db_path)
    ldb.ensure_table()
    with ldb.connect() as con:
        return _fetch_one_as_dict(
            con,
            f"SELECT * FROM {ldb.table} WHERE node_num = ? ORDER BY timestamp DESC LIMIT 1",
            (num,),
        )


def _latest_telem_dicts(
    owner_node_num: Union[int, str], num: int, db_path: Optional[str]
) -> Dict[str, Dict[str, Any]]:
    tdb = TelemetryDB(owner_node_num, db_path)
    tdb.ensure_tables()
    out: Dict[str, Dict[str, Any]] = {}
    with tdb.connect() as con:
        dev = _fetch_one_as_dict(
            con, f"SELECT * FROM {tdb.table_device} WHERE node_num = ? ORDER BY timestamp DESC LIMIT 1", (num,)
        )
        if dev:
            out["device"] = dev
        pwr = _fetch_one_as_dict(
            con, f"SELECT * FROM {tdb.table_power} WHERE node_num = ? ORDER BY timestamp DESC LIMIT 1", (num,)
        )
        if pwr:
            out["power"] = pwr
        env = _fetch_one_as_dict(
            con, f"SELECT * FROM {tdb.table_environment} WHERE node_num = ? ORDER BY timestamp DESC LIMIT 1", (num,)
        )
        if env:
            out["environment"] = env
        aq = _fetch_one_as_dict(
            con, f"SELECT * FROM {tdb.table_air_quality} WHERE node_num = ? ORDER BY timestamp DESC LIMIT 1", (num,)
        )
        if aq:
            out["air_quality"] = aq
        ls = _fetch_one_as_dict(
            con, f"SELECT * FROM {tdb.table_local_stats} WHERE node_num = ? ORDER BY timestamp DESC LIMIT 1", (num,)
        )
        if ls:
            out["local_stats"] = ls
        hl = _fetch_one_as_dict(
            con, f"SELECT * FROM {tdb.table_health} WHERE node_num = ? ORDER BY timestamp DESC LIMIT 1", (num,)
        )
        if hl:
            out["health"] = hl
        host = _fetch_one_as_dict(
            con, f"SELECT * FROM {tdb.table_host} WHERE node_num = ? ORDER BY timestamp DESC LIMIT 1", (num,)
        )
        if host:
            out["host"] = host
    return out


def get_node(
    identifier: Identifier, *, owner_node_num: Union[int, str], db_path: Optional[str] = None
) -> Union[Dict[str, Any], List[Dict[str, Any]], None]:
    """Return a consolidated snapshot for the node(s): nodeinfo + latest position + latest telemetry types.

    - If the identifier resolves to a single node: returns one dict.
    - If it resolves to many nodes: returns a list of dicts.
    - If no nodes match: returns None.

    Keys omitted when not available; e.g. no telemetry → no 'telemetry' key.
    """
    nums = _resolve_to_list(identifier, owner_node_num=owner_node_num, db_path=db_path)
    if not nums:
        return None

    snapshots: List[Dict[str, Any]] = []
    ndb = NodeDB(owner_node_num, db_path)
    ndb.ensure_table()
    with ndb.connect() as con:
        for num in nums:
            nodeinfo = _fetch_one_as_dict(con, f"SELECT * FROM {ndb.table} WHERE node_num = ?", (num,))
            snap: Dict[str, Any] = {"node_num": num}
            if nodeinfo:
                snap["nodeinfo"] = nodeinfo
            loc = _latest_location_dict(owner_node_num, num, db_path)
            if loc:
                snap["position"] = loc
            telem = _latest_telem_dicts(owner_node_num, num, db_path)
            if telem:
                snap["telemetry"] = telem
            snapshots.append(snap)

    return snapshots[0] if len(snapshots) == 1 else snapshots


def get_node_metric(
    identifier: Identifier, metric: str, *, owner_node_num: Union[int, str], db_path: Optional[str] = None
) -> Optional[Union[int, float, str]]:
    """
    Convenience helper: return a single field value for a node.

    Priority:
      1) Latest telemetry tables (device → power → environment → air_quality → local_stats → health → host)
      2) NodeInfo table (e.g., hw_model, long_name, short_name, role, is_licensed, public_key, is_unmessagable,
         last_heard, hops_away, snr, macaddr). Also supports common aliases like 'hardware_model' → 'hw_model'.

    Returns a single scalar value or None.
    """
    # Resolve identifier → single node_num (first match wins)
    nums = _resolve_to_list(identifier, owner_node_num=owner_node_num, db_path=db_path)
    if not nums:
        return None
    num = nums[0]

    # 1) Try telemetry first
    telem = _latest_telem_dicts(owner_node_num, num, db_path)
    if telem:
        for subtype in (
            "device",
            "power",
            "environment",
            "air_quality",
            "local_stats",
            "health",
            "host",
        ):
            row = telem.get(subtype)
            if not row:
                continue
            if metric in row:
                return row.get(metric)

    # 2) Fallback to NodeInfo table
    # Normalize metric name and handle common aliases
    aliases = {
        "hardware_model": "hw_model",
        "longName": "long_name",
        "shortName": "short_name",
        "isLicensed": "is_licensed",
        "isUnmessagable": "is_unmessagable",
        "lastHeard": "last_heard",
        "hopsAway": "hops_away",
    }
    col = aliases.get(metric, metric)

    # Special-case synthetic IDs
    if col in ("id", "node_id"):
        try:
            return decimal_to_hex(num)
        except Exception:
            return None

    # Only allow known safe NodeDB columns
    allowed_nodedb_cols = {
        "long_name",
        "short_name",
        "macaddr",
        "hw_model",
        "role",
        "is_licensed",
        "public_key",
        "is_unmessagable",
        "last_heard",
        "hops_away",
        "snr",
    }
    if col not in allowed_nodedb_cols:
        return None

    ndb = NodeDB(owner_node_num, db_path)
    ndb.ensure_table()
    try:
        with ndb.connect() as con:
            cur = con.cursor()
            cur.execute(f"SELECT {col} FROM {ndb.table} WHERE node_num = ? LIMIT 1", (num,))
            row = cur.fetchone()
            if row is None:
                return None
            return row[0]
    except Exception:
        return None
