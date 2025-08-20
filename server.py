import os

import uuid
import datetime
from datetime import timezone, timedelta
import logging
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple, Union

from dotenv import load_dotenv
import pyodbc 
from azure.kusto.data import (
    KustoClient,
    KustoConnectionStringBuilder,
    ClientRequestProperties,
)
from azure.kusto.data.response import KustoResponseDataSet

from mcp.server.fastmcp import FastMCP
from mcp.server.sse import SseServerTransport
from rapidfuzz import fuzz
from starlette.applications import Starlette
from starlette.routing import Route, Mount
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import re
from pathlib import Path

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mcp-server")

def _dbg(msg: str):
    """Mirror logs to stdout for easy n8n/Azure log viewing."""
    print(msg, flush=True)
    logger.info(msg)


KCSB = KustoConnectionStringBuilder.with_aad_application_key_authentication(
    os.environ["KUSTO_SERVICE_URI"],
    os.environ["AZURE_CLIENT_ID"],
    os.environ["AZURE_CLIENT_SECRET"],
    os.environ["AZURE_TENANT_ID"],
)
kusto = KustoClient(KCSB)
DB = os.environ.get("KUSTO_DATABASE", "")

# ISPPLANTDATA schema so the tool list_table_schema is not required 
ISP_TABLE   = "ISPPLANTDATA"
ISP_TAG_COL = "TagName"
ISP_VAL_COL = "Value"
ISP_TS_COL  = "Timestamp"


WEATHER_SERVICE_URI = os.environ.get("KUSTO_WEATHER_SERVICE_URI", "")
if not WEATHER_SERVICE_URI:
    _dbg("ERROR: Missing KUSTO_WEATHER_SERVICE_URI")
    raise ValueError("Missing Kusto weather cluster URI (set KUSTO_WEATHER_SERVICE_URI).")
if not WEATHER_SERVICE_URI.startswith("https://"):
    WEATHER_SERVICE_URI = "https://" + WEATHER_SERVICE_URI

WEATHER_KCSB = KustoConnectionStringBuilder.with_aad_application_key_authentication(
    WEATHER_SERVICE_URI,
    os.environ.get("AZURE_CLIENT_ID", ""),
    os.environ.get("AZURE_CLIENT_SECRET", ""),
    os.environ.get("AZURE_TENANT_ID", ""),
)
weather_client = KustoClient(WEATHER_KCSB)
WEATHER_DB = os.environ.get("WEATHER_DB", "")
if not WEATHER_DB:
    _dbg("ERROR: Missing weather database name (set WEATHER_DB).")
    raise ValueError("Missing Kusto weather database name.")


mcp = FastMCP(name="fabric-eventhouse-mcp", description="MCP server for Fabric Eventhouse tools")


def format_results(result_set: Optional[KustoResponseDataSet]) -> List[Dict[str, Any]]:
    if not result_set or not getattr(result_set, "primary_results", None):
        return []
    first = result_set.primary_results[0]
    cols = [col.column_name for col in first.columns]
    return [dict(zip(cols, row)) for row in first.rows]

def execute_kusto(query: str, client: KustoClient, database: str, readonly: bool = True) -> List[Dict[str, Any]]:
    crp = ClientRequestProperties()
    crp.application = "fabric-eventhouse-mcp"
    crp.client_request_id = f"MCP:{uuid.uuid4()}"
    if readonly:
        crp.set_option("request_readonly", True)
    q_preview = (query or "")[:500].replace("\n", " ")
    _dbg(f"[execute_kusto] DB={database}  Query(<=500ch): {q_preview}")
    try:
        response = client.execute(database, query, crp)
        rows = format_results(response)
        _dbg(f"[execute_kusto] Returned rows: {len(rows)}")
        return rows
    except Exception as e:
        _dbg(f"[execute_kusto] ERROR: {e}")
        raise

def _sql_conn():
    driver = '{ODBC Driver 17 for SQL Server}'
    server = os.getenv("SQL_SERVER_HOST", "")
    database = os.getenv("SQL_SERVER_DB", "")
    client_id = os.getenv("AZURE_CLIENT_ID", "")
    client_secret = os.getenv("AZURE_CLIENT_SECRET", "")
    _dbg(f"[SQL] Connecting to {server} / DB={database} (AAD SPN)")
    conn_str = (
        f"Driver={driver};Server={server},1433;Database={database};"
        "Encrypt=yes;TrustServerCertificate=no;"
        "Authentication=ActiveDirectoryServicePrincipal;"
        f"UID={client_id};PWD={client_secret}"
    )
    return pyodbc.connect(conn_str)

def _parse_iso_dt(s: Optional[str], default_dt: datetime.datetime) -> datetime.datetime:
    if not s:
        return default_dt
    try:
        s2 = s.strip()
        if s2.endswith("Z"):
            s2 = s2[:-1] + "+00:00"
        dt = datetime.datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception as e:
        raise ValueError(f"Invalid ISO time '{s}': {e}")

def _utc_str(dt: datetime.datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _escape_sql_literal(s: str) -> str:
    return s.replace("'", "''")

_escape_kql_literal = _escape_sql_literal

PERF_WORDS = re.compile(r"\b(analy[sz]e|analysis|evaluate|evaluation|trend|trends|impact|performance|health|efficien|stability|overview)\b", re.I)

def _parse_relative_window(relative: Optional[str], now: Optional[datetime.datetime] = None) -> Optional[Tuple[datetime.datetime, datetime.datetime]]:
    """
      'last 7 days', 'last 1 week', 'past 24 hours', '7d', '24h', '1w', 'last week'
    Returns (start_dt_utc, end_dt_utc) or None if not parsed.
    """
    if not relative:
        return None
    text = relative.strip().lower()
    if not text:
        return None
    if now is None:
        now = datetime.datetime.now(timezone.utc)
    end_dt = now

    # direct forms: 7d, 24h, 1w
    m = re.fullmatch(r"(\d+)\s*([dhw])", text)
    if m:
        n = int(m.group(1))
        unit = m.group(2)
        if unit == "h":
            return (end_dt - timedelta(hours=n), end_dt)
        if unit == "d":
            return (end_dt - timedelta(days=n), end_dt)
        if unit == "w":
            return (end_dt - timedelta(days=7*n), end_dt)

    # phrases: last/past N days/weeks/hours
    m = re.search(r"(last|past)\s+(\d+)\s*(day|days|d|week|weeks|w|hour|hours|h)", text)
    if m:
        n = int(m.group(2))
        unit = m.group(3)[0]
        if unit == "h":
            return (end_dt - timedelta(hours=n), end_dt)
        if unit == "d":
            return (end_dt - timedelta(days=n), end_dt)
        if unit == "w":
            return (end_dt - timedelta(days=7*n), end_dt)
    m = re.search(r"(last|past)\s+(\d+)\s*(month|months|mo)", text)
    if m:
        n = int(m.group(2))
        return (end_dt - timedelta(days=30*n), end_dt)
    # "last week" -> 7 days
    if text in ("last week", "past week"):
        return (end_dt - timedelta(days=7), end_dt)

    # "last 24 hours"
    if text in ("last 24 hours", "past 24 hours"):
        return (end_dt - timedelta(hours=24), end_dt)
    # "last month" -> 30 days
    if text in ("last month", "past month"):
        return (end_dt - timedelta(days=30), end_dt)

    return None

def _normalize_period(period: Optional[str]) -> Optional[str]:
    """
    Normalize human-friendly period to Kusto timespan literal.
    Returns None if not set.
    """
    if not period:
        return None
    t = str(period).strip().lower()

    if t in ("hour", "hourly", "per hour"):  return "1h"
    if t in ("day", "daily", "per day"):     return "1d"
    if t in ("week", "weekly", "per week"):  return "7d"
    if t in ("month", "monthly", "per month"):  return "30d"

    # 1w -> 7d
    m = re.fullmatch(r"(\d+)\s*w", t)
    if m:
        return f"{int(m.group(1)) * 7}d"

    # Accept raw Kusto-like spans: 1h, 2h, 1d, 3d, 12h, etc.
    if re.fullmatch(r"\d+\s*[smhd]", t):
        return t.replace(" ", "")

    return t  # pass-through (let Kusto complain if invalid)

LONG_WINDOW_THRESHOLD_DAYS = 15
MIN_BIN_FOR_LONG_WINDOWS = "6h"

def _span_to_timedelta(span: Optional[str]) -> Optional[datetime.timedelta]:
    """
    Convert a Kusto-like timespan string ('30m','1h','12h','1d','7d') to timedelta.
    Returns None if span is None or unparseable.
    """
    if not span:
        return None
    s = span.strip().lower()
    m = re.fullmatch(r"(\d+)\s*([smhd])", s)
    if not m:
        return None
    n = int(m.group(1))
    unit = m.group(2)
    if unit == "s": return datetime.timedelta(seconds=n)
    if unit == "m": return datetime.timedelta(minutes=n)
    if unit == "h": return datetime.timedelta(hours=n)
    if unit == "d": return datetime.timedelta(days=n)
    return None

def _coerce_min_bin_for_window(span: Optional[str],
                               start_dt: datetime.datetime,
                               end_dt: datetime.datetime,
                               threshold_days: int = LONG_WINDOW_THRESHOLD_DAYS,
                               min_span: str = MIN_BIN_FOR_LONG_WINDOWS) -> Optional[str]:
    """
    If window length > threshold_days and span < min_span (or missing), coerce to min_span.
    Applied ONLY to ISPPLANTDATA.
    """
    window_len = end_dt - start_dt
    if window_len > datetime.timedelta(days=threshold_days):
        min_td = _span_to_timedelta(min_span)
        curr_td = _span_to_timedelta(span)
        if curr_td is None or (min_td is not None and curr_td < min_td):
            return min_span
    return span

# Fixed EPF JT tag set (in-memory)
FIXED_TAGS: Dict[str, str] = {
    "400JTPIT4031/PV.CV": "EPF JT INLET EXCHANGER GAS PRESSURE (E-403A/B)",
    "400JTPIT4050/PV.CV": "EPF JT COLD SEPARATOR PRESSURE",
    "400JTTIT4031/PV.CV": "EPF JT INLET EXCHANGER GAS TEMPERATURE (E-403A/B)",
    "400JTTIT4030/PV.CV": "EPF JT OUTLET EXCHANGER GAS TEMPERATURE (E-403A/B)",
    "400JTPIT4010/PV.CV": "EPF JT SKID INLET PRESSURE",
    "400JTTIT4010/PV.CV": "EPF JT INLET GAS TEMPERATURE (E-401A/B/C)",
    "400JTTIT4012/PV.CV": "EPF JT CONDITIONNED GAS TEMPERATURE",
    "400JTPIT4030/PV.CV": "EPF JT E-402 DISCHARGE PRESSURE",
    "400JTPIT4032/PV.CV": "EPF JT OUTLET EXCHANGER GAS PRESSURE (E-403A/B)",
    "400JTTIT4032/PV.CV": "EPF JT OUTLET EXCHANGER GAS TEMPERATURE (E-403A/B)",
    "400JTTIT4050/PV.CV": "EPF JT COLD SEPARATOR TEMPERATURE",
    "400JTPIT4011/PV.CV": "EPF JT CONDITIONNED GAS PRESSURE",
    "400JTTIT4011/PV.CV": "EPF JT E-401/A/B/C OUTLET TEMPERATURE",
    "400FI401/PV.CV":     "GAS TO EPF COLD EXCHANGER",
    "400PIC010/PV.CV":    "EPF COMP SUCTION TO FLARE",
}

def _fixed_search(query: str, threshold: int = 60) -> List[Tuple[str, str]]:
    q = (query or "").strip().lower()
    if not q:
        return []
    results: List[Tuple[str, str, int]] = []
    for tag, desc in FIXED_TAGS.items():
        score_desc = fuzz.partial_ratio(q, desc.lower())
        score_tag  = fuzz.partial_ratio(q, tag.lower())
        score = max(score_desc, score_tag)
        if score >= threshold:
            results.append((tag, desc, score))
    results.sort(key=lambda x: x[2], reverse=True)
    hits = [(tag, desc) for tag, desc, _ in results]
    _dbg(f"[resolver] Query='{query}'  Threshold={threshold}  Matches={len(hits)}")
    for t, d in hits:
        _dbg(f"[resolver]  -> {t} :: {d}")
    return hits

def _resolve_tags_for_query(query: str, default_all_if_perf: bool = True) -> List[str]:
    """If query is generic 'performance/analysis/trend/etc', return ALL tags; else fuzzy resolve."""
    q = (query or "").strip()
    if default_all_if_perf and PERF_WORDS.search(q):
        tags = list(FIXED_TAGS.keys())
        _dbg(f"[resolve_tags] Performance-style query detected -> ALL tags ({len(tags)})")
        return tags
    # try fuzzy matches first
    pairs = _fixed_search(q)
    tags = [t for t, _ in pairs]
    if not tags and "/" in q:
        tags = [q]  
    if not tags and default_all_if_perf:
        # if vague short ask like "performance?" — return all
        if len(q.split()) <= 2:
            tags = list(FIXED_TAGS.keys())
            _dbg(f"[resolve_tags] Very vague query -> ALL tags ({len(tags)})")
    tags = sorted(set(tags))
    _dbg(f"[resolve_tags] Resolved={tags}")
    return tags
def _get_weather_for_window(
    start_time: Optional[str], 
    end_time: Optional[str], 
    relative: Optional[str],
    period: Optional[str] = None
) -> List[Dict[str, Any]]:
    # If user didn't specify period, default to 30m
    bin_period = period or "30m"
    return plant_weather(
        start_time=start_time,
        end_time=end_time,
        agg="avg",
        period=bin_period,
        weather_database=WEATHER_DB
    )


# pid.txt context loader
_PID_CACHE: Dict[str, Any] = {"text": None, "mtime": None, "path": None}

def _load_pid_text() -> str:
    path = Path(__file__).parent / "pid.txt"
    try:
        stat = path.stat()
    except Exception:
        _dbg("[pid] pid.txt not found next to server.py")
        _PID_CACHE.update({"text": "", "mtime": None, "path": str(path)})
        return ""
    mtime = stat.st_mtime
    if _PID_CACHE["text"] is None or _PID_CACHE["mtime"] != mtime:
        try:
            txt = path.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            _dbg(f"[pid] Error reading pid.txt: {e}")
            txt = ""
        _PID_CACHE.update({"text": txt, "mtime": mtime, "path": str(path)})
        _dbg(f"[pid] Loaded pid.txt from {path} ({len(txt)} chars)")
    return _PID_CACHE["text"] or ""




# Time tool (in case it is required by the mcp to get the current time with zone)
@mcp.tool()
def TimeTool(input_timezone: Optional[str] = None) -> str:
    fmt = "%Y-%m-%d %H:%M:%S %Z%z"
    now = datetime.datetime.now()
    if input_timezone:
        try:
            now = now.astimezone(ZoneInfo(input_timezone))
        except Exception as e:
            _dbg(f"[TimeTool] Invalid timezone '{input_timezone}': {e}")
            return f"Error: '{input_timezone}' is not a valid timezone."
    out = f"The current time is {now.strftime(fmt)}."
    _dbg(f"[TimeTool] {out}")
    return out


# Generic Kusto query (read-only)
@mcp.tool(description="Run a read-only KQL query on the specified database ISPPLANTDATA and return the results.")
def kusto_query(query: str, database: str = DB) -> List[Dict[str, Any]]:
    _dbg(f"[kusto_query] DB={database}")
    return execute_kusto(query=query, client=kusto, database=database)


# ISPPLANTDATA tools (with relative window + fallback)
@mcp.tool(description=f"Return the full JSON schema for the {ISP_TABLE} table in the database.")
def isp_table_schema(database: str = DB) -> str:
    rows = execute_kusto(
        query=f".show table {ISP_TABLE} cslschema",
        client=kusto,
        database=database
    )
    _dbg(f"[isp_table_schema] Schema rows={len(rows)}")
    return rows[0]["Schema"] if rows else ""

def _build_tokens_for_fallback(tags: List[str]) -> List[str]:
    tokens: List[str] = []
    for t in tags:
        t = t.strip()
        if not t:
            continue
        tokens.append(t)
        if "/" in t:
            base = t.split("/", 1)[0].strip()
            if base:
                tokens.append(base)
    seen = set()
    out = []
    for x in tokens:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out

@mcp.tool(description=f"Fetch raw rows from {ISP_TABLE} for the provided comma-separated tag list, with optional time window or limit. You can also pass relative='last 7 days' etc.")
def isp_get_tags_data(
    tag_names: str,
    start_time: Optional[str] = None,
    end_time:   Optional[str] = None,
    limit:      Optional[int] = None,
    database:   str = DB,
    relative:   Optional[str] = None,   # NEW
) -> List[Dict[str, Any]]:
    tags = [t.strip() for t in (tag_names or "").split(",") if t.strip()]
    _dbg(f"[isp_get_tags_data] Input tags={tags}  limit={limit}  DB={database}  relative={relative}")
    if not tags:
        return []

    tags_array = "[" + ",".join(f"'{_escape_kql_literal(t)}'" for t in tags) + "]"

    now = datetime.datetime.now(timezone.utc)

    # Relative window takes precedence if provided
    if relative:
        win = _parse_relative_window(relative, now)
        if win:
            start_time = _utc_str(win[0])
            end_time   = _utc_str(win[1])
            _dbg(f"[isp_get_tags_data] Parsed relative='{relative}' -> {start_time} .. {end_time}")

    if limit:
        kql = (
            f"{ISP_TABLE} "
            f"| where {ISP_TAG_COL} in (dynamic({tags_array})) "
            f"| sort by {ISP_TS_COL} desc "
            f"| take {int(limit)} "
            f"| project {ISP_TS_COL}, {ISP_TAG_COL}, {ISP_VAL_COL}"
        )
        rows = execute_kusto(query=kql, client=kusto, database=database)
        if len(rows) == 0:
            _dbg("[isp_get_tags_data] No rows (exact IN). Trying fallback: has_any/contains with tokens...")
            tokens = _build_tokens_for_fallback(tags)
            tokens_array = "[" + ",".join(f"'{_escape_kql_literal(t)}'" for t in tokens) + "]"
            kql_fb = (
                f"{ISP_TABLE} "
                f"| sort by {ISP_TS_COL} desc "
                f"| where {ISP_TAG_COL} has_any (dynamic({tokens_array})) "
                f"| take {int(limit)} "
                f"| project {ISP_TS_COL}, {ISP_TAG_COL}, {ISP_VAL_COL}"
            )
            rows = execute_kusto(query=kql_fb, client=kusto, database=database)
        _dbg(f"[isp_get_tags_data] Rows returned={len(rows)}")
        return rows

    try:
        end_dt   = _parse_iso_dt(end_time, now)
        start_dt = _parse_iso_dt(start_time, end_dt - timedelta(minutes=15))
    except ValueError as e:
        _dbg(f"[isp_get_tags_data] Time parsing error: {e}")
        return [{"error": str(e)}]

    if start_dt > end_dt:
        _dbg("[isp_get_tags_data] start_dt > end_dt; swapping.")
        start_dt, end_dt = end_dt, start_dt

    start_str = _utc_str(start_dt)
    end_str   = _utc_str(end_dt)
    _dbg(f"[isp_get_tags_data] Window UTC: {start_str} .. {end_str}")

    kql = (
        f"{ISP_TABLE} "
        f"| where {ISP_TS_COL} between (datetime({start_str})..datetime({end_str})) "
        f"| where {ISP_TAG_COL} in (dynamic({tags_array})) "
        f"| project {ISP_TS_COL}, {ISP_TAG_COL}, {ISP_VAL_COL} "
        f"| order by {ISP_TS_COL} asc"
    )
    rows = execute_kusto(query=kql, client=kusto, database=database)
    if len(rows) == 0:
        _dbg("[isp_get_tags_data] No rows (exact IN). Trying fallback: has_any/contains with tokens...")
        tokens = _build_tokens_for_fallback(tags)
        tokens_array = "[" + ",".join(f"'{_escape_kql_literal(t)}'" for t in tokens) + "]"
        kql_fb = (
            f"{ISP_TABLE} "
            f"| where {ISP_TS_COL} between (datetime({start_str})..datetime({end_str})) "
            f"| where {ISP_TAG_COL} has_any (dynamic({tokens_array})) "
            f"| project {ISP_TS_COL}, {ISP_TAG_COL}, {ISP_VAL_COL} "
            f"| order by {ISP_TS_COL} asc"
        )
        rows = execute_kusto(query=kql_fb, client=kusto, database=database)

        if len(rows) == 0:
            ors = " or ".join(f'{ISP_TAG_COL} contains "{_escape_kql_literal(t)}"' for t in tokens)
            kql_fb2 = (
                f"{ISP_TABLE} "
                f"| where {ISP_TS_COL} between (datetime({start_str})..datetime({end_str})) "
                f"| where {ors} "
                f"| project {ISP_TS_COL}, {ISP_TAG_COL}, {ISP_VAL_COL} "
                f"| order by {ISP_TS_COL} asc"
            )
            rows = execute_kusto(query=kql_fb2, client=kusto, database=database)

    _dbg(f"[isp_get_tags_data] Rows returned={len(rows)}")
    return rows

@mcp.tool(description=f"Summarize {ISP_TABLE} over time for tags. Supports agg in (avg,min,max,sum,count) and optional bin period like 1h/1d. You can pass relative='last 7 days' etc.")
def isp_stats(
    tag_names: str,
    agg: str = "avg",
    period: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    database: str = DB,
    relative: Optional[str] = None,   # NEW
) -> List[Dict[str, Any]]:
    tags = [t.strip() for t in (tag_names or "").split(",") if t.strip()]
    _dbg(f"[isp_stats] tags={tags} agg={agg} period={period} DB={database} relative={relative}")
    if not tags:
        return [{"error": "No tags provided"}]

    agg_l = agg.lower().strip()
    if agg_l not in ("avg", "min", "max", "sum", "count"):
        return [{"error": f"Unsupported agg '{agg}'. Use avg,min,max,sum,count."}]

    now = datetime.datetime.now(timezone.utc)

    if relative:
        win = _parse_relative_window(relative, now)
        if win:
            start_time = _utc_str(win[0])
            end_time   = _utc_str(win[1])
            _dbg(f"[isp_stats] Parsed relative='{relative}' -> {start_time} .. {end_time}")

    try:
        end_dt   = _parse_iso_dt(end_time, now)
        start_dt = _parse_iso_dt(start_time, end_dt - timedelta(minutes=15))
    except ValueError as e:
        return [{"error": str(e)}]

    if start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt

    start_str = _utc_str(start_dt)
    end_str   = _utc_str(end_dt)
    _dbg(f"[isp_stats] Window UTC: {start_str} .. {end_str}")

    span = _normalize_period(period)
    span_before = span
    span = _coerce_min_bin_for_window(span, start_dt, end_dt)  # NEw (ISPPLANTDATA only)not weather
    _dbg(f"[isp_stats] Bin policy: requested={span_before!r} -> used={span!r}; window_days={(end_dt-start_dt).total_seconds()/86400:.2f}")
    tags_array = "[" + ",".join(f"'{_escape_kql_literal(t)}'" for t in tags) + "]"

    if span:
        kql = (
            f"{ISP_TABLE} "
            f"| where {ISP_TS_COL} between (datetime({start_str})..datetime({end_str})) "
            f"| where {ISP_TAG_COL} in (dynamic({tags_array})) "
            f"| summarize {agg_l}_val = {agg_l}({ISP_VAL_COL}) by {ISP_TAG_COL}, bin({ISP_TS_COL}, {span}) "
            f"| order by {ISP_TS_COL} asc"
        )
    else:
        kql = (
            f"{ISP_TABLE} "
            f"| where {ISP_TS_COL} between (datetime({start_str})..datetime({end_str})) "
            f"| where {ISP_TAG_COL} in (dynamic({tags_array})) "
            f"| summarize {agg_l}_val = {agg_l}({ISP_VAL_COL}) by {ISP_TAG_COL}"
        )
    rows = execute_kusto(query=kql, client=kusto, database=database)
    if len(rows) == 0:
        _dbg("[isp_stats] No rows with exact IN. Trying fallback has_any...")
        tokens = _build_tokens_for_fallback(tags)
        tokens_array = "[" + ",".join(f"'{_escape_kql_literal(t)}'" for t in tokens) + "]"
        if span:
            kql_fb = (
                f"{ISP_TABLE} "
                f"| where {ISP_TS_COL} between (datetime({start_str})..datetime({end_str})) "
                f"| where {ISP_TAG_COL} has_any (dynamic({tokens_array})) "
                f"| summarize {agg_l}_val = {agg_l}({ISP_VAL_COL}) by {ISP_TAG_COL}, bin({ISP_TS_COL}, {span}) "
                f"| order by {ISP_TS_COL} asc"
            )
        else:
            kql_fb = (
                f"{ISP_TABLE} "
                f"| where {ISP_TS_COL} between (datetime({start_str})..datetime({end_str})) "
                f"| where {ISP_TAG_COL} has_any (dynamic({tokens_array})) "
                f"| summarize {agg_l}_val = {agg_l}({ISP_VAL_COL}) by {ISP_TAG_COL}"
            )
        rows = execute_kusto(query=kql_fb, client=kusto, database=database)

    _dbg(f"[isp_stats] Rows returned={len(rows)}")
    return rows


# Fixed tag tools
@mcp.tool(description="List the fixed EPF JT tags (in-memory) with descriptions.")
def fixed_tags_all() -> List[Dict[str, str]]:
    out = [{"tag": t, "desc": d} for t, d in FIXED_TAGS.items()]
    _dbg(f"[fixed_tags_all] Count={len(out)}")
    return out

@mcp.tool(description="Look up the fixed EPF JT tags by tag/prefix or by keywords like 'temperature' or 'pressure'. Returns matching tags with descriptions.")
def fixed_tags_lookup(query: str) -> List[Dict[str, str]]:
    _dbg(f"[fixed_tags_lookup] query='{query}'")
    pairs = _fixed_search(query)
    out = [{"tag": t, "desc": d} for t, d in pairs]
    _dbg(f"[fixed_tags_lookup] Matches={len(out)}")
    return out

@mcp.tool(description="Get description for a specific tag from the fixed set only. Returns {'tag','desc','source'='fixed'|'not_found'}.")
def fixed_tag_describe(tag_name: str) -> Dict[str, Any]:
    tag = (tag_name or "").strip()
    _dbg(f"[fixed_tag_describe] tag_name='{tag}'")
    if not tag:
        return {"error": "tag_name is required"}
    if tag in FIXED_TAGS:
        out = {"tag": tag, "desc": FIXED_TAGS[tag], "source": "fixed"}
        _dbg(f"[fixed_tag_describe] HIT fixed -> {out}")
        return out
    _dbg("[fixed_tag_describe] NOT FOUND")
    return {"tag": tag, "desc": None, "source": "not_found"}


# P&ID context tool (reads local pid.txt)

@mcp.tool(description="Fetch P&ID context from local pid.txt. Returns full file and automatically fetches weather for the same period if available.")
def pid_context(tags: Optional[str] = None,
                start_time: Optional[str] = None,
                end_time: Optional[str] = None,
                relative: Optional[str] = None,
                period: Optional[str] = None) -> Dict[str, Any]:
    _dbg(f"[pid_context] tags={tags} start={start_time} end={end_time} relative={relative} period={period}")
    txt = _load_pid_text()
    if not txt:
        return {
            "source": _PID_CACHE["path"],
            "matched": [],
            "snippet": "",
            "note": "pid.txt missing or empty",
            "weather": []
        }

    try:
        weather_data = _get_weather_for_window(start_time, end_time, relative, period=period)
    except Exception as e:
        _dbg(f"[pid_context] Weather fetch error: {e}")
        weather_data = []

    return {
        "source": _PID_CACHE["path"],
        "matched": [],
        "snippet": txt,
        "note": "Full file returned",
        "weather": weather_data
    }




# One-shot context query (resolve → values → P&ID from pid.txt)
@mcp.tool(description="Resolve tags from free-text (or default to ALL tags for 'performance/analysis/trend' asks), then fetch ISPPLANTDATA values. Use limit for latest N; otherwise supply a time window or relative='last 7 days'. Returns values + in-memory tag desc + local pid.txt context.")
def context_values_by_query(
    query: str,
    start_time: Optional[str] = None,
    end_time:   Optional[str] = None,
    limit:      Optional[int] = None,
    database:   str = DB,
    relative:   Optional[str] = None,
    period: Optional[str] = None   # NEW
) -> Dict[str, Any]:
    _dbg(f"[context_values_by_query] query='{query}' limit={limit} start={start_time} end={end_time} DB={database} relative={relative} period={period}")
    tags = _resolve_tags_for_query(query, default_all_if_perf=True)
    _dbg(f"[context_values_by_query] tags_resolved={tags}")
    if not tags:
        return {"tags_resolved": [], "note": "No tags matched your query."}

    tags_csv = ",".join(tags)

    # Fetch values (limit or window with relative support)
    if limit:
        values = isp_get_tags_data(tag_names=tags_csv, limit=limit, database=database)
    else:
        values = isp_get_tags_data(tag_names=tags_csv, start_time=start_time, end_time=end_time, database=database, relative=relative)
    _dbg(f"[context_values_by_query] values_rows={len(values)}")

    # In-memory descriptions & local P&ID context
    inmem = [{"tag": t, "desc": FIXED_TAGS.get(t)} for t in tags]
    pid = pid_context(start_time=start_time, end_time=end_time, relative=relative, period=period)
    # pid = pid_context(start_time=start_time, end_time=end_time, relative=relative, period=None)
    
    # Attach weather if this is a performance/trend/analysis ask
    weather_data = pid.get("weather", [])
    if PERF_WORDS.search(query or ""):
        try:
            weather_data = _get_weather_for_window(start_time, end_time, relative, period=period)
        except Exception as e:
            _dbg(f"[context_values_by_query] Weather fetch error: {e}")

    out = {
        "tags_resolved": tags,
        "tag_context_in_memory": inmem,
        "values": values,
        "pid_context": pid,
        "weather": weather_data
    }
    return out

# Aggregate (defaults to hourly bins if not specified) + P&ID context
# @mcp.tool(description="Resolve tags from free-text (or default to ALL tags for 'performance/analysis/trend' asks), then compute aggregates over ISPPLANTDATA. agg in (avg,min,max,sum,count). period defaults to '1h' if not provided. You may pass relative='last 7 days'. Returns results + in-memory tag desc + local pid.txt context.")
# def isp_aggregate_for_query(
#     query: Optional[str] = None,
#     tag_names: Optional[str] = None,
#     agg: str = "avg",
#     period: Optional[str] = None,
#     start_time: Optional[str] = None,
#     end_time: Optional[str] = None,
#     database: str = DB,
#     relative: Optional[str] = None,     # NEW
# ) -> Dict[str, Any]:
#     _dbg(f"[isp_aggregate_for_query] query='{query}' tag_names='{tag_names}' agg={agg} period={period} start={start_time} end={end_time} DB={database} relative={relative}")

#     if tag_names:
#         tags = [t.strip() for t in (tag_names or "").split(",") if t.strip()]
#     else:
#         tags = _resolve_tags_for_query(query or "", default_all_if_perf=True)

#     _dbg(f"[isp_aggregate_for_query] tags_resolved={tags}")
#     if not tags:
#         return {"tags_resolved": [], "note": "No tags found to aggregate."}

#     # Default resolution: hourly if not specified
#     span = _normalize_period(period) or "1h"

#     rows = isp_stats(
#         tag_names=",".join(tags),
#         agg=agg,
#         period=span,
#         start_time=start_time,
#         end_time=end_time,
#         database=database,
#         relative=relative,
#     )

#     inmem = [{"tag": t, "desc": FIXED_TAGS.get(t)} for t in tags]
#     pid = pid_context(start_time=start_time, end_time=end_time, relative=relative, period=span)

#     out: Dict[str, Any] = {
#         "tags_resolved": tags,
#         "aggregate": agg,
#         "period": span,
#         "results": rows,
#         "time_window": {"start_time": start_time, "end_time": end_time, "relative": relative},
#         "tag_context_in_memory": inmem,
#         "pid_context": pid,
#     }

#     _dbg(f"[isp_aggregate_for_query] results_rows={len(rows)}")
#     return out
@mcp.tool(description="Resolve tags from free-text (or default to ALL tags for 'performance/analysis/trend' asks), then compute aggregates over ISPPLANTDATA. agg in (avg,min,max,sum,count). period defaults to '1h' if not provided. You may pass relative='last 7 days'. Returns results + in-memory tag desc + local pid.txt context.")
def isp_aggregate_for_query(
    query: Optional[str] = None,
    tag_names: Optional[str] = None,
    agg: str = "avg",
    period: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    database: str = DB,
    relative: Optional[str] = None,    
) -> Dict[str, Any]:
    _dbg(f"[isp_aggregate_for_query] query='{query}' tag_names='{tag_names}' agg={agg} period={period} start={start_time} end={end_time} DB={database} relative={relative}")

    if tag_names:
        tags = [t.strip() for t in (tag_names or "").split(",") if t.strip()]
    else:
        tags = _resolve_tags_for_query(query or "", default_all_if_perf=True)

    _dbg(f"[isp_aggregate_for_query] tags_resolved={tags}")
    if not tags:
        return {"tags_resolved": [], "note": "No tags found to aggregate."}

    # Default resolution: hourly if not specified
    span = _normalize_period(period) or "1h"

    # --- Derive the actual time window here (same logic as isp_stats) ---
    now = datetime.datetime.now(timezone.utc)
    if relative:
        win = _parse_relative_window(relative, now)
        if win:
            start_dt, end_dt = win
        else:
            end_dt = now
            start_dt = end_dt - datetime.timedelta(minutes=15)
    else:
        try:
            end_dt   = _parse_iso_dt(end_time, now)
            start_dt = _parse_iso_dt(start_time, end_dt - datetime.timedelta(minutes=15))
        except ValueError as e:
            return {"tags_resolved": tags, "error": str(e)}

    if start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt

    # --- Enforce bin policy for long windows (ISPPLANTDATA only) ---
    span_before = span
    span = _coerce_min_bin_for_window(span, start_dt, end_dt)
    window_days = (end_dt - start_dt).total_seconds() / 86400.0
    coerced = (span_before != span)
    coercion_note = None
    if coerced:                      # NEW
        coercion_note = (            # NEW
            f"Requested period {span_before or 'None'} coerced to {span} "
            f"because window {window_days:.2f} days exceeds {LONG_WINDOW_THRESHOLD_DAYS}-day minimum-bin policy."
        )
    _dbg(f"[isp_aggregate_for_query] Bin policy: requested={span_before!r} -> used={span!r}; window_days={window_days:.2f}")
    # _dbg(f"[isp_aggregate_for_query] Bin policy: requested={span_before!r} -> used={span!r}; window_days={(end_dt-start_dt).total_seconds()/86400:.2f}")

    # Use the coerced span everywhere downstream for consistency (plant data)
    rows = isp_stats(
        tag_names=",".join(tags),
        agg=agg,
        period=span,
        start_time=_utc_str(start_dt),
        end_time=_utc_str(end_dt),
        database=database,
        relative=None,  # already resolved
    )

    inmem = [{"tag": t, "desc": FIXED_TAGS.get(t)} for t in tags]
    # IMPORTANT: keep weather independent -> do NOT pass the plant period here
    pid = pid_context(start_time=_utc_str(start_dt), end_time=_utc_str(end_dt), relative=None, period=None)

    out: Dict[str, Any] = {
        "tags_resolved": tags,
        "aggregate": agg,
        "period": span,  # report the actual bin used
        "results": rows,
        "time_window": {
            "start_time": _utc_str(start_dt),
            "end_time": _utc_str(end_dt),
            "relative": relative,
        },
        "tag_context_in_memory": inmem,
        "pid_context": pid,
        "bin_policy": {
            "requested": span_before,
            "used": span,
            "coerced": coerced,
            "window_days": window_days,
            "threshold_days": LONG_WINDOW_THRESHOLD_DAYS
        },
        "coercion_note": coercion_note,  # string or None
    }

    _dbg(f"[isp_aggregate_for_query] results_rows={len(rows)}")
    return out

# Weather tool
@mcp.tool(
    description="Fetch plant weather data from KM400WeatherDataTBL. Supports raw rows, top-N latest, or aggregated (avg/min/max/sum/count) over a time window with optional binning."
)
def plant_weather(
    start_time: Optional[str] = None,
    end_time:   Optional[str] = None,
    agg:        Optional[str] = None,
    period:     Optional[str] = None,
    limit:      Optional[int] = None,
    weather_database: str = WEATHER_DB
) -> List[Dict[str, Any]]:
    _dbg(f"[plant_weather] agg={agg} period={period} limit={limit} start={start_time} end={end_time} DB={weather_database}")
    now = datetime.datetime.now(timezone.utc)

    if limit:
        kql = (
            "KM400WeatherDataTBL "
            "| sort by Timestamp desc "
            f"| take {int(limit)}"
        )
        rows = execute_kusto(query=kql, client=weather_client, database=weather_database)
        _dbg(f"[plant_weather] rows={len(rows)} (latest mode)")
        return rows

    try:
        end_dt   = _parse_iso_dt(end_time, now)
        start_dt = _parse_iso_dt(start_time, end_dt - timedelta(minutes=15))
    except ValueError as e:
        _dbg(f"[plant_weather] Time parsing error: {e}")
        return [{"error": str(e)}]

    if start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt

    start_str = _utc_str(start_dt)
    end_str   = _utc_str(end_dt)
    _dbg(f"[plant_weather] Window UTC: {start_str} .. {end_str}")

    if agg:
        agg_l = agg.lower()
        if agg_l not in ("avg", "min", "max", "sum", "count"):
            return [{"error": f"Unsupported agg '{agg}'. Use avg,min,max,sum,count."}]

        span = _normalize_period(period)
        metrics = [
            "temp", "heatIndex", "windChill", "windSpeed", "windGust",
            "pressure", "precipRate", "precipTotal", "winddir", "humidity"
        ]
        agg_exprs = ", ".join([f"{agg_l}_{m} = {agg_l}({m})" for m in metrics])

        if span:
            kql = (
                "KM400WeatherDataTBL "
                f"| where Timestamp between (datetime({start_str})..datetime({end_str})) "
                f"| summarize {agg_exprs} by bin(Timestamp, {span}) "
                "| order by Timestamp asc"
            )
        else:
            kql = (
                "KM400WeatherDataTBL "
                f"| where Timestamp between (datetime({start_str})..datetime({end_str})) "
                f"| summarize {agg_exprs}"
            )

        rows = execute_kusto(query=kql, client=weather_client, database=weather_database)
        _dbg(f"[plant_weather] rows={len(rows)} (agg mode - all metrics)")
        return rows

    # Raw mode: fetch all metrics
    kql = (
        "KM400WeatherDataTBL "
        f"| where Timestamp between (datetime({start_str})..datetime({end_str})) "
        "| project Timestamp, temp, heatIndex, windChill, windSpeed, windGust, pressure, precipRate, precipTotal, winddir, humidity"
    )
    rows = execute_kusto(query=kql, client=weather_client, database=weather_database)
    _dbg(f"[plant_weather] rows={len(rows)} (raw mode)")
    return rows



# SSE transport & application setup
transport = SseServerTransport("/messages/")

async def handle_sse(request):
    async with transport.connect_sse(
        request.scope, request.receive, request._send
    ) as (in_stream, out_stream):
        await mcp._mcp_server.run(
            in_stream,
            out_stream,
            mcp._mcp_server.create_initialization_options()
        )

sse_app = Starlette(
    routes=[
        Route("/sse", handle_sse, methods=["GET"]),
        Mount("/messages/", app=transport.handle_post_message),
    ]
)

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)
app.mount("/", sse_app)

@app.get("/health")
def health():
    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    _dbg("Starting MCP server on 0.0.0.0:8000 ...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
