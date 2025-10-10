import os
import uuid
import datetime
from datetime import timezone, timedelta
import logging
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple, Union
from attr import dataclass
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

# ==========================
# Boot & logging
# ==========================
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mcp-server")

def _dbg(msg: str):
    print(msg, flush=True)
    logger.info(msg)

# ==========================
# Tag metadata
# ==========================
@dataclass(frozen=True)
class TagDetails:
    description: str
    unit: str
    min_val: float
    max_val: float
    hi_hi: float
    hi: float
    low: float
    low_low: float
    component_reference: str
    actionable_guidance: str

TAG_DETAILS: Dict[str, TagDetails] = {
    "400PIC010/PV.CV": TagDetails(
        description="EPF COMP SUCTION TO FLARE",
        unit="PSI",
        min_val=0, max_val=1300, hi_hi=650, hi=558, low=456, low_low=250,
        component_reference="Compressor suction, downstream of V-405",
        actionable_guidance="If >650 PSI: Flare bypass triggered; <250 PSI: flow issue"
    ),
    "400JTPIT4031/PV.CV": TagDetails(
        description="JT INLET EXCHANGER GAS PRESSURE (E-403A/B)",
        unit="PSI",
        min_val=0, max_val=1600, hi_hi=1500, hi=1200, low=950, low_low=400,
        component_reference="E-403A/B inlet (tube side), parallel branch",
        actionable_guidance=">1500 PSI: Overpressure risk; <400 PSI: low feed pressure"
    ),
    "400JTPIT4050/PV.CV": TagDetails(
        description="JT COLD SEPARATOR PRESSURE",
        unit="PSI",
        min_val=0, max_val=2000, hi_hi=1100, hi=1040, low=400, low_low=200,
        component_reference="Cold separator V-405, downstream of JT valve",
        actionable_guidance=">1100 PSI: Overpressure; <200 PSI: separator issue"
    ),
    "400JTTIT4031/PV.CV": TagDetails(
        description="JT INLET EXCHANGER GAS TEMPERATURE (E-403A/B)",
        unit="F",
        min_val=-58, max_val=392, hi_hi=95, hi=90, low=10, low_low=5,
        component_reference="E-403A/B inlet (hot gas side)",
        actionable_guidance=">95°F: Insufficient cooling; <5°F: hydrate risk"
    ),
    "400JTTIT4030/PV.CV": TagDetails(
        description="JT OUTLET EXCHANGER GAS TEMPERATURE (E-403A/B)",
        unit="F",
        min_val=-58, max_val=392, hi_hi=95, hi=90, low=10, low_low=5,
        component_reference="E-403A/B outlet (cold gas side)",
        actionable_guidance=">95°F: Poor heat exchange; <5°F: hydrate risk"
    ),
    "400JTPIT4010/PV.CV": TagDetails(
        description="JT SKID INLET PRESSURE",
        unit="PSI",
        min_val=0, max_val=2000, hi_hi=1300, hi=1200, low=1000, low_low=900,
        component_reference="10″ inlet gas line upstream of E-401/E-403",
        actionable_guidance=">1300 PSI: High upstream pressure; <1080 PSI: feed issue"
    ),
    "400JTTIT4010/PV.CV": TagDetails(
        description="JT INLET GAS TEMPERATURE (E-401A/B/C)",
        unit="F",
        min_val=0, max_val=250, hi_hi=95, hi=90, low=10, low_low=5,
        component_reference="Upstream of E-401A/B/C",
        actionable_guidance=">95°F: Insufficient cooling; <5°F: hydrate risk"
    ),
    "400JTTIT4012/PV.CV": TagDetails(
        description="CONDITIONED GAS TEMPERATURE",
        unit="F",
        min_val=0, max_val=250, hi_hi=140, hi=120, low=85, low_low=70,
        component_reference="Downstream of E-401A/B/C, merging all streams",
        actionable_guidance=">140°F: Poor cooling; <70°F: hydrate risk"
    ),
    "400JTPIT4030/PV.CV": TagDetails(
        description="E-402 DISCHARGE PRESSURE",
        unit="PSI",
        min_val=0, max_val=2000, hi_hi=1090, hi=1076, low=1070, low_low=1060,
        component_reference="E-402 outlet, upstream of JT valve",
        actionable_guidance=">1090 PSI: Outlet high; <1060 PSI: flow issue"
    ),
    "400JTPIT4032/PV.CV": TagDetails(
        description="JT OUTLET EXCHANGER GAS PRESSURE (E-403A/B)",
        unit="PSI",
        min_val=0, max_val=1600, hi_hi=1500, hi=1200, low=400, low_low=300,
        component_reference="E-403A/B outlet (hot side)",
        actionable_guidance=">1500 PSI: Restriction; <300 PSI: leak/flow issue"
    ),
    "400JTTIT4032/PV.CV": TagDetails(
        description="JT OUTLET EXCHANGER GAS TEMPERATURE (E-403A/B)",
        unit="F",
        min_val=-58, max_val=392, hi_hi=95, hi=90, low=10, low_low=5,
        component_reference="E-403A/B outlet (after cooling)",
        actionable_guidance=">95°F: Poor cooling; <5°F: hydrate risk"
    ),
    "400JTTIT4050/PV.CV": TagDetails(
        description="JT COLD SEPARATOR TEMPERATURE",
        unit="F",
        min_val=-40, max_val=250, hi_hi=200, hi=150, low=-5, low_low=-10,
        component_reference="Cold separator V-405",
        actionable_guidance=">200°F: Too warm; <-10°F: hydrate risk"
    ),
    "400JTPIT4011/PV.CV": TagDetails(
        description="CONDITIONED GAS PRESSURE",
        unit="PSI",
        min_val=0, max_val=2000, hi_hi=1200, hi=945, low=400, low_low=300,
        component_reference="Upstream of JT valve (PCV-4050)",
        actionable_guidance=">1200 PSI: High pressure; <300 PSI: leak/flow issue"
    ),
    "400JTTIT4011/PV.CV": TagDetails(
        description="E-401A/B/C OUTLET TEMPERATURE",
        unit="F",
        min_val=-40, max_val=250, hi_hi=200, hi=150, low=-5, low_low=-10,
        component_reference="E-401A/B/C outlet (after cooling)",
        actionable_guidance=">200°F: Poor cooling; <-10°F: hydrate risk"
    ),
    "400FI401/PV.CV": TagDetails(
        description="GAS TO EPF COLD EXCHANGER",
        unit="MMSCFD",
        min_val=0, max_val=200, hi_hi=200, hi=150, low=-5, low_low=-10,
        component_reference="JT skid exchanger system inlet",
        actionable_guidance=">200 MMSCFD: Overcapacity; <-10 MMSCFD: flow issue"
    ),
}

# ==========================
# Kusto clients
# ==========================
KCSB = KustoConnectionStringBuilder.with_aad_application_key_authentication(
    os.environ["KUSTO_SERVICE_URI"],
    os.environ["AZURE_CLIENT_ID"],
    os.environ["AZURE_CLIENT_SECRET"],
    os.environ["AZURE_TENANT_ID"],
)
kusto = KustoClient(KCSB)
DB = os.environ.get("KUSTO_DATABASE", "")

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

# ==========================
# MCP wiring
# ==========================
mcp = FastMCP(name="fabric-eventhouse-mcp", description="MCP server for Fabric Eventhouse tools")

# ==========================
# Helpers
# ==========================

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
    if not relative:
        return None
    text = relative.strip().lower()
    if not text:
        return None
    if now is None:
        now = datetime.datetime.now(timezone.utc)
    end_dt = now

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

    if text in ("last week", "past week"):
        return (end_dt - timedelta(days=7), end_dt)
    if text in ("last 24 hours", "past 24 hours"):
        return (end_dt - timedelta(hours=24), end_dt)
    if text in ("last month", "past month"):
        return (end_dt - timedelta(days=30), end_dt)

    return None


def _normalize_period(period: Optional[str]) -> Optional[str]:
    if not period:
        return None
    t = str(period).strip().lower()

    if t in ("hour", "hourly", "per hour"):  return "1h"
    if t in ("day", "daily", "per day"):     return "1d"
    if t in ("week", "weekly", "per week"):  return "7d"
    if t in ("month", "monthly", "per month"):  return "30d"

    m = re.fullmatch(r"(\d+)\s*w", t)
    if m:
        return f"{int(m.group(1)) * 7}d"

    if re.fullmatch(r"\d+\s*[smhd]", t):
        return t.replace(" ", "")

    return t

# Smart bin policy
LONG_WINDOW_THRESHOLD_DAYS = 15
MIN_BIN_FOR_LONG_WINDOWS = "6h"

SMART_BIN_RULES = [
    (7, "1h"),
    (15, "3h"),
    (10**9, "6h"),  # >15d
]


def _span_to_timedelta(span: Optional[str]) -> Optional[datetime.timedelta]:
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


def _choose_smart_bin(start_dt: datetime.datetime, end_dt: datetime.datetime, requested: Optional[str]) -> Tuple[str, Dict[str, Any]]:
    days = (end_dt - start_dt).total_seconds() / 86400.0
    if requested:
        # Respect explicit period but enforce minimum for very long windows
        td_req = _span_to_timedelta(_normalize_period(requested))
        td_min = _span_to_timedelta(MIN_BIN_FOR_LONG_WINDOWS)
        if days > LONG_WINDOW_THRESHOLD_DAYS and (td_req is None or (td_min and td_req < td_min)):
            return MIN_BIN_FOR_LONG_WINDOWS, {
                "requested": requested, "used": MIN_BIN_FOR_LONG_WINDOWS, "window_days": days, "coerced": True
            }
        return _normalize_period(requested) or "1h", {
            "requested": requested, "used": _normalize_period(requested) or "1h", "window_days": days, "coerced": False
        }
    # choose from rules
    for max_days, span in SMART_BIN_RULES:
        if days <= max_days:
            return span, {"requested": None, "used": span, "window_days": days, "coerced": False}
    return "1h", {"requested": None, "used": "1h", "window_days": days, "coerced": False}


def _fixed_search(query: str, threshold: int = 60) -> List[Tuple[str, str, int]]:
    q = (query or "").strip().lower()
    if not q:
        return []
    results: List[Tuple[str, str, int]] = []
    for tag, details in TAG_DETAILS.items():
        score_desc = fuzz.partial_ratio(q, details.description.lower())
        score_tag  = fuzz.partial_ratio(q, tag.lower())
        score = max(score_desc, score_tag)
        if score >= threshold:
            results.append((tag, details.description, score))
    results.sort(key=lambda x: x[2], reverse=True)
    hits = [(tag, desc) for tag, desc, _ in results]
    _dbg(f"[resolver] Query='{query}'  Threshold={threshold}  Matches={len(hits)}")
    for t, d in hits:
        _dbg(f"[resolver]  -> {t} :: {d}")
    return hits


def _resolve_tags_for_query(query: str, default_all_if_perf: bool = True) -> List[str]:
    q = (query or "").strip()
    if default_all_if_perf and PERF_WORDS.search(q):
        tags = list(TAG_DETAILS.keys())
        _dbg(f"[resolve_tags] Performance-style query detected -> ALL tags ({len(tags)})")
        return tags
    pairs = _fixed_search(q)
    tags = [t for t, _ in pairs]
    if not tags and "/" in q:
        tags = [q]
    if not tags and default_all_if_perf:
        if len(q.split()) <= 2:
            tags = list(TAG_DETAILS.keys())
            _dbg(f"[resolve_tags] Very vague query -> ALL tags ({len(tags)})")
    tags = sorted(set(tags))
    _dbg(f"[resolve_tags] Resolved={tags}")
    return tags

# ==========================
# Weather helper
# ==========================

def _get_weather_for_window(
    start_time: Optional[str], 
    end_time: Optional[str], 
    relative: Optional[str],
    period: Optional[str] = None
) -> List[Dict[str, Any]]:
    bin_period = period or "1h"
    return plant_weather(
        start_time=start_time,
        end_time=end_time,
        agg="avg",
        period=bin_period,
        weather_database=WEATHER_DB
    )

# ==========================
# pid.txt context loader
# ==========================
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

# ==========================
# Tools
# ==========================

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


@mcp.tool(description="Run a read-only KQL query on the specified database ISPPLANTDATA and return the results.")
def kusto_query(query: str, database: str = DB) -> List[Dict[str, Any]]:
    _dbg(f"[kusto_query] DB={database}")
    return execute_kusto(query=query, client=kusto, database=database)


@mcp.tool(description=f"Return the full JSON schema for the ISPPLANTDATA table in the database.")
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


def _meta_for_tag(tag: str) -> Dict[str, Any]:
    details = TAG_DETAILS.get(tag)
    if not details:
        return {"tag": tag, "desc": None}
    return {
        "tag": tag,
        "desc": details.description,
        "unit": details.unit,
        "ranges": {
            "min": details.min_val, "max": details.max_val,
            "hi_hi": details.hi_hi, "hi": details.hi,
            "low": details.low, "low_low": details.low_low
        },
        "component_reference": details.component_reference,
        "actionable_guidance": details.actionable_guidance
    }


def _classify_value(tag: str, val: Optional[Union[int, float]]) -> Dict[str, Any]:
    if val is None:
        return {"status": "UNKNOWN", "reason": "value is None"}
    rng = TAG_DETAILS.get(tag)
    if not rng:
        return {"status": "UNKNOWN", "reason": "no ranges for tag"}
    try:
        v = float(val)
    except Exception:
        return {"status": "UNKNOWN", "reason": "non-numeric"}

    if v < rng.min_val or v > rng.max_val:
        return {"status": "INVALID", "reason": f"outside sensor bounds [{rng.min_val},{rng.max_val}]"}
    if v <= rng.low_low:
        return {"status": "LOW_LOW", "reason": f"<= low_low ({rng.low_low})"}
    if v <= rng.low:
        return {"status": "LOW", "reason": f"<= low ({rng.low})"}
    if v >= rng.hi_hi:
        return {"status": "HI_HI", "reason": f">= hi_hi ({rng.hi_hi})"}
    if v >= rng.hi:
        return {"status": "HI", "reason": f">= hi ({rng.hi})"}
    return {"status": "OK", "reason": f"within ({rng.low},{rng.hi})"}


def _summarize_series(tag: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    import math
    vals: List[float] = []
    statuses: List[str] = []
    anomalies: List[Dict[str, Any]] = []
    crossings = 0

    last_status = None
    first_val = None
    last_val = None

    for r in rows:
        v = r.get("Value")
        cls = _classify_value(tag, v)
        st = cls["status"]
        statuses.append(st)

        if st in ("INVALID", "LOW_LOW", "LOW", "HI", "HI_HI"):
            anomalies.append({
                "timestamp": r.get("Timestamp"),
                "value": v,
                "status": st,
                "reason": cls["reason"]
            })

        if last_status is not None and st != last_status:
            crossings += 1
        last_status = st

        try:
            fv = float(v)
            vals.append(fv)
            if first_val is None:
                first_val = fv
            last_val = fv
        except Exception:
            pass

    dist = {
        "OK": statuses.count("OK"),
        "LOW": statuses.count("LOW"),
        "LOW_LOW": statuses.count("LOW_LOW"),
        "HI": statuses.count("HI"),
        "HI_HI": statuses.count("HI_HI"),
        "INVALID": statuses.count("INVALID"),
        "UNKNOWN": statuses.count("UNKNOWN"),
        "total": len(statuses),
    }

    if vals:
        vmin, vmax = min(vals), max(vals)
        vavg = sum(vals) / len(vals)
        trend = (last_val - first_val) if (first_val is not None and last_val is not None) else None
    else:
        vmin = vmax = vavg = trend = None

    return {
        "min": vmin, "max": vmax, "avg": vavg,
        "first": first_val, "last": last_val, "trend": trend,
        "status_distribution": dist,
        "status_crossings": crossings,
        "anomalies": anomalies
    }

# ==========================
# Aggregations (Kusto summarize)
# ==========================
@mcp.tool(description=f"Summarize ISPPLANTDATA over time for tags. Supports agg in (avg,min,max,sum,count) and optional bin period like 1h/1d. You can pass relative='last 7 days' etc.")
def isp_stats(
    tag_names: str,
    agg: str = "avg",
    period: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    database: str = DB,
    relative: Optional[str] = None,
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

    # Choose / enforce bin
    chosen_span, bin_meta = _choose_smart_bin(start_dt, end_dt, period)
    _dbg(f"[isp_stats] Bin policy: requested={bin_meta['requested']!r} -> used={chosen_span!r}; window_days={bin_meta['window_days']:.2f}")

    tags_array = "[" + ",".join(f"'{_escape_kql_literal(t)}'" for t in tags) + "]"

    if chosen_span:
        kql = (
            f"{ISP_TABLE} "
            f"| where {ISP_TS_COL} between (datetime({start_str})..datetime({end_str})) "
            f"| where {ISP_TAG_COL} in (dynamic({tags_array})) "
            f"| summarize {agg_l}_val = {agg_l}({ISP_VAL_COL}) by {ISP_TAG_COL}, bin({ISP_TS_COL}, {chosen_span}) "
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
        if chosen_span:
            kql_fb = (
                f"{ISP_TABLE} "
                f"| where {ISP_TS_COL} between (datetime({start_str})..datetime({end_str})) "
                f"| where {ISP_TAG_COL} has_any (dynamic({tokens_array})) "
                f"| summarize {agg_l}_val = {agg_l}({ISP_VAL_COL}) by {ISP_TAG_COL}, bin({ISP_TS_COL}, {chosen_span}) "
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

# ==========================
# Fixed tag tools
# ==========================
@mcp.tool(description="List all EPF JT tags with full metadata.")
def fixed_tags_all() -> List:
    return [
        {
            "tag": tag,
            "desc": details.description,
            "unit": details.unit,
            "ranges": {
                "min": details.min_val, "max": details.max_val,
                "hi_hi": details.hi_hi, "hi": details.hi,
                "low": details.low, "low_low": details.low_low
            },
            "component_reference": details.component_reference,
            "actionable_guidance": details.actionable_guidance
        }
        for tag, details in TAG_DETAILS.items()
    ]


@mcp.tool(description="Look up the fixed EPF JT tags by tag/prefix or by keywords like 'temperature' or 'pressure'. Returns matching tags with descriptions.")
def fixed_tags_lookup(query: str) -> List[Dict[str, str]]:
    _dbg(f"[fixed_tags_lookup] query='{query}'")
    pairs = _fixed_search(query)
    out = [{"tag": t, "desc": d, "unit": TAG_DETAILS.get(t).unit if t in TAG_DETAILS else None} for t, d in pairs]
    _dbg(f"[fixed_tags_lookup] Matches={len(out)}")
    return out


@mcp.tool(description="Get description for a specific tag from the fixed set only. Returns {'tag','desc','source'='fixed'|'not_found'}.")
def fixed_tag_describe(tag_name: str) -> Dict[str, Any]:
    tag = (tag_name or "").strip()
    _dbg(f"[fixed_tag_describe] tag_name='{tag}'")
    if not tag:
        return {"error": "tag_name is required"}
    if tag in TAG_DETAILS:
        out = {"tag": tag, "desc": TAG_DETAILS[tag].description, "source": "fixed"}
        _dbg(f"[fixed_tag_describe] HIT fixed -> {out}")
        return out
    _dbg("[fixed_tag_describe] NOT FOUND")
    return {"tag": tag, "desc": None, "source": "not_found"}

# ==========================
# P&ID context tool
# ==========================
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

# ==========================
# Aggregation-FIRST value/context query (replaces raw fetch path)
# ==========================
@mcp.tool(description="Resolve tags from free-text (or ALL tags for 'performance/analysis/trend' asks), then fetch ISPPLANTDATA **aggregated** values (avg) using smart bin policy; returns values + tag desc + pid context + weather.")
def context_values_by_query(
    query: str,
    start_time: Optional[str] = None,
    end_time:   Optional[str] = None,
    database:   str = DB,
    relative:   Optional[str] = None,
    period: Optional[str] = None
) -> Dict[str, Any]:
    _dbg(f"[context_values_by_query] query='{query}' start={start_time} end={end_time} DB={database} relative={relative} period={period}")
    tags = _resolve_tags_for_query(query, default_all_if_perf=True)
    _dbg(f"[context_values_by_query] tags_resolved={tags}")
    if not tags:
        return {"tags_resolved": [], "note": "No tags matched your query."}

    now = datetime.datetime.now(timezone.utc)
    if relative:
        win = _parse_relative_window(relative, now)
        if win:
            start_dt, end_dt = win
        else:
            end_dt = now
            start_dt = end_dt - timedelta(minutes=15)
    else:
        try:
            end_dt   = _parse_iso_dt(end_time, now)
            start_dt = _parse_iso_dt(start_time, end_dt - timedelta(minutes=15))
        except ValueError as e:
            return {"tags_resolved": tags, "error": str(e)}

    if start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt

    chosen_span, bin_meta = _choose_smart_bin(start_dt, end_dt, period)

    # Fetch aggregated
    rows = isp_stats(
        tag_names=",".join(tags),
        agg="avg",
        period=chosen_span,
        start_time=_utc_str(start_dt),
        end_time=_utc_str(end_dt),
        database=database,
        relative=None,
    )

    # Map to canonical series per tag (Timestamp, TagName, Value)
    by_tag: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        t = r.get(ISP_TAG_COL)
        ts = r.get(ISP_TS_COL)
        val = r.get("avg_val")
        if t is None:
            continue
        if ts is None and isinstance(val, (int, float)):
            # unbinned aggregate; synthesize a single timestamp at end_dt
            ts = _utc_str(end_dt)
        by_tag.setdefault(t, []).append({
            "Timestamp": ts,
            "TagName": t,
            "Value": val,
        })

    for t in by_tag:
        by_tag[t].sort(key=lambda r: r.get("Timestamp"))

    inmem = [_meta_for_tag(t) for t in tags]
    pid = pid_context(start_time=_utc_str(start_dt), end_time=_utc_str(end_dt), relative=None, period=chosen_span)

    out = {
        "tags_resolved": tags,
        "tag_context_in_memory": inmem,
        "series": by_tag,                      # per-tag series (canonical shape)
        "values": rows,                        # raw aggregated KQL output (compat)
        "period": chosen_span,
        "time_window": {
            "start_time": _utc_str(start_dt),
            "end_time": _utc_str(end_dt),
            "relative": relative,
        },
        "bin_policy": bin_meta,
        "pid_context": pid,
        "weather": pid.get("weather", []),
    }
    return out

# ==========================
# Performance analysis (now uses aggregated values only)
# ==========================
@mcp.tool(description="Evaluate performance vs thresholds using aggregated values (smart-binned). Resolves tags by query (ALL canonical for perf-style). Returns per-tag summaries + anomalies + P&ID + optional weather.")
def isp_performance_for_query(
    query: Optional[str] = None,
    tag_names: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    relative: Optional[str] = None,
    database: str = DB,
    with_weather: bool = False,
    period: Optional[str] = None,
) -> Dict[str, Any]:
    _dbg(f"[isp_performance_for_query] query='{query}' tag_names='{tag_names}' start={start_time} end={end_time} relative={relative} weather={with_weather} period={period}")

    # Resolve tags
    if tag_names:
        tags = [t.strip() for t in (tag_names or "").split(",") if t.strip()]
    else:
        tags = _resolve_tags_for_query(query or "", default_all_if_perf=True)

    if not tags:
        return {"tags_resolved": [], "note": "No tags found to evaluate."}

    now = datetime.datetime.now(timezone.utc)
    if relative:
        win = _parse_relative_window(relative, now)
        if win:
            start_dt, end_dt = win
        else:
            end_dt = now
            start_dt = end_dt - timedelta(minutes=15)
    else:
        try:
            end_dt   = _parse_iso_dt(end_time, now)
            start_dt = _parse_iso_dt(start_time, end_dt - timedelta(minutes=15))
        except ValueError as e:
            return {"tags_resolved": tags, "error": str(e)}

    if start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt

    chosen_span, bin_meta = _choose_smart_bin(start_dt, end_dt, period)

    # Aggregated values fetch
    agg_rows = isp_stats(
        tag_names=",".join(tags),
        agg="avg",
        period=chosen_span,
        start_time=_utc_str(start_dt),
        end_time=_utc_str(end_dt),
        database=database,
        relative=None,
    )

    # Canonicalize for summarizer
    by_tag: Dict[str, List[Dict[str, Any]]] = {}
    for r in agg_rows:
        t = r.get(ISP_TAG_COL)
        ts = r.get(ISP_TS_COL)
        val = r.get("avg_val")
        if t is None:
            continue
        if ts is None and isinstance(val, (int, float)):
            ts = _utc_str(end_dt)
        by_tag.setdefault(t, []).append({
            "Timestamp": ts,
            "TagName": t,
            "Value": val,
        })
    for t in by_tag:
        by_tag[t].sort(key=lambda r: r.get("Timestamp"))

    evaluations: Dict[str, Any] = {}
    global_anomalies: List[Dict[str, Any]] = []

    for t in tags:
        series = by_tag.get(t, [])
        summary = _summarize_series(t, series)
        meta = _meta_for_tag(t)
        evaluations[t] = {
            "meta": meta,
            "summary": summary
        }
        for a in summary["anomalies"]:
            a2 = dict(a)
            a2["tag"] = t
            a2["unit"] = meta.get("unit")
            global_anomalies.append(a2)

    pid = pid_context(start_time=_utc_str(start_dt), end_time=_utc_str(end_dt), relative=None, period=chosen_span)

    weather = []
    if with_weather:
        try:
            weather = plant_weather(start_time=_utc_str(start_dt), end_time=_utc_str(end_dt), agg="avg", period="1h")
        except Exception as e:
            _dbg(f"[isp_performance_for_query] weather error: {e}")

    out = {
        "tags_resolved": tags,
        "tag_context_in_memory": [_meta_for_tag(t) for t in tags],
        "window": {
            "start_time": _utc_str(start_dt),
            "end_time": _utc_str(end_dt),
            "relative": relative,
        },
        "period": chosen_span,
        "bin_policy": bin_meta,
        "evaluations": evaluations,
        "anomalies": global_anomalies,
        "pid_context": pid,
        "weather": weather,
    }
    _dbg(f"[isp_performance_for_query] tags={len(tags)} anomalies={len(global_anomalies)}")
    return out

# ==========================
# Query-time aggregate convenience wrapper
# ==========================
@mcp.tool(description="Resolve tags then compute aggregates over ISPPLANTDATA. agg in (avg,min,max,sum,count). period defaults to smart policy. Returns P&ID context and bin-policy metadata.")
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

    span, bin_meta = _choose_smart_bin(start_dt, end_dt, period)

    rows = isp_stats(
        tag_names=",".join(tags),
        agg=agg,
        period=span,
        start_time=_utc_str(start_dt),
        end_time=_utc_str(end_dt),
        database=database,
        relative=None,
    )

    inmem = [_meta_for_tag(t) for t in tags]
    pid = pid_context(start_time=_utc_str(start_dt), end_time=_utc_str(end_dt), relative=None, period=None)

    out: Dict[str, Any] = {
        "tags_resolved": tags,
        "aggregate": agg,
        "period": span,
        "results": rows,
        "time_window": {
            "start_time": _utc_str(start_dt),
            "end_time": _utc_str(end_dt),
            "relative": relative,
        },
        "tag_context_in_memory": inmem,
        "pid_context": pid,
        "bin_policy": bin_meta,
    }

    _dbg(f"[isp_aggregate_for_query] results_rows={len(rows)}")
    return out

# ==========================
# Weather tool
# ==========================
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

        span = _normalize_period(period) or "1h"
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

    kql = (
        "KM400WeatherDataTBL "
        f"| where Timestamp between (datetime({start_str})..datetime({end_str})) "
        "| project Timestamp, temp, heatIndex, windChill, windSpeed, windGust, pressure, precipRate, precipTotal, winddir, humidity"
    )
    rows = execute_kusto(query=kql, client=weather_client, database=weather_database)
    _dbg(f"[plant_weather] rows={len(rows)} (raw mode)")
    return rows

# ==========================
# App & SSE transport
# ==========================
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
