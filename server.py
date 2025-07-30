import os
import uuid
import datetime
import logging
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
import requests

from azure.kusto.data import (
    KustoClient,
    KustoConnectionStringBuilder,
    ClientRequestProperties,
)
from azure.kusto.data.response import KustoResponseDataSet

from mcp.server.fastmcp import FastMCP
from mcp.server.sse import SseServerTransport

from starlette.applications import Starlette
from starlette.routing import Route, Mount
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mcp-server")

# Kusto (Fabric Eventhouse) connection
KCSB = KustoConnectionStringBuilder.with_aad_application_key_authentication(
    os.environ["KUSTO_SERVICE_URI"],
    os.environ["AZURE_CLIENT_ID"],
    os.environ["AZURE_CLIENT_SECRET"],
    os.environ["AZURE_TENANT_ID"],
)
kusto = KustoClient(KCSB)
DB = os.environ.get("KUSTO_DATABASE", "")
CLUSTER_URI = os.environ.get("KUSTO_SERVICE_URI", "")

# Initialize the MCP server with your tools
mcp = FastMCP(name="fabric-eventhouse-mcp", description="MCP server for Fabric Eventhouse tools")

# ──────────────────────────────────────────────────────────
# Time & Weather tools
# ──────────────────────────────────────────────────────────

@mcp.tool()
def TimeTool(input_timezone: Optional[str] = None) -> str:
    """
    Provides the current time for a given timezone like Asia/Kolkata.
    If no timezone is provided, returns local time.
    """
    fmt = "%Y-%m-%d %H:%M:%S %Z%z"
    now = datetime.datetime.now()
    if input_timezone:
        try:
            now = now.astimezone(ZoneInfo(input_timezone))
        except Exception as e:
            logger.error("Invalid timezone %s: %s", input_timezone, e)
            return f"Error: '{input_timezone}' is not a valid timezone."
    return f"The current time is {now.strftime(fmt)}."

@mcp.tool(description="Get weather data (current, forecast, or historical) from WeatherAPI.com")
def weather_tool(
    location: str,
    date: Optional[str] = None,
    forecast_days: int = 0
) -> Dict[str, Any]:
    """
    location: city name or lat,long
    date:   YYYY-MM-DD if you want historical weather
    forecast_days: number of days ahead if you want a forecast
    If neither date nor forecast_days is set, returns current weather.
    """
    api_key = os.getenv("WEATHERAPI_API_KEY", "3e4ebd0c4cf54473994103714252907")
    base = "http://api.weatherapi.com/v1"
    
    if date:
        endpoint = "history.json"
        params = {"key": api_key, "q": location, "dt": date}
    elif forecast_days and forecast_days > 0:
        endpoint = "forecast.json"
        params = {"key": api_key, "q": location, "days": forecast_days}
    else:
        endpoint = "current.json"
        params = {"key": api_key, "q": location, "aqi": "no"}

    resp = requests.get(f"{base}/{endpoint}", params=params)
    data = resp.json()
    
    return data

# ──────────────────────────────────────────────────────────
# Kusto helper functions
# ──────────────────────────────────────────────────────────

def format_results(
    result_set: Optional[KustoResponseDataSet],
) -> List[Dict[str, Any]]:
    """Format Kusto response into list of dictionaries."""
    if not result_set or not getattr(result_set, "primary_results", None):
        return []
    first = result_set.primary_results[0]
    cols = [col.column_name for col in first.columns]
    return [dict(zip(cols, row)) for row in first.rows]

def execute_kusto(
    query: str, database: str = DB, readonly: bool = True
) -> List[Dict[str, Any]]:
    """Execute a Kusto query and return formatted results."""
    crp = ClientRequestProperties()
    crp.application = "fabric-eventhouse-mcp"
    crp.client_request_id = f"MCP:{uuid.uuid4()}"
    if readonly:
        crp.set_option("request_readonly", True)
    result = kusto.execute(database, query, crp)
    return format_results(result)

# ──────────────────────────────────────────────────────────
# Kusto‑based MCP tools
# ──────────────────────────────────────────────────────────

@mcp.tool(description="List all table names in the specified Kusto database. Use this to explore what data sources are available.")
def list_tables(database: str = DB) -> str:
    rows = kusto.execute(database, ".show tables").primary_results[0]
    names = [r["TableName"] for r in rows]
    return "\n".join(names)

@mcp.tool(description="Return the full JSON schema (column names, types) for a specified table in the database.")
def table_schema(table: str, database: str = DB) -> str:
    result = kusto.execute(database, f".show table {table} cslschema")
    return result.primary_results[0][0]["Schema"]

@mcp.tool(description="Run a read-only Kusto Query Language (KQL) query on the specified database and return the results.")
def kusto_query(query: str, database: str = DB) -> List[Dict[str, Any]]:
    return execute_kusto(query, database, readonly=True)

@mcp.tool(description="Execute a Kusto management command")
def kusto_command(command: str, database: str = DB) -> List[Dict[str, Any]]:
    return execute_kusto(command, database, readonly=False)

@mcp.tool(description="List all available databases in the connected Kusto cluster. Useful to discover valid targets for queries.")
def kusto_list_databases() -> List[Dict[str, Any]]:
    return execute_kusto(".show databases", DB, readonly=True)

@mcp.tool(description="List all tables in the specified database")
def kusto_list_tables(database: str = DB) -> List[Dict[str, Any]]:
    return execute_kusto(".show tables", database, readonly=True)

@mcp.tool(description="Get schema information for all entities in the database")
def kusto_get_entities_schema(database: str = DB) -> List[Dict[str, Any]]:
    q = (
        ".show databases entities with (showObfuscatedStrings=true) "
        f"| where DatabaseName == '{database}' "
        "| project EntityName, EntityType, Folder, DocString"
    )
    return execute_kusto(q, database, readonly=True)

@mcp.tool(description="Retrieve the column schema and types for a specific table in the given Kusto database.")
def kusto_get_table_schema(
    table_name: str, database: str = DB
) -> List[Dict[str, Any]]:
    return execute_kusto(f".show table {table_name} cslschema", database, readonly=True)

@mcp.tool(description="Retrieve the function definition and metadata for a specified Kusto function within the given database.")
def kusto_get_function_schema(
    function_name: str, database: str = DB
) -> List[Dict[str, Any]]:
    return execute_kusto(f".show function {function_name}", database, readonly=True)

@mcp.tool(description="Return a random sample of records from a specific table in the database. Useful for quick previews.")
def kusto_sample_table_data(
    table_name: str, sample_size: int = 10, database: str = DB
) -> List[Dict[str, Any]]:
    return execute_kusto(f"{table_name} | sample {sample_size}", database, readonly=True)

@mcp.tool(description="Sample random records from a function call result")
def kusto_sample_function_data(
    function_call_with_params: str,
    sample_size: int = 10,
    database: str = DB,
) -> List[Dict[str, Any]]:
    return execute_kusto(
        f"{function_call_with_params} | sample {sample_size}", database, readonly=True
    )

@mcp.tool(description="Ingest inline CSV data into a table")
def kusto_ingest_inline_into_table(
    table_name: str, data_comma_separator: str, database: str = DB
) -> List[Dict[str, Any]]:
    cmd = f".ingest inline into table {table_name} <| {data_comma_separator}"
    return execute_kusto(cmd, database, readonly=False)

@mcp.tool(description="Get cluster information")
def kusto_get_clusters() -> List[tuple]:
    return [(CLUSTER_URI, "Primary cluster")]

@mcp.tool(
    description="Get semantically similar shots from a shots table"
)
def kusto_get_shots(
    prompt: str,
    shots_table_name: str,
    sample_size: int = 3,
    database: str = DB,
    embedding_endpoint: Optional[str] = None,
) -> List[Dict[str, Any]]:
    endpoint = embedding_endpoint or os.getenv("AZ_OPENAI_EMBEDDING_ENDPOINT", "")
    if not endpoint:
        raise ValueError("No embedding endpoint provided.")
    kql = f"""
        let model_endpoint = '{endpoint}';
        let embedded_term = toscalar(evaluate ai_embeddings('{prompt}', model_endpoint));
        {shots_table_name}
        | extend similarity = series_cosine_similarity(embedded_term, EmbeddingVector)
        | top {sample_size} by similarity
        | project similarity, EmbeddingText, AugmentedText
    """
    return execute_kusto(kql, database, readonly=True)

# ──────────────────────────────────────────────────────────
# SSE transport & application setup
# ──────────────────────────────────────────────────────────

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

# Starlette app for SSE endpoints
sse_app = Starlette(
    routes=[
        Route("/sse", handle_sse, methods=["GET"]),
        Mount("/messages/", app=transport.handle_post_message),
    ]
)

# Main FastAPI app
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
    uvicorn.run(app, host="0.0.0.0", port=8000)
