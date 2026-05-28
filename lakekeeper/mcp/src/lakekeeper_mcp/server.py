"""Lakekeeper MCP server — Iceberg catalog metadata for OpenCode.

Provides content-level catalog discovery (schema, comments, partitioning,
snapshots) — no catalog management. Goal: give the LLM enough context to
write efficient StarRocks SQL against the `lake` catalog.

File layout (top to bottom):
  1. Configuration   — all settings come from environment variables.
  2. Authentication  — optional OIDC token (client_credentials), cached.
  3. Catalog access  — HTTP calls against the Lakekeeper REST API.
  4. Format helpers  — turn Iceberg metadata into readable text.
  5. Tools           — the four functions decorated with @mcp.tool().

How FastMCP works: each function decorated with @mcp.tool() is exposed to
OpenCode as a callable tool. The function docstring is the description
the LLM sees; the type-annotated parameters become the input schema.
main() at the bottom of the file calls mcp.run() to start the JSON-RPC
loop over stdin/stdout. New tool = new function + @mcp.tool() + a
meaningful docstring; nothing else. Details: README.md, section
"Code-Aufbau & eigene Tools".
"""

import datetime
import os
import time
from typing import Any
from urllib.parse import quote

import httpx
from fastmcp import FastMCP

mcp = FastMCP("lakekeeper-catalog")

# --- 1. Configuration (all from env vars) ---------------------------------
LAKEKEEPER_URL = os.environ.get(
    "LAKEKEEPER_URL", "http://lakekeeper.lakekeeper.svc.cluster.local:8181"
).rstrip("/")
WAREHOUSE = os.environ.get("LAKEKEEPER_WAREHOUSE", "main")
KEYCLOAK_TOKEN_URL = os.environ.get("KEYCLOAK_TOKEN_URL", "")
KEYCLOAK_CLIENT_ID = os.environ.get("KEYCLOAK_CLIENT_ID", "")
KEYCLOAK_CLIENT_SECRET = os.environ.get("KEYCLOAK_CLIENT_SECRET", "")

HTTP_TIMEOUT = 15.0

# Process-local caches: the token until shortly before expiry, the base URL
# permanently.
_token_cache: dict[str, Any] = {"token": None, "expires_at": 0.0}
_prefix_cache: dict[str, str] = {}


# --- 2. Authentication ----------------------------------------------------
def _get_token() -> str | None:
    """Fetch and cache a client_credentials token.

    Without KEYCLOAK_TOKEN_URL the server runs unauthenticated (dev mode)
    and this function returns None.
    """
    if not KEYCLOAK_TOKEN_URL:
        return None
    now = time.time()
    if _token_cache["token"] and now < _token_cache["expires_at"]:
        return _token_cache["token"]
    resp = httpx.post(
        KEYCLOAK_TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": KEYCLOAK_CLIENT_ID,
            "client_secret": KEYCLOAK_CLIENT_SECRET,
        },
        timeout=HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()
    _token_cache["token"] = data["access_token"]
    # 60s buffer so a token does not expire mid-request.
    _token_cache["expires_at"] = now + data.get("expires_in", 3600) - 60
    return _token_cache["token"]


def _headers() -> dict[str, str]:
    """Authorization header if OIDC is active — else an empty dict."""
    token = _get_token()
    return {"Authorization": f"Bearer {token}"} if token else {}


# --- 3. Catalog access ----------------------------------------------------
def _catalog_base() -> str:
    """Base URL for all Iceberg REST calls, including the warehouse prefix.

    Lakekeeper routes multiple warehouses via a prefix returned by the
    /config endpoint in `overrides.prefix`. Look it up once, cache it.
    """
    if "base" not in _prefix_cache:
        resp = httpx.get(
            f"{LAKEKEEPER_URL}/catalog/v1/config",
            headers=_headers(),
            params={"warehouse": WAREHOUSE},
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        cfg = resp.json()
        prefix = cfg.get("overrides", {}).get("prefix") or cfg.get("defaults", {}).get("prefix", "")
        _prefix_cache["base"] = (
            f"{LAKEKEEPER_URL}/catalog/v1/{quote(prefix, safe='')}"
            if prefix
            else f"{LAKEKEEPER_URL}/catalog/v1"
        )
    return _prefix_cache["base"]


def _ns_raw(namespace: str) -> str:
    """'gold.finance' -> 'gold\x1ffinance'.

    Iceberg REST joins namespace levels with the unit separator 0x1F.
    This form is meant for query parameters (httpx encodes them).
    """
    return "\x1F".join(namespace.split("."))


def _ns_path(namespace: str) -> str:
    """Like _ns_raw, but additionally URL-encoded — for use in URL paths."""
    return quote(_ns_raw(namespace), safe="")


def _get(path: str, params: dict | None = None) -> dict:
    """GET against the catalog API; `path` is relative to _catalog_base()."""
    resp = httpx.get(
        f"{_catalog_base()}/{path}",
        headers=_headers(),
        params=params or {},
        timeout=HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def _table_metadata(namespace: str, table: str) -> dict:
    """Load the full Iceberg metadata of a table (LoadTableResult).

    The returned dict is the `metadata` object — see README.md, section
    "Form der Iceberg-Metadaten" for the important keys.
    """
    result = _get(f"namespaces/{_ns_path(namespace)}/tables/{quote(table, safe='')}")
    return result["metadata"]


# --- 4. Format helpers ----------------------------------------------------
def _format_type(t: Any) -> str:
    """Render an Iceberg type in readable form.

    Primitive types are strings ('long', 'string', ...); nested ones
    (struct/list/map) are dicts and are resolved recursively.
    """
    if isinstance(t, str):
        return t
    if isinstance(t, dict):
        kind = t.get("type", "")
        if kind == "struct":
            inner = ", ".join(
                f"{f['name']}: {_format_type(f['type'])}" for f in t.get("fields", [])
            )
            return f"struct<{inner}>"
        if kind == "list":
            return f"list<{_format_type(t.get('element-type', '?'))}>"
        if kind == "map":
            return (
                f"map<{_format_type(t.get('key-type', '?'))}, "
                f"{_format_type(t.get('value-type', '?'))}>"
            )
    return str(t)


def _current_schema(meta: dict) -> dict:
    """The currently active schema of a table.

    `schemas` is a list; the active one is chosen via `current-schema-id`.
    Tolerates the v1 format that only has a single `schema`.
    """
    schema_id = meta.get("current-schema-id", 0)
    schemas = meta.get("schemas") or [meta.get("schema", {})]
    return next((s for s in schemas if s.get("schema-id") == schema_id), schemas[-1])


# --- 5. Tools (callable from OpenCode) ------------------------------------
@mcp.tool()
def list_namespaces(parent: str = "") -> str:
    """List all namespaces in the lakehouse catalog (bronze.*, silver.*,
    gold.*, serving.*, mart.*).

    parent: optional top-level namespace (e.g. 'gold') to show only its
    child namespaces. Empty = all.
    """
    params = {"parent": _ns_raw(parent)} if parent else None
    raw = _get("namespaces", params).get("namespaces", [])
    names = sorted(".".join(levels) for levels in raw)
    return "\n".join(names) if names else "No namespaces found."


@mcp.tool()
def list_tables(namespace: str) -> str:
    """List all tables in a namespace.

    namespace: e.g. 'gold.finance' or 'silver.crm'
    """
    raw = _get(f"namespaces/{_ns_path(namespace)}/tables").get("identifiers", [])
    names = sorted(i["name"] for i in raw)
    return "\n".join(names) if names else f"No tables in '{namespace}'."


@mcp.tool()
def describe_table(namespace: str, table: str) -> str:
    """Full metadata for a table: columns with types and comments,
    partition spec, sort order, relevant TBLPROPERTIES, current snapshot.

    Use this to write efficient StarRocks SQL: the partition spec shows
    which WHERE conditions trigger partition pruning.

    namespace: e.g. 'gold.finance', table: e.g. 'orders'
    """
    meta = _table_metadata(namespace, table)
    schema = _current_schema(meta)
    fields = schema.get("fields", [])
    # source-id in the partition/sort spec refers to a schema field id.
    id_to_field = {f["id"]: f for f in fields}

    lines: list[str] = [f"# {namespace}.{table}"]
    props = meta.get("properties", {})
    if props.get("comment"):
        lines.append(f"Description: {props['comment']}")
    lines.append(f"Location: {meta.get('location', '—')}")
    lines.append(f"Format version: {meta.get('format-version', '—')}")

    lines.append("")
    lines.append("## Schema")
    lines.append(f"{'Column':<32} {'Type':<28} {'Nullable':<9} Comment")
    lines.append("─" * 96)
    for f in fields:
        nullable = "no" if f.get("required") else "yes"
        lines.append(
            f"{f['name']:<32} {_format_type(f['type']):<28} "
            f"{nullable:<9} {f.get('doc', '')}"
        )

    spec_id = meta.get("default-spec-id", 0)
    spec = next(
        (s for s in meta.get("partition-specs", []) if s.get("spec-id") == spec_id), None
    )
    lines.append("")
    if spec and spec.get("fields"):
        lines.append("## Partitioning")
        for pf in spec["fields"]:
            src = id_to_field.get(pf["source-id"], {}).get("name", f"id={pf['source-id']}")
            lines.append(f"  {pf['transform']}({src})")
        lines.append("")
        lines.append("  -> WHERE filters on these source columns trigger partition pruning.")
        lines.append("  -> Without a matching filter, all partitions are scanned.")
    else:
        lines.append("## Partitioning: none (every query is a full table scan)")

    sort_id = meta.get("default-sort-order-id", 0)
    sort = next(
        (s for s in meta.get("sort-orders", []) if s.get("order-id") == sort_id), None
    )
    lines.append("")
    if sort and sort.get("fields"):
        lines.append("## Sort order")
        for sf in sort["fields"]:
            src = id_to_field.get(sf["source-id"], {}).get("name", f"id={sf['source-id']}")
            transform = sf.get("transform", "identity")
            col = f"{transform}({src})" if transform != "identity" else src
            lines.append(f"  {col} {sf.get('direction', 'asc')} {sf.get('null-order', '')}".rstrip())
    else:
        lines.append("## Sort order: none")

    relevant = {
        k: v
        for k, v in props.items()
        if k.startswith("maintenance.")
        or k in {
            "write.format.default",
            "write.parquet.compression-codec",
            "write.target-file-size-bytes",
            "write.parquet.row-group-size-bytes",
        }
    }
    if relevant:
        lines.append("")
        lines.append("## Relevant properties")
        for k, v in sorted(relevant.items()):
            lines.append(f"  {k} = {v}")

    current_id = meta.get("current-snapshot-id")
    current = next(
        (s for s in meta.get("snapshots", []) if s.get("snapshot-id") == current_id), None
    )
    if current:
        ts = datetime.datetime.fromtimestamp(
            current["timestamp-ms"] / 1000, tz=datetime.timezone.utc
        )
        summary = current.get("summary", {})
        lines.append("")
        lines.append("## Current snapshot")
        lines.append(f"  ID: {current_id}")
        lines.append(f"  Timestamp: {ts.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        lines.append(f"  Operation: {summary.get('operation', '—')}")
        if "total-records" in summary:
            lines.append(f"  Rows (approx.): {int(summary['total-records']):,}")

    return "\n".join(lines)


@mcp.tool()
def list_snapshots(namespace: str, table: str, limit: int = 10) -> str:
    """Show the latest snapshots of a table — for time-travel queries.

    namespace: e.g. 'gold.finance', table: e.g. 'orders', limit: max count.
    """
    meta = _table_metadata(namespace, table)
    snapshots = sorted(
        meta.get("snapshots", []), key=lambda s: s["timestamp-ms"], reverse=True
    )[:limit]
    if not snapshots:
        return f"No snapshots for '{namespace}.{table}'."

    lines = [f"# Snapshots: {namespace}.{table} (latest {len(snapshots)})"]
    lines.append(f"{'Snapshot ID':<22} {'Timestamp (UTC)':<22} {'Operation':<12} Row delta")
    lines.append("─" * 78)
    for s in snapshots:
        ts = datetime.datetime.fromtimestamp(
            s["timestamp-ms"] / 1000, tz=datetime.timezone.utc
        )
        summary = s.get("summary", {})
        delta = summary.get("added-records", summary.get("deleted-records", "—"))
        lines.append(
            f"{s['snapshot-id']:<22} {ts.strftime('%Y-%m-%d %H:%M:%S'):<22} "
            f"{summary.get('operation', '—'):<12} {delta}"
        )

    latest = snapshots[0]
    ts_latest = datetime.datetime.fromtimestamp(
        latest["timestamp-ms"] / 1000, tz=datetime.timezone.utc
    )
    lines.append("")
    lines.append("Time-travel syntax (StarRocks):")
    lines.append(f"  FOR VERSION AS OF {latest['snapshot-id']}")
    lines.append(f"  FOR TIMESTAMP AS OF '{ts_latest.strftime('%Y-%m-%d %H:%M:%S')}'")
    return "\n".join(lines)


# --- Entry point ----------------------------------------------------------
def main() -> None:
    """Console-script entry point — declared in pyproject.toml as
    `lakekeeper-mcp`. Starts the JSON-RPC loop over stdin/stdout.
    """
    mcp.run()


if __name__ == "__main__":
    main()
