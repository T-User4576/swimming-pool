# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "fastmcp>=2.14.0",
#   "httpx>=0.27.0",
# ]
# ///
"""Lakekeeper MCP Server — Iceberg-Catalog-Metadaten für OpenCode.

Stellt inhaltliche Catalog-Discovery bereit (Schema, Kommentare, Partitionierung,
Snapshots) — kein Catalog-Management. Ziel: dem LLM genug Kontext geben, um
effiziente StarRocks-SQL gegen den `lake`-Catalog zu schreiben.

Aufbau der Datei (von oben nach unten):
  1. Konfiguration      — alle Einstellungen kommen aus Umgebungsvariablen.
  2. Authentifizierung  — optionaler OIDC-Token (client_credentials), gecacht.
  3. Catalog-Zugriff    — HTTP-Calls gegen die Lakekeeper-REST-API.
  4. Format-Helfer      — Iceberg-Metadaten in lesbaren Text umwandeln.
  5. Tools              — die vier mit @mcp.tool() dekorierten Funktionen.

Wie FastMCP funktioniert: Jede mit @mcp.tool() dekorierte Funktion wird OpenCode
als aufrufbares Tool angeboten. Der Funktions-Docstring ist die Beschreibung,
die das LLM sieht; die Typ-annotierten Parameter werden zum Eingabe-Schema.
mcp.run() am Dateiende startet die JSON-RPC-Schleife über stdin/stdout. Ein
neues Tool = neue Funktion + @mcp.tool() + aussagekräftiger Docstring; sonst
nichts. Ausführlich: README.md, Abschnitt "Code-Aufbau & eigene Tools".
"""

import datetime
import os
import time
from typing import Any
from urllib.parse import quote

import httpx
from fastmcp import FastMCP

mcp = FastMCP("lakekeeper-catalog")

# --- 1. Konfiguration (alles aus Umgebungsvariablen) -----------------------
LAKEKEEPER_URL = os.environ.get(
    "LAKEKEEPER_URL", "http://lakekeeper.lakekeeper.svc.cluster.local:8181"
).rstrip("/")
WAREHOUSE = os.environ.get("LAKEKEEPER_WAREHOUSE", "main")
KEYCLOAK_TOKEN_URL = os.environ.get("KEYCLOAK_TOKEN_URL", "")
KEYCLOAK_CLIENT_ID = os.environ.get("KEYCLOAK_CLIENT_ID", "")
KEYCLOAK_CLIENT_SECRET = os.environ.get("KEYCLOAK_CLIENT_SECRET", "")

HTTP_TIMEOUT = 15.0

# Prozess-lokale Caches: das Token bis kurz vor Ablauf, die Basis-URL dauerhaft.
_token_cache: dict[str, Any] = {"token": None, "expires_at": 0.0}
_prefix_cache: dict[str, str] = {}


# --- 2. Authentifizierung --------------------------------------------------
def _get_token() -> str | None:
    """client_credentials-Token holen und cachen.

    Ohne gesetztes KEYCLOAK_TOKEN_URL läuft der Server unauthenticated
    (dev-Modus) — dann wird None zurückgegeben.
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
    # 60s Puffer, damit ein Token nicht mitten im Request abläuft.
    _token_cache["expires_at"] = now + data.get("expires_in", 3600) - 60
    return _token_cache["token"]


def _headers() -> dict[str, str]:
    """Authorization-Header, falls OIDC aktiv ist — sonst leeres Dict."""
    token = _get_token()
    return {"Authorization": f"Bearer {token}"} if token else {}


# --- 3. Catalog-Zugriff ----------------------------------------------------
def _catalog_base() -> str:
    """Basis-URL für alle Iceberg-REST-Calls, inklusive Warehouse-Prefix.

    Lakekeeper routet mehrere Warehouses über einen Prefix, den der
    /config-Endpoint in `overrides.prefix` liefert. Einmal ermitteln, cachen.
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
    """'gold.finance' → 'gold\x1ffinance'.

    Iceberg-REST verbindet Namespace-Ebenen mit dem Unit-Separator 0x1F.
    Diese Form ist für Query-Parameter gedacht (httpx encodet die selbst).
    """
    return "\x1F".join(namespace.split("."))


def _ns_path(namespace: str) -> str:
    """Wie _ns_raw, aber zusätzlich URL-encodet — für die Verwendung im URL-Pfad."""
    return quote(_ns_raw(namespace), safe="")


def _get(path: str, params: dict | None = None) -> dict:
    """GET gegen die Catalog-API; `path` ist relativ zu _catalog_base()."""
    resp = httpx.get(
        f"{_catalog_base()}/{path}",
        headers=_headers(),
        params=params or {},
        timeout=HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def _table_metadata(namespace: str, table: str) -> dict:
    """Lädt die vollständigen Iceberg-Metadaten einer Tabelle (LoadTableResult).

    Das zurückgegebene Dict ist das `metadata`-Objekt — siehe README.md,
    Abschnitt "Form der Iceberg-Metadaten", für die wichtigsten Schlüssel.
    """
    result = _get(f"namespaces/{_ns_path(namespace)}/tables/{quote(table, safe='')}")
    return result["metadata"]


# --- 4. Format-Helfer ------------------------------------------------------
def _format_type(t: Any) -> str:
    """Iceberg-Typ in lesbare Form bringen.

    Primitive Typen sind Strings ('long', 'string', ...), verschachtelte
    (struct/list/map) sind Dicts und werden rekursiv aufgelöst.
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
    """Das aktuell gültige Schema einer Tabelle.

    `schemas` ist eine Liste; das aktive wird über `current-schema-id`
    ausgewählt. Toleriert das v1-Format, das nur ein einzelnes `schema` hat.
    """
    schema_id = meta.get("current-schema-id", 0)
    schemas = meta.get("schemas") or [meta.get("schema", {})]
    return next((s for s in schemas if s.get("schema-id") == schema_id), schemas[-1])


# --- 5. Tools (von OpenCode aufrufbar) -------------------------------------
@mcp.tool()
def list_namespaces(parent: str = "") -> str:
    """Listet alle Namespaces im Lakehouse-Catalog (bronze.*, silver.*, gold.*,
    serving.*, mart.*).

    parent: optionaler Top-Level-Namespace (z.B. 'gold'), um nur dessen
    Child-Namespaces zu zeigen. Leer = alle.
    """
    params = {"parent": _ns_raw(parent)} if parent else None
    raw = _get("namespaces", params).get("namespaces", [])
    names = sorted(".".join(levels) for levels in raw)
    return "\n".join(names) if names else "Keine Namespaces gefunden."


@mcp.tool()
def list_tables(namespace: str) -> str:
    """Listet alle Tabellen in einem Namespace.

    namespace: z.B. 'gold.finance' oder 'silver.crm'
    """
    raw = _get(f"namespaces/{_ns_path(namespace)}/tables").get("identifiers", [])
    names = sorted(i["name"] for i in raw)
    return "\n".join(names) if names else f"Keine Tabellen in '{namespace}'."


@mcp.tool()
def describe_table(namespace: str, table: str) -> str:
    """Vollständige Metadaten einer Tabelle: Spalten mit Typen und Kommentaren,
    Partition-Spec, Sort-Order, relevante TBLPROPERTIES, aktueller Snapshot.

    Nutze das, um effiziente StarRocks-SQL zu schreiben: die Partition-Spec
    zeigt, welche WHERE-Bedingungen Partition-Pruning auslösen.

    namespace: z.B. 'gold.finance', table: z.B. 'orders'
    """
    meta = _table_metadata(namespace, table)
    schema = _current_schema(meta)
    fields = schema.get("fields", [])
    # source-id in der Partition-/Sort-Spec verweist auf eine Schema-Feld-ID.
    id_to_field = {f["id"]: f for f in fields}

    lines: list[str] = [f"# {namespace}.{table}"]
    props = meta.get("properties", {})
    if props.get("comment"):
        lines.append(f"Beschreibung: {props['comment']}")
    lines.append(f"Location: {meta.get('location', '—')}")
    lines.append(f"Format-Version: {meta.get('format-version', '—')}")

    lines.append("")
    lines.append("## Schema")
    lines.append(f"{'Spalte':<32} {'Typ':<28} {'Nullable':<9} Kommentar")
    lines.append("─" * 96)
    for f in fields:
        nullable = "nein" if f.get("required") else "ja"
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
        lines.append("## Partitionierung")
        for pf in spec["fields"]:
            src = id_to_field.get(pf["source-id"], {}).get("name", f"id={pf['source-id']}")
            lines.append(f"  {pf['transform']}({src})")
        lines.append("")
        lines.append("  → WHERE-Filter auf diesen Quell-Spalten lösen Partition-Pruning aus.")
        lines.append("  → Ohne passenden Filter werden alle Partitionen gescannt.")
    else:
        lines.append("## Partitionierung: keine (jede Query ist ein Full-Table-Scan)")

    sort_id = meta.get("default-sort-order-id", 0)
    sort = next(
        (s for s in meta.get("sort-orders", []) if s.get("order-id") == sort_id), None
    )
    lines.append("")
    if sort and sort.get("fields"):
        lines.append("## Sort-Order")
        for sf in sort["fields"]:
            src = id_to_field.get(sf["source-id"], {}).get("name", f"id={sf['source-id']}")
            transform = sf.get("transform", "identity")
            col = f"{transform}({src})" if transform != "identity" else src
            lines.append(f"  {col} {sf.get('direction', 'asc')} {sf.get('null-order', '')}".rstrip())
    else:
        lines.append("## Sort-Order: keine")

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
        lines.append("## Relevante Properties")
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
        lines.append("## Aktueller Snapshot")
        lines.append(f"  ID: {current_id}")
        lines.append(f"  Zeitstempel: {ts.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        lines.append(f"  Operation: {summary.get('operation', '—')}")
        if "total-records" in summary:
            lines.append(f"  Zeilen (ca.): {int(summary['total-records']):,}")

    return "\n".join(lines)


@mcp.tool()
def list_snapshots(namespace: str, table: str, limit: int = 10) -> str:
    """Zeigt die letzten Snapshots einer Tabelle — für Time-Travel-Queries.

    namespace: z.B. 'gold.finance', table: z.B. 'orders', limit: max. Anzahl.
    """
    meta = _table_metadata(namespace, table)
    snapshots = sorted(
        meta.get("snapshots", []), key=lambda s: s["timestamp-ms"], reverse=True
    )[:limit]
    if not snapshots:
        return f"Keine Snapshots für '{namespace}.{table}'."

    lines = [f"# Snapshots: {namespace}.{table} (neueste {len(snapshots)})"]
    lines.append(f"{'Snapshot-ID':<22} {'Zeitstempel (UTC)':<22} {'Operation':<12} Zeilen-Delta")
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
    lines.append("Time-Travel-Syntax (StarRocks):")
    lines.append(f"  FOR VERSION AS OF {latest['snapshot-id']}")
    lines.append(f"  FOR TIMESTAMP AS OF '{ts_latest.strftime('%Y-%m-%d %H:%M:%S')}'")
    return "\n".join(lines)


if __name__ == "__main__":
    mcp.run()
