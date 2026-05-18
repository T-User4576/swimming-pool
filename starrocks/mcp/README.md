# StarRocks MCP Server — Offline-Setup

## 1. Wheels herunterladen (einmalig, Online-Maschine)

Im Repo-Root oder in einem leeren Ordner:

```bash
pip download mcp-server-starrocks --dest ./wheels
```

Lädt `mcp-server-starrocks` + alle transitiven Dependencies als `.whl`-Dateien
in `./wheels/`. Kein Klonen des Repos nötig — das Paket ist auf PyPI veröffentlicht.

---

## 2. Wheels in Nexus hochladen (einmalig)

Voraussetzung: Nexus hat ein Repository vom Typ `pypi (hosted)`, z.B. `pypi-intern`.

```bash
for f in ./wheels/*.whl; do
  curl -u admin:PASSWORD \
    -X POST "http://nexus:8081/service/rest/v1/components?repository=pypi-intern" \
    -F "pypi.asset=@$f;type=application/octet-stream"
done
```

---

## 3. uv konfigurieren (einmalig, pro Maschine)

uv globale Config unter `~/.config/uv/uv.toml`:

```toml
[[index]]
url = "http://nexus:8081/repository/pypi-intern/simple/"
default = true

[pip]
trusted-host = ["nexus:8081"]
```

Damit zieht `uv run --with <paket>` automatisch aus Nexus statt PyPI —
kein zusätzlicher Flag in der opencode.json nötig.

Verbindungstest:

```bash
uv run --with mcp-server-starrocks mcp-server-starrocks --help
```

Beim ersten Aufruf lädt uv die Pakete von Nexus und cached sie lokal.
Danach funktioniert es offline.

---

## 4. opencode.json

```jsonc
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "starrocks": {
      "type": "local",
      "command": ["uv", "run", "--with", "mcp-server-starrocks", "mcp-server-starrocks"],
      "enabled": true,
      "environment": {
        "STARROCKS_HOST": "starrocks-fe.starrocks.svc.cluster.local",
        "STARROCKS_PORT": "9030",
        "STARROCKS_USER": "svc_api",
        "STARROCKS_PASSWORD": "...",
        "STARROCKS_DB": ""
      }
    }
  }
}
```

`STARROCKS_DB` leer lassen → Zugriff auf alle Catalogs inkl. `lake`.

---

## Verfügbare Tools nach dem Start

| Tool | Nutzen |
|---|---|
| `read_query` | SELECT auf allen Iceberg-Schichten via `lake`-Catalog |
| `analyze_query` | EXPLAIN ANALYZE — Query-Performance direkt im Chat |
| `table_overview` | Schema + Row Count + Sample Data in einem Call |
| `db_overview` | Alle Tabellen einer Datenbank zusammengefasst |
| `write_query` | DDL/DML (z.B. External Catalog anlegen) |

Namespaces mit Punkt (z.B. `gold.finance`) als gequoteter DB-Name ansprechen:

```sql
SELECT * FROM lake.`gold.finance`.transactions LIMIT 10;
```
