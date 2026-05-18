# Lakekeeper MCP Server — Offline-Setup

Eigener MCP-Server, der OpenCode inhaltliche Catalog-Metadaten aus Lakekeeper
liefert: welche Tabellen es gibt, wie sie aussehen (Schema, Typen, Kommentare),
wie sie partitioniert sind und welche Snapshots existieren. Ziel: dem LLM genug
Kontext für effiziente StarRocks-SQL geben.

Der Server ist ein einzelnes Skript (`server.py`) mit PEP-723-Inline-Dependencies
— kein `pyproject.toml`, kein manuelles venv. `uv` liest die Dependencies aus dem
`# /// script`-Block am Dateianfang.

## Tools

| Tool | Parameter | Zweck |
|---|---|---|
| `list_namespaces` | `parent` (optional) | Alle Namespaces, optional Child-Ebene |
| `list_tables` | `namespace` | Tabellen in einem Namespace |
| `describe_table` | `namespace`, `table` | Schema + Kommentare, Partition-Spec, Sort-Order, Properties, aktueller Snapshot |
| `list_snapshots` | `namespace`, `table`, `limit` | Snapshot-Historie + Time-Travel-Syntax |

---

## 1. Dependencies herunterladen (einmalig, Online-Maschine)

```bash
pip download fastmcp httpx --dest ./wheels
```

Lädt `fastmcp` + `httpx` + transitive Dependencies als `.whl`-Dateien.

---

## 2. Wheels in Nexus hochladen (einmalig)

Voraussetzung: Nexus-Repository vom Typ `pypi (hosted)`, z.B. `pypi-intern`.

```bash
for f in ./wheels/*.whl; do
  curl -u admin:PASSWORD \
    -X POST "http://nexus:8081/service/rest/v1/components?repository=pypi-intern" \
    -F "pypi.asset=@$f;type=application/octet-stream"
done
```

Falls `uv` bereits für den StarRocks-MCP eingerichtet wurde (`starrocks/mcp/README.md`),
sind `fastmcp` und `httpx` ggf. schon teilweise in Nexus — doppelte Uploads sind
unkritisch, Nexus dedupliziert.

---

## 3. uv konfigurieren (einmalig, pro Maschine)

Identisch zum StarRocks-MCP — `~/.config/uv/uv.toml`:

```toml
[[index]]
url = "http://nexus:8081/repository/pypi-intern/simple/"
default = true

[pip]
trusted-host = ["nexus:8081"]
```

Verbindungstest (lädt Dependencies aus Nexus, danach offline lauffähig):

```bash
uv run lakekeeper/mcp/server.py --help
```

---

## 4. opencode.json

```jsonc
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "lakekeeper": {
      "type": "local",
      "command": ["uv", "run", "/absoluter/pfad/zu/lakekeeper/mcp/server.py"],
      "enabled": true,
      "environment": {
        "LAKEKEEPER_URL": "http://lakekeeper.lakekeeper.svc.cluster.local:8181",
        "LAKEKEEPER_WAREHOUSE": "main",
        "KEYCLOAK_TOKEN_URL": "",
        "KEYCLOAK_CLIENT_ID": "",
        "KEYCLOAK_CLIENT_SECRET": ""
      }
    }
  }
}
```

`command` braucht einen **absoluten Pfad** zu `server.py` — OpenCode startet den
Prozess nicht zwingend aus dem Repo-Root.

### Authentifizierung

- **Dev / OIDC deaktiviert**: `KEYCLOAK_*`-Variablen leer lassen → Server läuft
  unauthenticated.
- **Prod / OIDC aktiv**: alle drei `KEYCLOAK_*`-Variablen setzen. Der Server holt
  per `client_credentials`-Flow ein Token und cached es. Empfohlen: eigener
  Keycloak-Client `svc-opencode-mcp` mit Lesezugriff, **nicht** der Sync-Client.

```
KEYCLOAK_TOKEN_URL   https://keycloak.example.com/realms/lakehouse/protocol/openid-connect/token
KEYCLOAK_CLIENT_ID   svc-opencode-mcp
KEYCLOAK_CLIENT_SECRET  <secret>
```

---

## 5. Verifikation

```bash
# Lakekeeper aus dem Cluster erreichbar machen
kubectl port-forward svc/lakekeeper 8181:8181 -n lakekeeper

# Server gegen localhost testen
LAKEKEEPER_URL=http://localhost:8181 uv run lakekeeper/mcp/server.py
```

In OpenCode danach `list_namespaces` aufrufen — erwartete Ausgabe sind die
Namespaces `bronze.*`, `silver.*`, `gold.*`, `serving.*`, `mart.*`.

---

## Code-Aufbau & eigene Tools hinzufügen

`server.py` ist in fünf Teile gegliedert (von oben nach unten, jeweils mit einem
`# --- N. … ---`-Kommentar markiert):

1. **Konfiguration** — alle Einstellungen kommen aus Umgebungsvariablen
   (`LAKEKEEPER_URL`, `LAKEKEEPER_WAREHOUSE`, `KEYCLOAK_*`).
2. **Authentifizierung** — `_get_token()` holt optional ein OIDC-Token und cached
   es; ohne `KEYCLOAK_TOKEN_URL` läuft der Server unauthenticated.
3. **Catalog-Zugriff** — `_catalog_base()` ermittelt den Warehouse-Prefix,
   `_get()` macht die HTTP-Calls, `_ns_raw`/`_ns_path` kodieren mehrstufige
   Namespaces (`gold.finance` → `gold\x1Ffinance`).
4. **Format-Helfer** — `_format_type()`, `_table_metadata()`, `_current_schema()`
   wandeln die Iceberg-Metadaten in lesbare Form.
5. **Tools** — die vier mit `@mcp.tool()` dekorierten Funktionen.

### Wie ein Tool funktioniert

FastMCP macht jede mit `@mcp.tool()` dekorierte Funktion für OpenCode aufrufbar:

- Der **Funktionsname** wird zum Tool-Namen.
- Der **Docstring** ist die Beschreibung, die das LLM sieht — er entscheidet, ob
  und wie das Tool genutzt wird. Aussagekräftig und präzise formulieren.
- Die **Parameter** (mit Typ-Hints) werden zum Eingabe-Schema. Ein Default-Wert
  macht einen Parameter optional.
- Der **Rückgabewert** (hier immer `str`) geht zurück an das LLM.

`mcp.run()` am Dateiende startet die JSON-RPC-Schleife über stdin/stdout.

### Beispiel: neues Tool hinzufügen

Ein neues Tool braucht nur eine neue Funktion — kein Eingriff in den Rest. Die
vorhandenen Helfer (`_table_metadata`, `_get`, ...) lassen sich wiederverwenden:

```python
@mcp.tool()
def get_table_location(namespace: str, table: str) -> str:
    """Gibt den S3-Pfad (MinIO) zurück, unter dem die Tabelle gespeichert ist.

    namespace: z.B. 'gold.finance', table: z.B. 'orders'
    """
    meta = _table_metadata(namespace, table)
    return meta.get("location", "unbekannt")
```

### Form der Iceberg-Metadaten

`_table_metadata()` liefert das `metadata`-Objekt der Iceberg-REST-Antwort. Die
wichtigsten Schlüssel, wenn du ein eigenes Tool baust:

| Schlüssel | Inhalt |
|---|---|
| `schemas` / `current-schema-id` | Spalten-Liste; jedes Feld hat `name`, `type`, `required`, optional `doc` (= Kommentar) |
| `partition-specs` / `default-spec-id` | Partition-Transforms; `source-id` verweist auf eine Schema-Feld-`id` |
| `sort-orders` / `default-sort-order-id` | Sort-Order, analog zur Partition-Spec |
| `properties` | TBLPROPERTIES als Dict (enthält u.a. `comment`) |
| `snapshots` / `current-snapshot-id` | Snapshot-Liste mit `timestamp-ms` und `summary` |

`schemas`, `partition-specs` und `sort-orders` sind **Listen** — das jeweils
aktive Element wird über die zugehörige `*-id` ausgewählt (Muster: siehe
`_current_schema()` und die `next(...)`-Aufrufe in `describe_table`).

---

## Bezug zum StarRocks-MCP

Dieser Server liefert **Metadaten**, nicht Daten. Für tatsächliche Abfragen den
StarRocks-MCP nutzen (`starrocks/mcp/`). Typischer Ablauf im Chat:

1. `list_namespaces` / `list_tables` — Überblick verschaffen
2. `describe_table` — Schema + Partitionierung verstehen
3. StarRocks-MCP `read_query` — die eigentliche SQL gegen `lake.<namespace>.<table>`

Diesen Ablauf kann man als wiederverwendbaren OpenCode-Slash-Command hinterlegen
— siehe [`opencode-commands.md`](./opencode-commands.md).

Namespaces mit Punkt in StarRocks gequotet ansprechen:

```sql
SELECT * FROM lake.`gold.finance`.orders LIMIT 10;
```
