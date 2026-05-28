# Lakekeeper MCP Server — Offline-Setup

Eigener MCP-Server, der OpenCode inhaltliche Catalog-Metadaten aus Lakekeeper
liefert: welche Tabellen es gibt, wie sie aussehen (Schema, Typen, Kommentare),
wie sie partitioniert sind und welche Snapshots existieren. Ziel: dem LLM genug
Kontext für effiziente StarRocks-SQL geben.

Der Server ist als installierbares Python-Paket `lakekeeper-mcp` (Hatchling-Backend)
gepackaged und stellt ein Console-Script `lakekeeper-mcp` bereit. Im Agent-Setup
reicht damit `uvx lakekeeper-mcp` — uv löst die Dependencies aus dem Nexus-Index
auf, installiert in ein ephemerales venv und startet den Server.

## Tools

| Tool | Parameter | Zweck |
|---|---|---|
| `list_namespaces` | `parent` (optional) | Alle Namespaces, optional Child-Ebene |
| `list_tables` | `namespace` | Tabellen in einem Namespace |
| `describe_table` | `namespace`, `table` | Schema + Kommentare, Partition-Spec, Sort-Order, Properties, aktueller Snapshot |
| `list_snapshots` | `namespace`, `table`, `limit` | Snapshot-Historie + Time-Travel-Syntax |

## Layout

```
lakekeeper/mcp/
├── pyproject.toml                 # Paket-Metadaten + Build-Config (Hatchling)
├── README.md                      # diese Datei
├── opencode-commands.md           # wiederverwendbare Slash-Commands
└── src/lakekeeper_mcp/
    ├── __init__.py                # Single Source of Truth für die Version
    └── server.py                  # FastMCP-Server + Entry-Point `main()`
```

`pyproject.toml` deklariert:
- **Name** `lakekeeper-mcp`, Dependencies `fastmcp`, `httpx`.
- **Console-Script** `lakekeeper-mcp = "lakekeeper_mcp.server:main"` — das ist
  der Name, den `uvx`, `pipx` oder `pip install` als ausführbares Binary anlegen.
- **Version dynamisch** aus `src/lakekeeper_mcp/__init__.py` — die einzige Stelle,
  an der die Versionsnummer für Release-Bumps gepflegt werden muss.

---

## 1. Wheel bauen (lokal oder im CI)

```bash
cd lakekeeper/mcp
uv build                  # erzeugt dist/lakekeeper_mcp-<version>-py3-none-any.whl
                          # + dist/lakekeeper_mcp-<version>.tar.gz
```

`uv build` braucht keine vorinstallierten Build-Tools — uv holt sich Hatchling
selber in ein ephemerales Build-venv.

Für lokale Sanity-Checks (ohne Nexus) reicht danach:

```bash
uvx --from ./dist/lakekeeper_mcp-0.1.0-py3-none-any.whl lakekeeper-mcp
```

---

## 2. Wheel + sdist zu Nexus pushen

Voraussetzung: Nexus-Repository vom Typ `pypi (hosted)`, z.B. `pypi-intern`.

### Variante a) `uv publish` (empfohlen)

```bash
uv publish \
  --publish-url http://nexus:8081/repository/pypi-intern/ \
  --username admin --password "$NEXUS_PASSWORD" \
  dist/*
```

`uv publish` lädt alle Artefakte aus `dist/` zum Index hoch.

### Variante b) Direkt per `curl` (falls `uv publish` blockiert ist)

```bash
for f in dist/*.whl dist/*.tar.gz; do
  curl -u admin:"$NEXUS_PASSWORD" \
    -X POST "http://nexus:8081/service/rest/v1/components?repository=pypi-intern" \
    -F "pypi.asset=@$f;type=application/octet-stream"
done
```

Bei einem Re-Push derselben Versionsnummer lehnt Nexus den Upload ab (gut so).
Für jeden Release `__version__` in `src/lakekeeper_mcp/__init__.py` bumpen.

### Transitive Dependencies vor-mirroren (einmalig)

Damit `uvx lakekeeper-mcp` air-gapped läuft, müssen `fastmcp`, `httpx` und ihre
transitiven Abhängigkeiten ebenfalls in Nexus liegen. Wenn das schon für den
StarRocks-MCP gemacht wurde, ist nichts mehr zu tun — sonst:

```bash
pip download fastmcp httpx --dest ./wheels
for f in ./wheels/*.whl ./wheels/*.tar.gz; do
  curl -u admin:"$NEXUS_PASSWORD" \
    -X POST "http://nexus:8081/service/rest/v1/components?repository=pypi-intern" \
    -F "pypi.asset=@$f;type=application/octet-stream"
done
```

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

Verbindungstest (lädt das Paket inkl. Dependencies aus Nexus):

```bash
uvx lakekeeper-mcp --help        # exit-code 0 + Fastmcp-Hilfe → Setup ok
```

---

## 4. opencode.json

Das Endziel: ein einziger Eintrag, kein hartkodierter Pfad, kein lokales Checkout.

```jsonc
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "lakekeeper": {
      "type": "local",
      "command": ["uvx", "lakekeeper-mcp"],
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

`uvx` cached das Paket nach dem ersten Aufruf in `~/.cache/uv` — nachfolgende
Starts sind schnell und brauchen keinen Netzwerk-Roundtrip mehr.

Optional eine Version pinnen (z.B. in CI-Setups):

```jsonc
"command": ["uvx", "lakekeeper-mcp==0.1.0"]
```

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

# Paket aus Nexus holen und gegen localhost testen
LAKEKEEPER_URL=http://localhost:8181 uvx lakekeeper-mcp
```

In OpenCode danach `list_namespaces` aufrufen — erwartete Ausgabe sind die
Namespaces `bronze.*`, `silver.*`, `gold.*`, `serving.*`, `mart.*`.

---

## 6. Lokale Entwicklung

Während der Code-Änderung interessiert kein gebautes Wheel. Direkt aus dem
Checkout starten — uv installiert das Paket im Editable-Mode in ein lokales venv:

```bash
cd lakekeeper/mcp
uv run lakekeeper-mcp                 # uses [project.scripts] entry point
# oder direkt:
uv run python -m lakekeeper_mcp.server
```

Für einen schnellen Smoke-Test ohne installiertes uv reicht auch:

```bash
uv pip install -e .
lakekeeper-mcp
```

---

## Code-Aufbau & eigene Tools hinzufügen

`src/lakekeeper_mcp/server.py` ist in fünf Teile gegliedert (von oben nach unten,
jeweils mit einem `# --- N. … ---`-Kommentar markiert):

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

Am Ende der Datei steht `main()` — der Entry-Point, auf den `[project.scripts]`
in `pyproject.toml` zeigt. `main()` ruft `mcp.run()` auf und startet die
JSON-RPC-Schleife über stdin/stdout.

### Wie ein Tool funktioniert

FastMCP macht jede mit `@mcp.tool()` dekorierte Funktion für OpenCode aufrufbar:

- Der **Funktionsname** wird zum Tool-Namen.
- Der **Docstring** ist die Beschreibung, die das LLM sieht — er entscheidet, ob
  und wie das Tool genutzt wird. Aussagekräftig und präzise formulieren.
- Die **Parameter** (mit Typ-Hints) werden zum Eingabe-Schema. Ein Default-Wert
  macht einen Parameter optional.
- Der **Rückgabewert** (hier immer `str`) geht zurück an das LLM.

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

Nach jedem nicht-trivialen Eingriff `__version__` in
`src/lakekeeper_mcp/__init__.py` bumpen und neu zu Nexus pushen.

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
