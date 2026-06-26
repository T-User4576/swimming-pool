# Data-Gateway — Zwischenschicht zwischen Frontend und Lakehouse

## Zweck in 5 Zeilen

Eine Control-Plane-Schicht zwischen kundenseitiger UI und dem Lakehouse-Backend
(Lakekeeper-Catalog + StarRocks). Sie kapselt **Provisioning** („Datentöpfe
zusammenklicken"), erzwingt **Data-Contracts**, bündelt **Audit-Logging** an
einer Stelle und gibt die Backend-APIs nur **kuratiert** frei. Das eigentliche
Lesen großer Datenmengen läuft **nicht** durch das Gateway — es brokert nur
kurzlebige, scoped Credentials, der Client liest danach **direkt**.

> **Modul-Name & Repos.** Implementiert wird dies als **`fusion-steward`** —
> ein Go-Control-Plane-Service und Fusion-Plattform-Modul. Dieses Verzeichnis
> (`swimming-pool/data-gateway/`) ist die **Konzept-/Design-SoT** (Deutsch); die
> **Implementierung** liegt im Schwester-Repo
> [`../../fusion-steward/`](../../fusion-steward/). Ziel-Plattform: **Fusion**
> (sitzt hinter `fusion-bff`, UI über `fusion-spectra` — siehe Abschnitt 11).

---

## 1. Motivation & Kontext

Kunden (interne Stakeholder — das System ist laut [`../AGENTS.md`](../AGENTS.md)
Abschnitt 1 **kein echtes Multi-Tenancy**) sollen sich über eine UI eigene
Iceberg-Tabellen anlegen, befüllen und abfragen können. Lakekeeper bietet dafür
eine REST-API, StarRocks ein MySQL-Protokoll. Ein **direkter** Zugriff aus dem
Frontend hätte aber keine Stelle für:

- zentrales, produktneutrales Audit-Logging,
- selektive Freigabe (nur Teile der Backend-APIs sichtbar machen),
- Durchsetzung von Data-Contracts (Pflicht-Properties, Naming-Regeln),
- Entkopplung, damit Catalog oder Query-Engine austauschbar bleiben.

### Reframing: Wo der Lock-in wirklich sitzt

Ein naheliegender, aber falscher Reflex wäre, **alles** hinter eine eigene
proprietäre API zu legen, „um zu entkoppeln". Das ist hier kontraproduktiv:

> **Lakekeeper ist ein offener Iceberg-REST-Catalog-Standard.** Die
> Catalog-Portabilität ist dadurch bereits geschenkt — jede Engine (Spark,
> StarRocks, Flink, Trino, pyiceberg) spricht denselben Standard. Eine eigene
> API davor zu setzen, durch die *alles* läuft, würde diesen Standard verstecken
> und **neuen** Lock-in schaffen — diesmal an das Gateway selbst.

Der echte Lock-in sitzt nicht beim Catalog, sondern bei der **Query-Engine**
(StarRocks-SQL-Dialekt + MySQL-Wire-Protokoll). Dort verdient eine Abstraktion
ihr Geld; beim Catalog reicht es, **auf dem REST-Standard zu bleiben**.

Daraus folgt der zentrale Schnitt dieses Konzepts.

---

## 2. Architekturprinzip: Control Plane vs. Data Plane

> **Standalone-Bild.** Dieser Abschnitt zeigt das Gateway eigenständig (inkl.
> eigenem OIDC, daher „BFF"). Im Fusion-Kontext sitzt es als Upstream **hinter**
> `fusion-bff` und macht **kein** Browser-OIDC — siehe Abschnitt 11.

```
                         ┌─────────────────────────────────────────┐
                         │              Frontend / UI               │
                         │  (Tabellen zusammenklicken, Browsen,     │
                         │   Query absetzen)                        │
                         └───────────────┬─────────────────────────┘
                                         │ OIDC (Keycloak)
                  CONTROL PLANE          │
          ┌──────────────────────────────▼──────────────────────────┐
          │                     DATA-GATEWAY (BFF)                    │
          │  • Self-Service-Provisioning  • Data-Contract-Engine      │
          │  • API-Governance (Subset)    • Audit-Emitter (stdout)    │
          │  • Credential-Broker          • AuthorizationProvider     │
          └───┬───────────────────────────┬───────────────────┬──────┘
              │ Management/Catalog-API     │ delegiert AuthZ    │ brokert
              │ (DDL, Properties)          │                    │ scoped Creds
              ▼                            ▼                    │
   ┌────────────────────┐      ┌────────────────────┐          │
   │     Lakekeeper     │      │  Lakekeeper-Perm.-  │          │
   │  (Iceberg REST)    │      │  /Mgmt-API (heute)  │          │
   └────────────────────┘      └────────────────────┘          │
                                                                │
   ─────────────────────────  DATA PLANE  ──────────────────────┼─────────
                                                                 │
          ┌──────────────────────────────────────────────────┐  │
          │   Client liest/queryt DIREKT — nicht durchs GW    │◀─┘
          │   • StarRocks (MySQL 9030)  • Iceberg-REST/S3     │
          └──────────────────────────────────────────────────┘
```

- **Control Plane** (alles, was *steuert*): Provisioning, Contract-Durchsetzung,
  Governance, Audit, Credential-Ausgabe. Hier lebt das Gateway als eigene API.
  Geringe Datenvolumina, hohe Policy-Dichte → ein BFF ist genau richtig.
- **Data Plane** (das eigentliche *Lesen*): bleibt bewusst **dünn**. Das Gateway
  steht **nicht** im Datenpfad. Es stellt nur kurzlebige, scoped Credentials aus;
  der Client öffnet danach eine direkte Verbindung zu StarRocks bzw. liest
  Iceberg-Daten via vended credentials direkt aus MinIO.

**Warum nicht alles proxien?** Bei riesigen Datenmengen und Sub-Second-SLA (AGENTS.md
Abschnitt 1) würde ein Gateway-Layer im Datenpfad zum Flaschenhals, würde
StarRocks' Datacache/Pipeline aushebeln und das Gateway eng an StarRocks'
Result-Format koppeln — das Gegenteil der gewünschten Entkopplung.

---

## 3. Verantwortlichkeiten

| Das Gateway macht | Das Gateway macht **NICHT** |
|---|---|
| Tabellen/Namespaces anlegen (mit Leitplanken) | Query-Ergebnisse / Nutzdaten durchreichen |
| Pflicht-`TBLPROPERTIES` + Naming erzwingen | Ein zweites Berechtigungssystem betreiben |
| Catalog-Metadaten zum Browsen liefern | Iceberg-REST-Standard hinter proprietärer API verstecken |
| Kurzlebige, scoped Credentials ausstellen | Langlebige Catalog-/S3-Credentials an Kunden geben |
| Audit-Events an einer Stelle nach stdout schreiben | Logs nach Loki/Splunk schicken (siehe `../lakekeeper/logging.md`) |
| Nur kuratiertes API-Subset exponieren | Management-Endpoints roh durchreichen |
| AuthZ an Lakekeeper (bzw. später externen Dienst) delegieren | AuthZ-Entscheidungen selbst treffen |

---

## 4. Datenflüsse

### 4.1 Provisioning-Flow (Write-Pfad, fail-closed)

```
UI ──(1)──▶ Gateway: "Erstelle Tabelle serving.fraud mit Spalten [...]"
            │
            │ (2) Token prüfen — Standalone: Keycloak JWKS; im Fusion-Kontext
            │     kommt der geprüfte, scoped User-Token von fusion-bff (§11.2)
            │ (3) AuthZ: darf User in diesem Namespace anlegen?  ──▶ Lakekeeper-Perm.-API
            │ (4) Contract-Engine prüft + ergänzt:
            │       - Namespace-Naming (bronze/silver/gold/serving/mart.<x>)
            │       - Pflicht-Spalten + Typen + Naming-Linter (z.B. "IP-ADDR")
            │       - Pflicht-TBLPROPERTIES (format-version=2, zstd,
            │         target-file-size, hidden partitioning, metrics.default)
            │ (5) Bei Verstoß → REJECT (fail-closed), Audit-Event "denied"
            ▼
            Lakekeeper Catalog-/Management-API (createTable / setProperties)
            │
            └─(6)─▶ Audit-Event "table.created" → stdout
```

Der Write-Pfad ist bewusst **fail-closed** — anders als der bestehende
Token-Interceptor (`../lakekeeper/interceptor/`), der auf dem Login-Pfad
**fail-open** ist (Verfügbarkeit vor Auto-Assign). Hier gilt das Gegenteil: Kein
Provisioning ohne bestandenen Contract-Check.

### 4.2 Read/Query-Flow (Data Plane bleibt dünn)

```
UI ──(1)──▶ Gateway: "Ich will serving.fraud abfragen"
            │
            │ (2) Token validieren + AuthZ delegieren
            │ (3) Credential-Broker (nutzt den User-Token; im Fusion-Kontext den
            │     von fusion-bff per Token-Exchange gelieferten scoped Token, §11.2):
            │       Variante A (Catalog/S3): Iceberg-REST vended credentials
            │         (kurzlebige S3-Creds, nur dieser Pfad, read-only)
            │       Variante B (StarRocks): scoped StarRocks-Session/User,
            │         gemappt auf Resource-Group rg_api (svc_api)
            ▼
            (4) Gateway gibt Credentials + Endpoint zurück → Audit "creds.issued"
            │
            ▼
UI/Client ──(5)──▶ DIREKT an StarRocks (9030) bzw. Iceberg-REST/MinIO
            (Nutzdaten fließen NIE durch das Gateway)
```

Damit bleibt die Sub-Second-Performance erhalten und das Gateway skaliert
unabhängig vom Datenvolumen.

**Kein Redirect — Out-of-Band-Brokering:** Schritt (4) ist eine **normale
Antwort** mit den eigentlichen Verbindungsdetails (Endpoint + kurzlebiges,
scoped Credential), *kein* HTTP-302-Redirect:

```json
{
  "endpoint": "starrocks.example.com:9030",
  "credential": "<scoped-token>",
  "expires_at": "2026-06-26T12:05:00Z"
}
```

Der Client baut damit eine **eigene, getrennte** Verbindung direkt zu StarRocks
bzw. Iceberg-REST/MinIO auf; das Gateway ist ein Seitenkanal, kein Zwischen-Hop
im Datenpfad. (Ein MySQL-Protokoll-Client folgt ohnehin keinen HTTP-Redirects —
es *muss* Credential-Brokering sein.)

---

## 5. AuthZ-Modell — delegieren, nicht duplizieren

Das Gateway trifft **keine** eigenen Berechtigungsentscheidungen. Es ist ein
**Policy Enforcement Point**, der an die vorhandene Lakekeeper-Permission-/
Management-API delegiert — dieselben Endpoints, die der Token-Interceptor heute
schon nutzt (`/management/v1/permissions/...`, siehe
[`../lakekeeper/interceptor/README.md`](../lakekeeper/interceptor/README.md)).

Gekapselt hinter einer Abstraktion:

```
AuthorizationProvider (Interface)
  ├─ LakekeeperAuthzProvider   ← Default (heute): fragt Lakekeeper-Perm.-API
  └─ ExternalRightsProvider    ← Platzhalter: externer Rechte-/Rollen-Service
                                  (kommt später, Einhängepunkt vorbereitet)
```

- **Heute**: ausschließlich `LakekeeperAuthzProvider`. Nutzt, was mit Lakekeeper
  bereits da ist — kein neues System.
- **Explizit nicht**: direkter OpenFGA-Zugriff. OpenFGA sitzt *innerhalb* von
  Lakekeeper; das Gateway redet nur mit Lakekeeper, nie mit dem Authorizer
  darunter.
- **Später**: ein externer Rechte-/Rollen-Service wird über
  `ExternalRightsProvider` eingehängt, ohne den Rest des Gateways anzufassen.

Der Service-Account des Gateways (z.B. `svc_api` / ein eigener `svc_gateway`)
und sein Token-Lebenszyklus gehören in das geplante [`../oidc/`](../oidc/)-Konzept.
Im Fusion-Kontext liegt der **Identitäts-Token-Exchange** (User → scoped
Lakekeeper-Token) bei `fusion-bff` (Abschnitt 11.2): damit erreicht der User-Token
Lakekeeper, das die Operation selbst autorisiert — das Gateway fragt **nicht**
stellvertretend die Perm-API ab.

---

## 6. Data-Contracts

Die Contract-Engine macht aus den heutigen **Konventionen** (AGENTS.md
Abschnitt 4 & 10) **erzwungene Policy** beim Anlegen/Schreiben. Sie prüft:

### 6.1 Namespace-Naming
Nur `bronze.<source>` / `silver.<domain>` / `gold.<domain>` /
`serving.<consumer>` / `mart.<consumer>` (AGENTS.md Abschnitt 10). Freie
Namespaces werden abgelehnt.

### 6.2 Pflicht-`TBLPROPERTIES`
Beim `createTable` automatisch gesetzt bzw. erzwungen (AGENTS.md Abschnitt 4):

| Property | Wert |
|---|---|
| `format-version` | `2` |
| `write.parquet.compression-codec` | `zstd` |
| `write.target-file-size-bytes` | `536870912` (512 MB) |
| `write.parquet.row-group-size-bytes` | `134217728` (128 MB) |
| `write.metadata.metrics.default` | `truncate(16)` |
| Hidden Partitioning | z.B. `days(event_ts)` — keine explizite Partition-Spalte |

Maintenance-Properties (`maintenance.*.*`) bleiben **Opt-out pro Tabelle** und
werden nicht hart erzwungen — die agnostische Maintenance-Pipeline liest sie zur
Laufzeit (siehe `../iceberg/maintenance.md`).

### 6.3 Schema-Regeln (Spalten)
Pflichtspalten, Typen, Nullability und ein **Naming-Linter** (das Beispiel: eine
IP-Adresse muss überall `IP-ADDR` heißen; ein Zeitstempel `event_ts` etc.),
optional PII-Tagging einzelner Spalten.

**Wiederverwendung statt Neu-Erfindung**: Das Schema-Modell existiert bereits im
Transform-Framework — [`../transform/transform-spec.md`](../transform/transform-spec.md)
mit `mode: validate|enforce`, `columns: [{name, type, nullable}]` und
`on_missing_columns`. Die Contract-Engine sollte dieses Modell teilen, nicht ein
zweites danebenstellen.

### 6.4 Posture
**Fail-closed**: Bei Contract-Verstoß wird das Provisioning abgelehnt und ein
`denied`-Audit-Event geschrieben. Keine „weiche" Warnung auf dem Write-Pfad.

---

## 7. Audit / Logging

Ein einziger **Chokepoint**: Jeder steuernde Call (Provisioning, Credential-
Ausgabe, abgelehnte Contracts) erzeugt ein strukturiertes Audit-Event.

- **Pluggable Senke (`AuditSink`)** — produktneutral. Das Gateway *emittiert*
  Events, kennt aber die finale Log-Senke nicht. Zwei Sinks von Anfang an:
  - **stdout** (Default, simpel): Container-Runtime fängt es ab — konsistent mit
    der Repo-Linie (kein hart verdrahtetes Loki/Splunk, siehe
    [`../lakekeeper/logging.md`](../lakekeeper/logging.md) und
    [`../starrocks/logging.md`](../starrocks/logging.md)).
  - **Kafka / Redpanda** (entkoppelt): Events gehen auf ein Topic, von dem aus
    sie sich später **losgelöst** in eine beliebige Senke routen lassen
    (Loki, Splunk, Elastic, eine Iceberg-Audit-Tabelle, …). Kafka/Redpanda ist
    hier **Transport-/Entkopplungs-Puffer**, kein Analytics-Produkt — das hält
    die Produktneutralität, gibt dir aber freie Senken-Wahl ohne Gateway-Änderung.
- **Strukturiert** (JSON-Lines / Event-Schema), mindestens: `actor` (sub),
  `action`, `resource` (Namespace/Tabelle), `decision` (allow/deny), `reason`,
  `ts` — identisches Schema auf beiden Sinks.
- Das Gateway-Audit ergänzt die bestehenden Audit-Quellen (Lakekeeper-Authz-
  Events, StarRocks-AuditLoader), ersetzt sie nicht.

---

## 8. API-Governance

- **Kuratiertes Subset**: Nur die fürs Self-Service nötigen Operationen werden
  exponiert (Namespaces/Tabellen anlegen & browsen, Credentials anfordern).
  Management-Endpoints (Permissions, Warehouses, Server-Config) bleiben verborgen.
- **Rate-Limiting / Quotas** auf Gateway-Ebene; der eigentliche Query-Workload
  wird über die bestehende StarRocks-**Resource-Group `rg_api`** (User `svc_api`)
  samt Big-Query-Limits begrenzt (siehe `../starrocks/sql/resource-groups.sql`).
- **Logische Consumer-Trennung** (kein echtes Multi-Tenancy): Mapping
  Consumer/Team → Lakekeeper-Namespace(s) + Resource-Group. Falls je harte
  Isolation nötig wird, ist der dokumentierte Trigger die StarRocks-Multi-
  Warehouse-Option (AGENTS.md Abschnitt 5) — nicht das Gateway.

---

## 9. Abgrenzung & Anti-Patterns

| Anti-Pattern | Warum |
|---|---|
| Iceberg-REST-Standard hinter eigener proprietärer API verstecken | Re-introduziert genau den Lock-in, den das Modul vermeiden soll |
| Query-Ergebnisse / Nutzdaten durchs Gateway tunneln | Flaschenhals bei TB-Volumen, killt StarRocks-Datacache/SLA |
| Direkt mit OpenFGA reden | AuthZ gehört zu Lakekeeper; Gateway delegiert, dupliziert nicht |
| Eigenes Berechtigungssystem im Gateway bauen | Zweite Source of Truth → Drift; PEP delegiert an vorhandenes Modell |
| Per-Consumer / per-Tabelle Config-Listen in YAML pflegen | Bricht das Agnostik-Prinzip (AGENTS.md Abschnitt 10) — property-/spec-getrieben bleiben |
| Read-only Metadaten neu implementieren | Überschneidet sich mit `../lakekeeper/mcp/` — gemeinsamen Catalog-Zugriff wiederverwenden |
| Langlebige Catalog-/S3-Creds an Kunden ausgeben | Credential-Broker stellt kurzlebige, scoped Creds aus |
| Write-Pfad fail-open auslegen | Contract-Durchsetzung muss fail-closed sein (Kontrast zum Login-Interceptor) |

---

## 10. Wiederzuverwendende Bausteine im Repo

- [`../lakekeeper/interceptor/interceptor.py`](../lakekeeper/interceptor/interceptor.py)
  — JWKS/Keycloak-Token-Validierung + Lakekeeper-Management-API-Calls
  (Vorlage für `LakekeeperAuthzProvider`).
- [`../lakekeeper/mcp/`](../lakekeeper/mcp/) — httpx-basierter read-only
  Catalog-Zugriff (Namespaces, Schema, Snapshots) für den Browse-Teil.
- [`../transform/transform-spec.md`](../transform/transform-spec.md) —
  Schema-`validate|enforce`-Modell als Basis der Contract-Engine.
- [`../starrocks/sql/resource-groups.sql`](../starrocks/sql/resource-groups.sql)
  — `rg_api`/`svc_api` + Big-Query-Limits für das Quota-Mapping.
- [`../iceberg/table-design.md`](../iceberg/table-design.md) /
  [`../iceberg/maintenance.md`](../iceberg/maintenance.md) — die Property-/
  Naming-Konventionen, die die Contract-Engine erzwingt.

Technologie-Empfehlung und Modul-Aufbau: siehe [`./tech-stack.md`](./tech-stack.md).

---

## 11. Fusion-Plattform-Integration

Dieses Gateway soll perspektivisch Teil der **Fusion-Plattform** werden (Go-
Plattform mit `fusion-spectra`-UI, `fusion-bff`, `fusion-index`, `fusion-forge`,
`fusion-weave`). Das verschiebt die Positionierung und das Auth-Modell gegenüber
den Abschnitten 2 und 5 — die dortige Standalone-Sicht bleibt als Baseline
gültig, wird hier aber für den Fusion-Kontext präzisiert.

### 11.1 Positionierung: Upstream hinter `fusion-bff`, kein eigener BFF
Fusion hat mit **`fusion-bff`** bereits den BFF (Browser-PKCE-OIDC, Server-
Session, RBAC, Reverse-Proxy). Das Data-Gateway (Implementierung:
[`fusion-steward`](../../fusion-steward/)) wird **ein weiterer Upstream**
hinter fusion-bff (analog `fusion-forge`/`-index`/`-weave`), erreichbar z.B. unter
`/api/lakehouse/*`. Es macht **selbst kein** Browser-OIDC/Session — das liegt bei
fusion-bff.

- **UI**: kein eigenes Frontend, sondern der heute leere **„Data"-Kontext in
  `fusion-spectra`** (Vue 3 / Quasar, Module Federation).
- **Abgrenzung**: `fusion-index` ist eine **Binär-Artefakt-Registry** (Modelle,
  venvs, Bundles) — **nicht** der Iceberg-Katalog. Lakekeeper (Iceberg-Tabellen)
  und fusion-index bleiben in UI und API getrennt.

### 11.2 AuthZ-Entscheidung: User-Identität wird durchgereicht (Option a) — **verbindlich**
**Anforderung**: Lakekeeper muss (über den Token-Interceptor) den **echten User**
identifizieren, um Datenzugriff korrekt zu gewähren. Das **erzwingt**, dass am
Lakekeeper ein User-bezogenes Token ankommt (`sub = user` + `groups`-Claim) —
nicht die Service-Identität des Gateways.

```
Browser (User) → fusion-bff  (prüft User-OIDC, Session)
      │  Token-Exchange (RFC 8693) NUR für die Lakehouse-Route:
      │    User-Token → scoped Token  (sub = USER, aud = lakekeeper,
      │    kurze TTL, groups-Claim enthalten)
      ▼
   data-gateway  (reicht den scoped User-Token weiter)
      ▼
   Token-Interceptor → Lakekeeper
   ── Lakekeeper sieht: sub = USER, groups = [User-Gruppen]  ✅
   → Provisioning / vended S3-Credentials werden AS THE USER autorisiert
```

- **Verworfen (Option b)**: Gateway ruft Lakekeeper mit eigener Service-Identität
  (`svc_gateway`) und kennt den User nur aus `X-User-ID`. Dann sieht Lakekeeper
  einen „Roboter", der Interceptor läse die Gruppen des Service statt des Users —
  **per-User-Autorisierung am Katalog wäre unmöglich**. Erfüllt die Anforderung
  nicht.
- **Folge für `fusion-bff`**: bewusste, eng begrenzte **Ausnahme** vom Default
  („BFF strippt das User-Token"). Für die Lakehouse-Route führt fusion-bff einen
  **Token-Exchange** durch; der ausgetauschte Token **muss `sub` *und* `groups`
  tragen** (am Keycloak-Client für die `lakekeeper`-Audience entsprechend
  konfigurieren). Der **Token-Interceptor bleibt unverändert** — er bekommt
  weiter ein User-JWT mit `groups`, genau wofür er gebaut ist.

### 11.3 Zwei AuthZ-Ebenen, klar geschnitten
Die Entscheidung kollidiert **nicht** mit fusion-bffs RBAC, sondern schichtet:

- **`fusion-bff`-RBAC** = grobe Gate (darf der User den Data-Kontext / Endpoint
  überhaupt nutzen) via Permission-Strings wie `data:tables:create`,
  `data:namespaces:read` in der Fusion-`rbac.yaml`. Dies ist konkret der
  **externe Rechte-/Rollen-Service**, an den `ExternalRightsProvider` (Abschnitt 5)
  bindet.
- **Lakekeeper** (mit dem durchgereichten User-Token) = feingranular pro
  Namespace/Tabelle.

---

## 12. Offene Punkte / spätere Ausbaustufen

- **`ExternalRightsProvider`** an die `fusion-bff`-RBAC binden (Permission-Strings
  `data:*` definieren) — siehe Abschnitt 11.3.
- **Token-Exchange in `fusion-bff`** für die Lakehouse-Route umsetzen (scoped
  Token mit `sub` + `groups`); **Service-Account + Token-Lebenszyklus** des
  Gateways im [`../oidc/`](../oidc/)-Konzept verankern (heute leer/geplant).
- **Data-Level-Masking / Row-Filtering** als PEP — die Lücke, die Lakekeepers
  Catalog-Level-AuthZ heute nicht füllt. Frühestens nach Ausbaustufe 1.
- **Konkrete Credential-Broker-Mechanik** für StarRocks (scoped Session vs.
  ephemerer User) im PoC verifizieren.
- **Deployment-Artefakte** (Helm-Chart, K8s-Manifeste) — noch nicht angelegt;
  Stil-Vorlage ist das `lakekeeper-interceptor`-Chart.
