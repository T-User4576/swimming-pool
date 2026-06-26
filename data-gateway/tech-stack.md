# Data-Gateway — Technologie-Empfehlung

Begleitdokument zu [`./konzept.md`](./konzept.md). Hier: *womit* das Gateway
gebaut werden sollte und wie es sich in den bestehenden Stack einfügt.

> **Implementierung:** Schwester-Repo [`../../fusion-steward/`](../../fusion-steward/)
> (Fusion-Plattform-Modul). Dieses Dokument ist Design-SoT, kein Code.

---

## 1. Empfehlung: Go-Upstream-Service (Control Plane)

Ein eigener **Go-Control-Plane-Service**, kein generisches API-Gateway-Produkt
als Kern. Im Fusion-Kontext ist er **kein eigener BFF**, sondern ein Upstream
hinter `fusion-bff` (siehe [`./konzept.md`](./konzept.md) Abschnitt 11) —
Browser-OIDC/Session macht fusion-bff, nicht dieser Service.

### Warum Go (und nicht das Repo-Python)

Das vorhandene Python im Repo (Token-Interceptor, MCP-Server, Spark-Jobs) ist
kleiner **Glue-Code**. Das Gateway ist dagegen ein **eigenständiger, langlebiger
Service**, der betrieben und geownt wird — dafür zählt **Team-/Ops-Fluency** mehr
als das Matchen der Glue-Skripte.

- **K8s-/Ops-Footprint (Hauptargument)**: statisches Binary, `distroless`/
  `scratch`-Image, schneller Cold-Start. **restricted PSS** (non-root, RO-FS,
  caps drop) ist quasi geschenkt — passt zu den PSS-Constraints, die schon beim
  `lakekeeper-interceptor` gelten.
- **Fachlogik, die kein Off-the-shelf-Gateway kann**: Contract-Durchsetzung
  (Iceberg-`TBLPROPERTIES`, Schema-/Naming-Regeln) und Provisioning sind
  domänenspezifisch. Kong/APISIX/Ingress können Routing, TLS, Rate-Limiting —
  aber **keine** Iceberg-Tabelle gegen einen Data-Contract prüfen.
- **I/O-Profil**: Goroutines passen zu vielen kleinen Calls gegen Lakekeeper/
  Keycloak (Control-Plane-Last, keine Datenpfad-Last).
- **Bausteine**: **Gin** (Router, Fusion-Konvention), `franz-go` (Kafka/Redpanda),
  `go-playground/validator` plus JSON-Schema/CUE für Contracts (siehe Abschnitt 3).
  `coreos/go-oidc` nur noch, um den von `fusion-bff` weitergereichten Token
  defensiv zu verifizieren — Browser-OIDC + Identitäts-Token-Exchange liegen bei
  fusion-bff (`konzept.md` §11.2).

### Alternative: Quarkus (JVM), enger Zweiter

Wenn JVM bevorzugt wird, ist **Quarkus** die starke Alternative: `quarkus-oidc`
macht die **Keycloak**-Anbindung quasi turnkey (geringster Auth-Boilerplate),
Kafka/Reactive-Messaging ist erstklassig, und GraalVM-Native-Image gibt
Go-ähnlich kleine Images. Trade-off gegen Go: größere Runtime/Build-Komplexität,
dafür weniger manuelle Auth-Verdrahtung. **Python/FastAPI** bleibt nur dritte
Wahl (war primär durch Glue-Konsistenz begründet).

### Abgrenzung zu fertigen Gateways

| Aufgabe | Go-Service | Kong/APISIX/Ingress |
|---|---|---|
| Routing, TLS-Termination | ✓ (oder davorgeschaltet) | ✓ |
| Rate-Limiting, AuthN-Edge | ✓ | ✓ (stark) |
| Iceberg-Contract-Durchsetzung | ✓ | ✗ |
| Provisioning-Logik / DDL | ✓ | ✗ |
| Credential-Broker / Token-Exchange | ✓ | teilweise |

**Empfohlene Kombination**: Go-Service für die Fachlogik; *davor* sitzt im
Fusion-Kontext bereits `fusion-bff` (Edge-AuthN/Session) bzw. ein Ingress für
TLS + Rate-Limiting. Nicht umgekehrt — die Wertschöpfung liegt in der Fachlogik,
nicht im Proxy.

---

## 2. Integration in den bestehenden Stack

| Gegenstelle | Wie |
|---|---|
| **Keycloak (OIDC)** | Browser-OIDC, Session und der **Identitäts-Token-Exchange** (User→scoped Lakekeeper-Token) liegen bei **fusion-bff** (`konzept.md` §11.2). Dieser Service verifiziert nur den weitergereichten Token (`coreos/go-oidc`) und nutzt einen eigenen `client_credentials`-Service-Token für service-eigene Calls. |
| **Lakekeeper (Catalog + Management)** | `net/http`-Client gegen `/catalog/v1/...` (DDL) und `/management/v1/...` (AuthZ-Delegation). Service-Token via `client_credentials`, einmalig bootstrapped (Muster wie Interceptor). |
| **MinIO / S3** | Keine direkten Creds an Kunden — Iceberg-REST **vended credentials** (kurzlebig, scoped) über Lakekeeper anfordern. |
| **StarRocks** | Read-Pfad direkt; Gateway mappt Consumer → `rg_api`/`svc_api` (siehe `../starrocks/sql/resource-groups.sql`). |
| **Audit** | strukturierte JSON-Lines über pluggable `AuditSink`: **stdout** (Default) oder **Kafka/Redpanda** (entkoppelt, von dort in beliebige Senke routebar). Kein hart verdrahtetes Loki/Splunk. |
| **Deployment** | Helm-Chart + **Flux-GitOps** (Fusion-Konvention); distroless/non-root/RO-FS, restricted PSS. Stil-Vorlagen: die Fusion-Module und `../lakekeeper/interceptor/`. |

---

## 3. Modul-/Schnittstellen-Skizze (Konzept, noch kein Code)

Go-Packages (Interfaces als Austausch-Nahtstellen):

```
data-gateway/
  cmd/gateway/        main: Wiring + Server-Start
  internal/
    auth/             AuthorizationProvider (Interface)
                        ├─ LakekeeperAuthzProvider   (Default heute)
                        └─ ExternalRightsProvider    (Platzhalter, später)
    contracts/        ContractEngine: validiert/ergänzt TBLPROPERTIES,
                        Namespace-Naming, Spalten-Schema/Naming-Linter —
                        gegen ein deklaratives Contract-Schema (YAML,
                        JSON-Schema/CUE), Format konsistent mit transform-spec
    catalog/          Lakekeeper-Client (net/http) — DDL + Browse
                        (Browse-Logik analog lakekeeper/mcp)
    credentials/      CredentialBroker: vended credentials (Iceberg-REST) +
                        StarRocks-Session-Mapping (Identitäts-Token-Exchange
                        macht fusion-bff, §11.2)
    audit/            AuditEmitter → AuditSink (stdout | Kafka/Redpanda via
                        franz-go), gleiches Event-Schema auf beiden Sinks
    api/              Gin-Router (kuratiertes Subset, Rate-Limiting)
```

Die Interfaces `AuthorizationProvider` und `CredentialBroker` sind die
**Austausch-Nahtstellen**: Catalog-/Engine-Wechsel oder der spätere externe
Rechte-Dienst werden hier eingehängt, ohne die übrigen Packages zu berühren.

---

## 4. Was bewusst NICHT gewählt wird

- **Kein dbt/SQLMesh** als Gateway — anderes Problem (Transformation, nicht
  Zugriffssteuerung); der Transform-Layer existiert bereits separat.
- **Kein eigenes AuthZ-Framework** (OPA/eigene Policy-Engine) für *Zugriffs*-
  Rechte — das delegiert an Lakekeeper. Für die *Contract*-Validierung reicht ein
  deklaratives Schema (YAML, JSON-Schema/CUE) plus Validator-Lib; eine separate
  Policy-Engine ist Overkill.
- **Nicht Python/FastAPI als Primärwahl** — war nur durch Glue-Konsistenz
  begründet; für einen eigenständigen, betriebenen Service zählt Go-/Ops-Fluency
  mehr (JVM-Alternative: Quarkus, siehe Abschnitt 1).
- **Kein generisches Gateway-Produkt als Kern** — siehe Abschnitt 1.

---

## 5. Nächste Schritte (wenn aus Konzept Umsetzung wird)

1. Service-Account des Service in [`../oidc/`](../oidc/) festlegen; den
   Identitäts-Token-Exchange für die Lakehouse-Route in **fusion-bff** umsetzen
   (scoped Token mit `sub` + `groups`, siehe `konzept.md` §11.2).
2. PoC Credential-Broker: Iceberg vended credentials + StarRocks-Scoping
   verifizieren (das ist die größte offene Mechanik-Frage).
3. ContractEngine gegen ein deklaratives Contract-Schema (YAML, JSON-Schema/CUE)
   aufsetzen — Format konsistent mit `transform-spec`.
4. Helm-Chart + Flux-GitOps anlegen (Fusion-Konvention).
5. Folder in [`../AGENTS.md`](../AGENTS.md) (Abschnitt 3 + eigener Abschnitt 4ff.)
   registrieren.
