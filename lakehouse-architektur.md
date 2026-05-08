# Lakehouse-Architektur — Konzeption (Phase 1)

## Context

Aufbau eines unternehmensinternen Lakehouse für Big-Data-Verarbeitung im Bereich
**100 TB – 1 PB Gesamt, mehrere TB/Tag Wachstum**. Endnutzer sind ausschließlich
intern (Analysten/BI, Customer-facing Dashboards für interne Stakeholder, ML/DS
Workloads) — **kein echtes Multi-Tenancy**, was die Architektur deutlich
vereinfacht.

Der bestehende Stack ist tragfähig:

- **Storage**: S3/MinIO (siehe Abschnitt "Storage-Entscheidung" — bewusst zu hinterfragen)
- **Tabellenformat**: Apache Iceberg (im Aufbau)
- **Catalog**: Lakekeeper (REST)
- **Processing**: Spark auf K8s (Batch, dominant) + Flink (Kafka → Iceberg, Realtime)
- **Orchestrierung**: Argo Workflows (K8s-nativ)

Die offenen Fragen, die dieses Konzept klärt:

1. Welche **Query-Engine** für Customer-facing Dashboards bei akzeptabler
   doppelter Datenhaltung?
2. Wie bleibt **Lakekeeper** das zentrale Verwaltungs-Tool, ohne dass der
   Serving-Layer einen eigenen Parallel-Catalog erzwingt?
3. Wie verhindern wir, dass das Lakehouse durch fehlende **Iceberg-Maintenance**
   in 3–6 Monaten unter Small-File- und Snapshot-Bloat zusammenbricht?
4. Ist **MinIO** das richtige Storage-Layer für eure Wachstums-Trajektorie?

---

## Empfehlung in einem Satz

**Iceberg als einheitliches Storage-Format für alle Schichten** (Raw bis Mart),
**Lakekeeper als single Catalog**, **StarRocks statt ClickHouse als Serving-Engine**
(über Iceberg External Catalog an Lakekeeper angebunden, mit Materialized Views
für die Sub-Second-Hot-Schicht), **MinIO als on-prem Storage-Backend**,
**Spark + Argo CronWorkflows für Maintenance**.

---

## Warum StarRocks und nicht ClickHouse

Trotz eurer ClickHouse-Erfahrung empfehle ich für diesen Use Case **StarRocks**:

| Kriterium | StarRocks | ClickHouse |
|---|---|---|
| Iceberg External Catalog (REST/Lakekeeper) | Production-ready, gut dokumentiert | Funktioniert, aber weniger ausgereift; Performance auf Iceberg deutlich schwächer |
| Materialized Views auf Iceberg-Quellen | Native, mit Auto-Refresh, transparent rewrite | Eingeschränkt; meist manueller ETL nötig |
| Iceberg-Writes (Marts zurück in Lake) | Ja (v3.x+) | Nein |
| Single-Catalog-Vision (alles über Lakekeeper sichtbar) | Erfüllbar | Nicht erfüllbar — ClickHouse braucht eigene MergeTree-Tabellen für Performance |
| Sub-Second-Performance auf Hot-Daten | Sehr gut (eigene OLAP-Engine + MV-Rewrite) | Exzellent (aber nur auf nativen MergeTree-Tabellen) |
| Team-Erfahrung | Neu — Lernkurve | Vorhanden |

**Zentrale Begründung**: Eure Anforderung "alles über ein Tool verwaltbar" ist
mit ClickHouse nicht erfüllbar, weil ClickHouse-Performance an seinem eigenen
Storage-Format hängt. StarRocks erlaubt es, Iceberg/Lakekeeper als Source of
Truth zu behalten, mit StarRocks-MVs als optionalen, transparenten Speed-Layer.

**Trade-off, den ihr akzeptieren müsst**: Lernkurve und kleinere Community als
ClickHouse. Wenn das Team-Risiko zu hoch ist, **Alternative**: Trino als
Lakehouse-Query-Engine (perfekte Iceberg-Integration, MVs auch in Iceberg
materialisierbar) — aber ohne Sub-Second-Latenz für Dashboards.

---

## Storage-Backend: MinIO (on-prem) — operative Implikationen

MinIO ist als verbindliches Storage-Backend gesetzt (on-prem, S3-kompatibel).
Folgende Punkte muss das Team auf dem Schirm haben, weil sie die Skalierungs-
und Maintenance-Strategie direkt beeinflussen:

**Lizenz-/Feature-Drift:** MinIO hat 2024/2025 schrittweise Features aus der
Community Edition entfernt (insb. Web Console stark beschnitten) und schiebt
Funktionen wie umfassendes IAM, Multi-Site-Replikation, Lifecycle/Tiering,
Verschlüsselungs-Management Richtung **AIStor (Commercial)**. Bei
Feature-Bedarf jenseits Basics → Lizenz-Diskussion einplanen.

**Operative Verantwortung bei eurer Skala (TB/Tag, 100 TB – 1 PB):**
- Erasure Coding, Disk-Planung, Node-Replacement, Rebalancing — alles
  hauseigen.
- Network wird Bottleneck — TB/Tag heißt 10/25/40 GbE und sauberes
  Networking-Design.
- Kapazitätsplanung diskret: Wachstum erfordert "neue Disks bestellen +
  Cluster expanden", nicht elastisch.
- Small-File-Performance: Iceberg erzeugt viele kleine Metadata-Files. MinIO
  ist hier OK, aber Compaction wird Pflicht (deckt sich mit Maintenance-Punkt
  in `iceberg/maintenance.md`).

---

## Ziel-Architektur

```
                      ┌────────────────────────┐
   Kafka  ──Flink───► │                        │
                      │   MinIO (S3-kompat.)   │
   Quell-DBs ──Spark──►   (Iceberg-Tabellen)   │ ◄── Lakekeeper
   Files     Batch     │   Bronze/Silver/Gold  │     (REST Catalog,
                      │   + Mart-Layer         │      Single Source
                      └───────────┬────────────┘      of Catalog Truth)
                                  │
                  ┌───────────────┼────────────────┐
                  │               │                │
          Spark/PySpark      StarRocks        Trino (optional)
          (ETL, ML,          (Serving:        (Ad-hoc SQL für
           Notebooks)         Iceberg          Power-User)
                              External
                              Catalog +
                              MVs)
                                  │
                          BI-Tools / Dashboards
```

### Komponenten-Verantwortlichkeiten

- **MinIO**: Reines Storage (on-prem, S3-kompatibel). Keine Lifecycle-Magie
  auf Iceberg-Pfaden (sonst zerstört Lifecycle-Policy Snapshot-Konsistenz).
- **Iceberg**: Format für alle Datenschichten — Raw, Bronze, Silver, Gold,
  Marts. Auch aggregierte/serving-orientierte Tabellen bleiben Iceberg, damit
  Lakekeeper alles sieht.
- **Lakekeeper**: Einziger Catalog. Spark, Flink, StarRocks und Trino sprechen
  alle den gleichen REST-Endpoint. Auth/Permissions zentral hier.
- **Flink**: Kafka → Iceberg (Bronze, Append/Upsert). Bleibt strategisch wegen
  echter Streaming-Semantik (Windows, State).
- **Spark on K8s**: Batch-ETL Bronze→Silver→Gold→Mart, ML-Workloads, Ad-hoc
  Notebooks, und Iceberg-Maintenance-Jobs.
- **Argo Workflows**: Orchestriert alle Spark-Jobs (ETL + Maintenance), Flink
  ist long-running Deployment.
- **StarRocks**: Serving-Engine für Dashboards. Liest Iceberg-Marts via
  Lakekeeper-Catalog. Materialized Views für Sub-Second-Hot-Pfad — diese MVs
  sind die einzige bewusst akzeptierte Daten-Duplikation.
- **Trino (optional, später)**: Falls Power-User SQL-Ad-hoc auf dem ganzen Lake
  brauchen, ohne StarRocks zu belasten.

---

## Kritische Bausteine, die JETZT mit eingeplant werden müssen

### 1. Iceberg-Maintenance (höchste Priorität — Risiko #1)

Ohne Maintenance wird das Lakehouse bei mehreren TB/Tag innerhalb von Wochen
unbenutzbar. Aufbau als Argo CronWorkflows, die Spark-Jobs starten:

- `rewriteDataFiles` (Compaction) — pro Tabelle, je nach Schreibfrequenz
  täglich oder mehrmals täglich. Besonders wichtig für Flink-geschriebene
  Tabellen (viele kleine Files).
- `expireSnapshots` — meist 7 Tage Retention reicht; spart Storage massiv.
- `removeOrphanFiles` — wöchentlich, vorsichtig (long lookback, sonst löscht
  es in-flight Writes).
- `rewriteManifests` — bei vielen Partitionen.

Konvention: Maintenance-Jobs werden pro Tabelle/Namespace via Argo-Template
parametrisiert, nicht hand-gepflegt.

### 2. Datenmodellierung & Naming-Konvention

Bevor Tabellen wuchern: Medallion-Layer als Lakekeeper-Namespaces festlegen
(z.B. `bronze.<source>`, `silver.<domain>`, `gold.<domain>`,
`mart.<consumer>`). Wer darf in welchen Layer schreiben? Schema-Evolution-Regeln?

### 3. Materialisierungs-Governance

StarRocks-MVs müssen ein Owner-Modell haben — sonst entstehen Schatten-MVs,
die keiner pflegt. Empfehlung: MVs werden im selben Argo-Workflow definiert
wie die zugrundeliegende Mart-Tabelle, nicht ad hoc in StarRocks angelegt.

### 4. Auth-Konzept

- Lakekeeper: OIDC gegen euren IdP, Permissions auf Namespace-Ebene.
- StarRocks: gleicher IdP, Mapping von Lakekeeper-Permissions auf
  StarRocks-Rollen.
- Nicht zwei voneinander entkoppelte Berechtigungssysteme entstehen lassen.

### 5. Observability

- Query-Performance: StarRocks Audit Log + Spark History Server.
- Catalog-Health: Lakekeeper Metrics (Tabellen-Count, Snapshot-Count pro
  Tabelle).
- Storage: S3-Usage pro Namespace (für Cost-Tracking).
- Pipeline: Argo + Prometheus.

---

## Bewusst nicht enthalten (für spätere Phasen)

- **Data Quality** (Soda, Great Expectations) — wichtig, aber nicht
  Architektur-blockierend.
- **Lineage** (OpenLineage, Marquez) — sobald Pipeline-Komplexität steigt.
- **Feature Store** für ML — kommt mit Use Cases.
- **Disaster Recovery** (Cross-Region Snapshot Replication) — abhängig von
  Compliance/RTO.
- **dbt o.ä. für Transformationen** — sobald Spark-SQL-Anteil dominiert.

---

## Validierung dieses Konzepts (PoC, ~2 Wochen)

1. Eine repräsentative Iceberg-Mart-Tabelle (~10–50 GB) in Lakekeeper anlegen.
2. StarRocks gegen Lakekeeper-REST anbinden, External Catalog testen.
3. Eine Materialized View auf der Mart definieren, Refresh testen.
4. Realistische Dashboard-Query gegen direkten Iceberg-Read und gegen MV
   benchmarken — Sub-Second-Ziel verifizieren.
5. Spark `rewriteDataFiles` + `expireSnapshots` auf der Tabelle laufen lassen,
   prüfen dass StarRocks-MV danach korrekt refresht.
6. Auth-Path: Test-User mit Lakekeeper-Permission, prüfen ob StarRocks die
   Permission respektiert.

Wenn 4 und 6 bestehen, trägt die Architektur. Wenn 4 scheitert: Fallback auf
Hybrid (StarRocks-native Tabellen für Hottest-Layer, Iceberg darunter).

---

## Offene Entscheidungen, die ihr noch treffen müsst

- **Schema-Evolution-Policy**: wer darf breaking changes auf Iceberg-Tabellen?
- **MV-Refresh-Strategie**: pull (StarRocks-Scheduled) vs push (von Argo nach
  Mart-Update getriggert)?
- **Trino ja/nein**: jetzt mitdenken oder später nachziehen?
- **ClickHouse vs StarRocks final**: Empfehlung StarRocks, aber Team-Risiko
  (Lernkurve) gegen Eignung abwägen.
