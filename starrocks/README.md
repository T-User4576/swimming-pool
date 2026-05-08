# StarRocks — Lakehouse Serving Layer

Deployment- und Konfigurations-Artefakte für StarRocks als Query-Engine
des Lakehouse. Basis: Helm Chart `starrocks/kube-starrocks` (Operator + CR),
Modus **Shared-Data** mit MinIO/S3 als Storage Backend und Lakekeeper als
Iceberg-Catalog.

Übergeordneter Architektur-Plan: [`../lakehouse-architektur.md`](../lakehouse-architektur.md)

## Profil & Architektur-Entscheidung

- 10–100 TB Hot Layer (Marts + MVs, Sub-Second-Ziel)
- >500 parallele User, >100 QPS
- **Shared-Data Mode** gewählt (statt Shared-Nothing) — Begründung im Master-Plan:
  - 1× Storage in S3, lokale NVMe nur als Cache → kosteneffizient bei großem Hot Layer
  - Compute-/Storage-Skalierung getrennt
  - Multi-Warehouse für Workload-Trennung möglich
  - Konsistent mit Lakehouse-Vision (alles in Object Storage)

## Verzeichnis-Inhalt

```
starrocks/
├── README.md                                    # diese Datei
├── values-prod.yaml                             # Helm Values Produktion
├── values-dev.yaml                              # Helm Values Dev/PoC
├── configmap-fe.yaml                            # FE-Tuning (Concurrency, Iceberg)
├── configmap-cn.yaml                            # CN-Tuning (Datacache, Memory)
├── sql/
│   ├── lakekeeper-catalog.sql                   # External Iceberg Catalog
│   ├── resource-groups.sql                      # Workload-Isolation
│   ├── example-mv.sql                           # Minimal-Beispiel MV
│   ├── mv-patterns.sql                          # 3 MV-Patterns: Tages-Aggregat, Hot-Subset, Hot-Cache
│   └── query-optimization.md                    # Cheatsheet Layout/Indizes/Stats/Anti-Patterns
├── argo/
│   └── mv-orchestration.yaml                    # WorkflowTemplate + CronWorkflow + DAG
└── secrets/
    └── starrocks-s3-credentials.example.yaml    # Secret-Template (NICHT echte Werte)
```

## Caching-Schichten (Performance-Hebel #1)

| Schicht | Wo konfiguriert | Funktion |
|---|---|---|
| Datacache (Disk) | `configmap-cn.yaml` | Block-Cache aus S3 auf lokaler NVMe |
| Datacache (Memory) | `configmap-cn.yaml` | Hot-Tier des Block-Caches im RAM |
| Iceberg Metadata Cache | `configmap-fe.yaml` + `configmap-cn.yaml` | Manifest/Snapshot-Files |
| Page Cache | `configmap-cn.yaml` | In-Memory Block Index |
| Query Cache | per `SET GLOBAL` zur Laufzeit | Result-Cache identischer Queries |
| Materialized Views | `sql/example-mv.sql` | Pre-Aggregation, Query-Rewrite |

Sizing-Faustregel: `(Hot-Layer-Subset / CN-Anzahl) × 1.2 ≤ datacache_disk_size pro Node`.

## Deployment-Reihenfolge

```bash
# 1. Namespace
kubectl create namespace starrocks

# 2. Secret (in Produktion via SealedSecrets / ExternalSecrets, NICHT plain)
kubectl apply -n starrocks -f secrets/starrocks-s3-credentials.yaml

# 3. ConfigMaps
kubectl apply -f configmap-fe.yaml
kubectl apply -f configmap-cn.yaml

# 4. Helm Repo
helm repo add starrocks https://starrocks.github.io/starrocks-kubernetes-operator
helm repo update

# 5. Cluster deployen (Produktion)
helm upgrade --install starrocks starrocks/kube-starrocks \
  -n starrocks \
  -f values-prod.yaml \
  --version <chart-version>

# 6. Cluster-Status pruefen
kubectl get starrockscluster -n starrocks
kubectl get pods -n starrocks

# 7. Initial-SQL ausfuehren (mysql-Client gegen FE :9030)
kubectl port-forward -n starrocks svc/starrocks-fe-service 9030:9030 &
mysql -h 127.0.0.1 -P 9030 -u root < sql/lakekeeper-catalog.sql
mysql -h 127.0.0.1 -P 9030 -u root < sql/resource-groups.sql
```

## Autoscaling

`values-prod.yaml` aktiviert HPA für **CN** (nicht FE — FE-Replicas sind wegen
Raft-Quorum bewusst statisch auf 3).

**Voraussetzungen, sonst greift HPA nicht:**

- `metrics-server` im Cluster aktiv (`kubectl top pods -n starrocks` muss CPU/Mem zeigen)
- K8s-Version ≥ 1.23 (für `autoscaling/v2`)
- CPU- und Memory-Requests sind in `values-prod.yaml` gesetzt — Pflicht für Resource-basierte HPA

**Scale-Down ist deaktiviert** (`selectPolicy: Disabled`). Begründung: ein
schrumpfender CN verliert seinen lokalen Datacache, neue Queries müssen kalt
auf S3 zugreifen. Manuell skaliert wird Off-Peak via `kubectl patch ...`.

Verifikation:
```bash
kubectl get hpa -n starrocks
kubectl describe hpa -n starrocks <hpa-name>
```

## Konfigurations-Hinweis

Die ConfigMaps `configmap-fe.yaml` / `configmap-cn.yaml` **ersetzen** die
Chart-Default-Configs vollständig (das ist Operator-Verhalten bei
`configMapInfo`). Deshalb stehen Standard-Ports, `JAVA_OPTS` und `LOG_DIR`
explizit in unseren Configs — nicht weglassen, auch wenn die StarRocks-Binaries
intern die gleichen Defaults hätten. Operator-Services und Health-Probes
mappen auf diese Werte.

## Validierungs-Checkliste (PoC)

- [ ] `SHOW BACKENDS;` zeigt alle CN-Pods als `alive=true`
- [ ] `SHOW CATALOGS;` enthält `lake` (External, Type=iceberg)
- [ ] `SET CATALOG lake; SHOW DATABASES;` listet Iceberg-Namespaces
- [ ] Test-Query auf einer Mart-Tabelle funktioniert
- [ ] MV (`sql/example-mv.sql`) lässt sich erstellen, Refresh läuft
- [ ] `EXPLAIN <Aggregations-Query>` zeigt MV-Rewrite (`mv_daily_orders` statt Iceberg-Scan)
- [ ] Cache-Hit-Rate steigt bei wiederholter Query (Query-Profile prüfen)
- [ ] Resource Groups greifen — heavy BI-Query darf API-Latenz nicht degradieren
- [ ] FE-Pod-Kill: Cluster bleibt query-fähig (nach kurzem Failover)
- [ ] p95 für Dashboard-Query mit warmem Cache < 1 s

## Offene Punkte (vor Produktions-Roll-out klären)

- Node-Label-Konvention für NVMe-Nodes (`values-prod.yaml` → `starrocks.io/cache=nvme` ist Platzhalter, ggf. anpassen)
- Bucket-Namensschema für Shared-Data (separat vs Sub-Pfad im Lake-Bucket)
- Ingress-Konfiguration für externen MySQL-Protokoll-Zugriff (Port 9030, TLS)
- Backup-Policy für FE-Meta-PVC (Velero oder native `BACKUP`-Statement)
- OIDC-Integration mit Lakekeeper — siehe `../oidc/` (separater Plan)

## Operative Hinweise

- **Image-Tag pinnen**: nie `:latest`, immer explizite Version (`3.3.5` etc.).
- **Statistics manuell triggern** nach großen Mart-Loads:
  `ANALYZE TABLE lake.gold.orders WITH SYNC MODE;`
- **Cache-Aufwärmung** nach CN-Restart: erste Queries gegen Hot-Tabellen
  laufen langsam, bis Datacache gefüllt. Optional Cache-Preload-Job in Argo
  einplanen, falls SLA strikt.
- **Audit Log** in Loki ingesten — sonst geht bei Pod-Recycling Historie verloren.
