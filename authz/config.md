# OpenFGA — Konfiguration (Helm-Values)

Chart `openfga/openfga` `0.3.16`. Nur was man kennen sollte; Rest siehe
`helm show values openfga/openfga --version 0.3.16`.

## Muss geändert werden

| Wert | Default | Prod |
|---|---|---|
| `authn.method` | leer = **keine Auth** | `preshared` (Secret via `keysSecret`) oder `oidc` |
| `playground.enabled` | `true` | `false` |
| `http.corsAllowedOrigins` | `["*"]` | `[]` wirkt **nicht** (Server bleibt bei `*`); konkrete Origins setzen oder mit aktiver Auth so lassen |
| `image.tag` / `pullPolicy` | Chart-Default / `Always` | `v1.22.0` / `IfNotPresent` |
| `securityContext`, `podSecurityContext` | leer | non-root, feste UID, RO-FS, caps drop |
| `resources` | leer | setzen (HPA braucht Requests) |

## Skalierung

- `replicaCount` (Default `3`) — fix, wenn kein HPA.
- **HPA vorhanden:** `autoscaling.enabled: true`, `minReplicas`/`maxReplicas`,
  Ziel CPU/Memory in %. Erzeugt `autoscaling/v2`; `replicaCount` wird dann
  ignoriert. Braucht metrics-server und `resources.requests`.
- **Kein PodDisruptionBudget** im Chart → über `extraObjects` ergänzen.

## Datenbank

- `datastore.engine: postgres` + `uriSecret` (Key `uri`) **oder**
  `existingSecret` + `secretKeys` (User/Passwort getrennt).
- `maxOpenConns`/`maxIdleConns`: pro Replica — mit Postgres-Limit abgleichen.
- `datastore.secondary`: optionale Read-Replica.
- Migration läuft als Post-Install-Job → **kein `helm --wait`**.
- `postgresql.enabled`/`mysql.enabled`: mitgelieferte Bitnami-DB — nicht nutzen.

## Achtung

- Der Init-Container wartet mit `groundnuty/k8s-wait-for:v2.0` (Docker Hub,
  `pullPolicy: Always`, läuft als root) auf die Migration → für eigene
  Registry/Air-Gap spiegeln über `initContainer.*`. `datastore.migrations.image`
  wird in 0.3.16 von keinem Template genutzt.
- Kein TLS per Default (`grpc.tls`, `http.tls`) — Preshared Key ginge im Klartext.
- `ingress.enabled` aus lassen; OpenFGA nicht nach außen.
- `extraObjects`: NetworkPolicy + PDB mitausrollen.

## Betrieb

- Metriken: Port `2112`, an; `telemetry.metrics.serviceMonitor.enabled` für Prometheus-Operator.
- Tracing: `telemetry.trace` (OTLP), aus.
- Logs: `log.format: json` (Default) für Log-Shipper.

## Grenzwerte (Defaults meist ok)

| Wert | Default | Hinweis |
|---|---|---|
| `listObjectsDeadline` / `MaxResults` | 3 s / 1000 | danach **unvollständiges** Ergebnis → Entscheidungen per `check` |
| `maxChecksPerBatchCheck` | 50 | |
| `maxTuplesPerWrite` | 100 | relevant für CI-Abgleich |
| `requestTimeout` | 3 s | |
| `checkQueryCache.enabled` | `false` | aus lassen (Konzept §6) |

Nicht anfassen: `allowWriting1_0Models`, `allowEvaluating1_0Models`,
`experimentals`, `profiler.enabled`.
