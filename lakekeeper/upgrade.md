# Lakekeeper-Upgrade

Lakekeeper-Upgrades sind in der Regel unkritisch: Postgres-Schema und
OpenFGA-Modell werden über mitgelieferte Subcommands migriert. Die Stolpersteine
sind weniger der Upgrade-Mechanismus als (a) übersehene Breaking-Changes in den
Release-Notes und (b) das distroless-Image, das keine Shell hat.

## Vorgehen (generisch)

1. **Release-Notes von der aktuellen bis zur Zielversion lesen** —
   <https://github.com/lakekeeper/lakekeeper/releases>. Auf `### ⚠ BREAKING CHANGES`
   und `### Features` mit `authz:` / `storage:` / `db:` achten. Mehrere Minor-Sprünge
   auf einmal sind ok, solange jedes BREAKING-Item explizit geprüft ist.
2. **Helm-Chart-Version finden, die die Ziel-App-Version pinnt**:
   ```bash
   helm search repo lakekeeper/lakekeeper --versions | head -10
   ```
   Die Spalte `APP VERSION` zeigt, welche Chart-Version welche Lakekeeper-Version
   ausrollt. Chart-Version ist nicht gleich App-Version.
3. **Im [`kiddie-pool/lakekeeper-helm`](../../kiddie-pool/lakekeeper-helm) testen**:
   Chart-Version im `helm install lakekeeper`-Aufruf pinnen (`--version`),
   `./start.sh` aus `tier1-auth/`, dann das Playbook aus `lakekeeper-helm/README.md`
   abfahren. Alice-Login + Token-Decode danach analog zum Auth-Test.
4. **Postgres-Backup** ziehen (siehe unten) — DB-Migration ist forward-only,
   das ist die einzige Recovery-Option.
5. **Helm-Upgrade in dev**, dann nach 1–2 Tagen Beobachtung in prod. Das Chart
   migriert DB-Schema und OpenFGA-Modell automatisch (siehe unten) — keine
   separaten Migrate-Calls nötig.
6. **Verifikation** (siehe unten).

## Postgres-Backup (vor dem Upgrade)

Postgres hostet **zwei** DBs in derselben Instanz: `lakekeeper` (Catalog-State)
und `openfga` (Authz-Tupel). Beide müssen mit, sonst ist ein Restore halb.

```bash
PG_POD=$(kubectl get pod -n lakekeeper -l app=postgres \
  -o jsonpath='{.items[0].metadata.name}')
TS=$(date -u +%Y%m%dT%H%M%SZ)

# pg_dumpall erfasst beide DBs + Rollen/Permissions in einem Schritt.
# Voraussetzung: der angegebene User ist Superuser (default: gleicher User wie
# POSTGRES_USER aus dem Bootstrap-Secret).
kubectl exec -n lakekeeper $PG_POD -- pg_dumpall -U lakekeeper --clean --if-exists \
  > lakekeeper-pgdumpall-$TS.sql

ls -lh lakekeeper-pgdumpall-$TS.sql   # Sanity-Check: nicht leer
```

`--clean --if-exists` macht den Dump idempotent restorebar (DROP + CREATE pro
Objekt). Datei sicher ablegen (versionierter Storage, S3, o.ä.) — sie ist die
einzige Rollback-Option für Schema-Migrationen.

## Helm-Upgrade

```bash
helm repo update lakekeeper
helm upgrade lakekeeper lakekeeper/lakekeeper \
  --namespace lakekeeper \
  --reuse-values \
  --values lakekeeper/values-dev.yaml \
  --version <chart-version> \
  --atomic --wait --timeout 5m
```

`--atomic` rollt bei Fehler automatisch zurück (greift auch, wenn der
Migrate-Job scheitert). `--wait` blockt bis der `dbMigrations`-Job +
OpenFGA-Hook durch sind. `--reuse-values` ist wichtig, damit existierende
OpenFGA-/OIDC-Settings nicht aus Versehen geleert werden.

## Migration

Das Chart erledigt die Migration **automatisch beim `helm upgrade`** ([offizielle
Doku](https://docs.lakekeeper.io/docs/0.12.x/concepts/)):

- `dbMigrations`-Job migriert das Lakekeeper-Postgres-Schema (Chart-Default
  `enabled: true`).
- OpenFGA-eigene Schema-Updates laufen als init-container des OpenFGA-Pods.
- OpenFGA-Modell-Migration läuft per Helm-Hook
  (`post-install, post-upgrade, post-rollback`).

Mit `helm upgrade … --wait` ist das durch, sobald der Befehl zurückkommt. Manuell
ist nur nötig, wenn man die Auto-Migration im Chart explizit deaktiviert hat oder
zur Fehleranalyse einsteigen will. Erkennungszeichen für vergessene/fehlgeschlagene
Migration:

> Database is not up to date with binary, make sure to run the migrate command before starting the server.

Manueller Migrate-Aufruf — das Image (`quay.io/lakekeeper/catalog`) ist
**distroless** (keine Shell), daher das Binary direkt aufrufen, nicht über `sh -c`:

```bash
LK_POD=$(kubectl get pod -n lakekeeper -l app=lakekeeper \
  -o jsonpath='{.items[0].metadata.name}')

kubectl exec -n lakekeeper $LK_POD -- /home/nonroot/lakekeeper migrate
```

`migrate` ist forward-only, idempotent und läuft in einer Transaktion — schlägt
sie fehl, bleibt die DB unverändert. Gilt auch für andere Subcommands
(`reopen-bootstrap`, `version`, `healthcheck`): immer voller Binary-Pfad.

### OpenFGA reconcile (separate Maintenance)

`reconcile` ist **nicht Teil der Auto-Migration**, sondern eine bewusste
Wartungsoperation — fehlende Tupel im OpenFGA-Modell nachziehen oder verwaiste
löschen:

```bash
kubectl exec -n lakekeeper $LK_POD -- /home/nonroot/lakekeeper openfga reconcile --mode add-missing
```

- **`--mode add-missing`** ist additiv (Default-Wahl, legt fehlende OpenFGA-Tuples an).
- **`--mode delete-stale`** ist destruktiv — kann existierende Permission-Zuweisungen
  entfernen (inkl. `security_admin` am `svc-lakekeeper-sync`). Nur einsetzen, wenn
  die Release-Notes es explizit fordern (z.B. nach Modell-Refactor).

## Worauf achten

- **DB-Migration ist forward-only.** Sobald `migrate` gelaufen ist, kann ein
  alter Server-Binary das Schema nicht mehr lesen. Eine fehlgeschlagene
  Migration rollt sich zwar transaktional zurück, eine **erfolgreiche** nicht —
  Rollback nur über Postgres-Restore aus dem Pre-Upgrade-Backup (siehe
  [Postgres-Backup](#postgres-backup-vor-dem-upgrade) und [Rollback](#rollback)).
- **Cache-Metric-Namen** wurden in v0.12.0 vereinheitlicht (shared names +
  `cache_type`-Label). Wenn Prometheus-Dashboards oder Alerts auf
  `lakekeeper_cache_*` o.ä. zugreifen: PromQL anpassen.
- **Log-Format** ist seit v0.12.0 strukturiert (Objekte statt Strings als
  Values). Wenn Logs an ein zentrales Log-Backend geshippt und dort
  regex-geparst werden: Parser-Regeln gegen das neue Format prüfen.
- **`security_admin`-Zuweisungen am `svc-lakekeeper-sync`** bleiben über
  Upgrades erhalten — das ist eine OpenFGA-Tuple, kein Image-/Code-Belang.
  Falls der Receiver nach Upgrade plötzlich 403-Errors loggt: prüfen, ob die
  Zuweisung versehentlich durch ein `reconcile --mode delete-stale` weggefegt
  wurde.
- **Instance-Admins (seit v0.12.1)**: clusterweite Admins können jetzt direkt
  per Env-Var (`LAKEKEEPER__INSTANCE_ADMINS`) gepinnt werden, statt nur über
  Bootstrap-Flow. Optional — vereinfacht aber den initialen `security_admin`-
  Bootstrap für `svc-lakekeeper-sync` (siehe [`interceptor/README.md`](./interceptor/README.md)
  Voraussetzung 6).
- **Interceptor ist nicht betroffen** — eigenes Deployment + getrennter
  Lifecycle. Receiver-Upgrade läuft separat per `helm upgrade
  lakekeeper-interceptor`, nur bei Änderungen an `interceptor.py` / `nginx.conf`
  / Image-Tag nötig.

## Rollback

### Fall A: Migrate ist NICHT gelaufen

Patch-Release ohne Schema-Änderung, oder `--atomic` hat schon vor dem
Migrate-Job zurückgerollt. Helm reicht aus:

```bash
helm rollback lakekeeper -n lakekeeper
```

### Fall B: Migrate ist gelaufen, DB ist auf neuem Schema

Helm allein reicht **nicht** — der alte Binary kann das neue Schema nicht
lesen ("Database is not up to date with binary…"). Es gibt kein
`lakekeeper migrate --rollback`. Runbook:

```bash
PG_POD=$(kubectl get pod -n lakekeeper -l app=postgres \
  -o jsonpath='{.items[0].metadata.name}')
DUMP=lakekeeper-pgdumpall-<TIMESTAMP>.sql   # die Datei aus dem Pre-Upgrade-Backup

# 1. Traffic & DB-Verbindungen kappen — sonst hält jemand offene Sessions auf
#    den DBs während des Restores.
kubectl scale -n lakekeeper deploy/lakekeeper-interceptor --replicas=0
kubectl scale -n lakekeeper deploy/lakekeeper             --replicas=0
kubectl scale -n lakekeeper deploy/openfga                --replicas=0

# Warten bis alle Pods weg sind.
kubectl wait -n lakekeeper --for=delete pod \
  -l 'app in (lakekeeper, openfga, lakekeeper-interceptor)' --timeout=2m

# 2. DBs aus dem Pre-Upgrade-Dump wiederherstellen. pg_dumpall mit
#    --clean --if-exists droppt und legt neu an — bestehende Daten werden
#    überschrieben (gewollt: das ist genau der Rollback).
kubectl exec -i -n lakekeeper $PG_POD -- psql -U lakekeeper -d postgres \
  < $DUMP

# 3. Helm-Rollback (revertet Image-Tag + Deployment-Spec + ConfigMaps zur
#    vorigen Revision; Replicas gehen automatisch wieder hoch).
helm rollback lakekeeper -n lakekeeper

# 4. Verifikation: alle Pods Ready, Server-Version = alte Version.
kubectl get pod -n lakekeeper
```

Falls Helm-Rollback nicht reicht, weil die `deploy/openfga` von einer
**anderen** Helm-Release verwaltet wird: dort separat rollbacken bzw. Replicas
manuell wieder auf den Sollwert setzen.

## Verifikation nach Upgrade

```bash
# 1. Pod READY?
kubectl get pod -n lakekeeper -l app=lakekeeper

# 2. Server-Info zeigt neue Version?
ADMIN_TOKEN=$(curl -s -X POST "$KEYCLOAK/realms/lakehouse/protocol/openid-connect/token" \
  -d "grant_type=password" -d "client_id=lakekeeper" \
  -d "username=admin" -d "password=..." | jq -r .access_token)
curl -s -H "Authorization: Bearer $ADMIN_TOKEN" \
  $LAKEKEEPER_URL/management/v1/info | jq '.version'

# 3. Existierende Rollen unverändert?
curl -s -H "Authorization: Bearer $ADMIN_TOKEN" \
  $LAKEKEEPER_URL/management/v1/role | jq '.roles[].name'

# 4. Interceptor tut noch was er soll?
kubectl logs -n lakekeeper -l app.kubernetes.io/name=lakekeeper-interceptor \
  -c role-sync --tail=50
```

Wenn 1–4 grün sind und ein Test-User sich noch einloggen + sein Warehouse sehen
kann: durch.
