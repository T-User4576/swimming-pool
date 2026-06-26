In starrocks/configmap-fe.yaml nur das eine Modul ergänzen:

- audit_log_modules = slow_query,query
+ audit_log_modules = slow_query,query,connection

nach Änderungen müssen die FE-Pods einmal neugestartet werden:
```
kubectl rollout restart statefulset -n starrocks -l app.kubernetes.io/component=fe
```

Logs landen in FE-Pod unter /opt/starrocks/fe/log (ist aktull nicht auf einem pv -> bei pod neustart weg)

Es gibt aber wohl ein Plugin, was diese Logs ausliest und in eine Tablle schreibt -> wäre dann also persistiert im s3

# Plugin installieren (über public s3 oder sowas)

# StarRocks Audit-Log

Schritt-für-Schritt: `fe.audit.log` mit Login-Auditing (`connection`) aktivieren
und das **AuditLoader-Plugin** installieren, sodass der Audit-Trail per SQL in
einer StarRocks-Tabelle abfragbar ist — **ohne Internet zur Laufzeit**.

Bezug: StarRocks `3.3.5`, Operator `kube-starrocks` 1.11.4, shared-data gegen
MinIO, Namespace `starrocks`. FE-Config kommt aus `configmap-fe.yaml`.

> **Internet nur einmalig** Nur die Beschaffung der
> `auditloader.zip` braucht Netz. Danach läuft alles cluster-intern (FE-HTTP-Port
> 8030 + interne Artefakt-Ablage in MinIO).

---

## Teil A — `connection`-Modul aktivieren

Login-Events (erfolgreich + fehlgeschlagen) landen erst im `fe.audit.log`, wenn
das `connection`-Modul aktiv ist (seit StarRocks 3.0.6, in 3.3.5 vorhanden).

1. In `configmap-fe.yaml` das Modul ergänzen:

   ```diff
   - audit_log_modules = slow_query,query
   + audit_log_modules = slow_query,query,connection
   ```

   `audit_log_modules` ist **nicht** runtime-mutable (`ADMIN SET FRONTEND CONFIG`
   greift nicht) — FE-Neustart nötig.

2. Anwenden + FE rollen (ConfigMap-Inhaltsänderung triggert keinen Operator-
   Rollout):

   ```bash
   kubectl apply -f starrocks/configmap-fe.yaml
   kubectl rollout restart statefulset -n starrocks \
     -l app.kubernetes.io/component=fe
   ```

3. Prüfen, dass FE sauber startet (und ob `enable_audit_log` als „unknown config"
   gewarnt wird — die Zeile ist vermutlich ein No-op, siehe Stolpersteine):

   ```bash
   FE=$(kubectl get pod -n starrocks -l app.kubernetes.io/component=fe \
     -o jsonpath='{.items[0].metadata.name}')
   kubectl exec -n starrocks $FE -- \
     grep -i "unknown\|audit" /opt/starrocks/fe/log/fe.log | tail
   ```

Ab hier wird `fe.audit.log` mit `queryType=connection`-Zeilen geschrieben. Die
Datei ist aber **ephemer** (liegt unter `/opt/starrocks/fe/log`, nicht auf der
`fe-meta`-PVC) → bei Pod-Recycling weg. Teil B persistiert sie in eine Tabelle.

---

## Teil B — AuditLoader installieren

### Schritt 0 — Artefakt beschaffen

```bash
# Version-matched: Branch/Release passend zu StarRocks 3.3 wählen.
git clone -b branch-3.3 https://github.com/StarRocks/fe-plugins-auditloader.git
cd fe-plugins-auditloader
# Ergebnis: auditloader.jar, plugin.conf, plugin.properties
```

Die Tabellen-Spalten und das Jar sind versionsabhängig — **nicht** eine Zip aus
einer fremden Version nehmen.

### Schritt 1 — `plugin.conf` anpassen + neu packen

```ini
# plugin.conf — runs INSIDE the FE process, so 127.0.0.1 hits the local FE.
frontend_host_port=127.0.0.1:8030
database=starrocks_audit_db__
table=starrocks_audit_tbl__
user=audit_loader
password=<plaintext-or-encrypted>
# Optional tuning:
max_batch_size=50000000
max_batch_interval_sec=60
```

```bash
zip -q -m -r auditloader.zip auditloader.jar plugin.conf plugin.properties
md5sum auditloader.zip          # md5 notieren — wird beim INSTALL gebraucht
```

### Schritt 2 — Artefakt in die Ablage auf MinIO

Die Quelle muss **dauerhaft** erreichbar bleiben (FE greift bei Reload darauf
zurück) und für **alle** FEs identisch sein. Daher **nicht** in den Pod kopieren,
sondern auf MinIO mit anonymem Read auf einem Artefakt-Prefix (kein presigned URL
— der läuft ab):

```bash
# mc gegen euer internes MinIO (dev: minio.minio-dev / prod: minio.minio).
mc mb       local/artifacts
mc cp       auditloader.zip local/artifacts/
mc anonymous set download local/artifacts   # read-only GET, no expiry
```

Erreichbar dann unter
`http://minio.minio.svc.cluster.local:9000/artifacts/auditloader.zip`
(dev: `minio.minio-dev`). Alternativ ein kleiner interner HTTP-Server.

### Schritt 3 — Audit-DB + Tabelle anlegen

Per MySQL-Client gegen den FE-Service (`starrocks-fe-service:9030` —
exakten Namen mit `kubectl get svc -n starrocks` prüfen):

```sql
CREATE DATABASE starrocks_audit_db__;

-- Schema version-matched aus dem AuditLoader-Doc übernehmen (3.3).
-- shared-data: replication_num=1, Durability kommt vom Object Store.
CREATE TABLE starrocks_audit_db__.starrocks_audit_tbl__ (
  -- ... vollständiges Spalten-Set aus der Doku ...
) ENGINE = OLAP
DUPLICATE KEY (`queryId`, `timestamp`, `queryType`)
PARTITION BY date_trunc('day', `timestamp`)
PROPERTIES ("replication_num" = "1", "partition_live_number" = "30");
```

Nach dem Anlegen ~10 min warten, bis die erste dynamische Partition existiert.

### Schritt 4 — Loader-User mit Schreibrecht

```sql
CREATE USER 'audit_loader' IDENTIFIED BY '<password>';
GRANT INSERT ON starrocks_audit_db__.starrocks_audit_tbl__ TO 'audit_loader';
```

User/Passwort müssen mit `plugin.conf` (Schritt 1) übereinstimmen.

### Schritt 5 — Plugin installieren

```sql
INSTALL PLUGIN FROM "http://minio.minio.svc.cluster.local:9000/artifacts/auditloader.zip"
  PROPERTIES("md5sum" = "<md5-aus-schritt-1>");

SHOW PLUGINS;   -- Status von 'AuditLoader' muss INSTALLED sein
```

---

## Teil C — Betrieb

```sql
-- Wer hat sich wann verbunden (connection-Auditing):
SELECT timestamp, user, clientIp, state
FROM   starrocks_audit_db__.starrocks_audit_tbl__
WHERE  queryType = 'connection'
ORDER BY timestamp DESC LIMIT 50;

-- Deinstallation:
UNINSTALL PLUGIN AuditLoader;
```

---

## Stolpersteine

| Punkt | Konsequenz |
|---|---|
| `enable_audit_log` in `configmap-fe.yaml` ist **kein dokumentierter Parameter** | Vermutlich No-op; Audit-Log wird allein über `audit_log_modules` gesteuert. Nach Verifikation (Teil A.3) entfernen. |
| `kubectl logs` zeigt das Audit-Log **nicht** | `fe.audit.log` ist eine Datei, kein stdout → per `kubectl exec … tail` ansehen. |
| Artefakt-Quelle aus dem Netz genommen | Doku verlangt „package must remain at its path after installation" — MinIO-Ablage **dauerhaft** halten, nicht presigned/temporär. |
| Falsche Plugin-Version | Tabellen-Schema/Jar versionsabhängig → Zip immer zu StarRocks 3.3 matchen. |
| Multi-FE (prod) | URL-Install deckt alle FEs identisch ab; lokaler Pfad müsste auf jedem FE liegen → URL-Variante bevorzugen. |

## Quellen

- [StarRocks Docs — AuditLoader](https://docs.starrocks.io/docs/administration/management/audit_loader/)
- [fe-plugins-auditloader (GitHub)](https://github.com/StarRocks/fe-plugins-auditloader)
- [StarRocks Docs — Logs](https://docs.starrocks.io/docs/administration/management/logs/)
