# Lakekeeper — OIDC-Group-basierte Rollenzuweisung (Cedar-Migration)

Migration des Lakekeeper-Authorizers von **OpenFGA → Cedar**, um die native
OIDC-Group-→-Role-Funktion (`LAKEKEEPER__OPENID_ROLES_CLAIM`) zu nutzen.
Danach erhalten Keycloak-User automatisch Lakekeeper-Rollen anhand ihrer
Keycloak-Gruppenzugehörigkeit, ohne manuelle Zuweisung in der UI.

## Beispiel-Setup (durchgängig in dieser Anleitung)

- **Keycloak-Gruppe `IT-Admin`** → **Lakekeeper-Rolle `IT-Admin`** (Vollzugriff)
- **Keycloak-Default-Gruppe `Viewer`** → **Lakekeeper-Rolle `Viewer`** (read-only)
  Jeder neue User wird in Keycloak automatisch Mitglied dieser Default-Gruppe.

> **Wichtig:** Lakekeeper hat keinen Mapping-Layer — Rollen-Name = Gruppen-Name (1:1).

---

## Inhalt dieses Ordners

| Datei | Zweck |
|---|---|
| `cedar/policies.cedar` | Cedar-Policies (Source of Truth) |
| `cedar/configmap.yaml` | K8s-ConfigMap mit denselben Policies inline |
| `values-dev.yaml` | Helm-Override-Fragment für dev |
| `values-prod.yaml` | Helm-Override-Fragment für prod |

Die `values-*.yaml`-Files sind **Fragmente**. Vor `helm upgrade` mit den
aktuellen Cluster-Werten mergen (siehe Phase B).

---

## Voraussetzungen

- Lakekeeper **≥ v0.12.1** (für Authorizer-Switch via Reconcile, [PR #1733](https://github.com/lakekeeper/lakekeeper/pull/1733))
- `kubectl` + `helm` mit Kontext auf den jeweiligen Cluster (dev bzw. prod)
- Zugriff auf Keycloak-Admin-Konsole im Realm `lakehouse`
- Admin-Token für die Lakekeeper-Management-API
- `jq`, `curl` lokal verfügbar

---

## Phase A — Lokale Vorbereitung ✅ erledigt

Files unter `lakekeeper/cedar/` und `lakekeeper/values-*.yaml` liegen bereits
im Repo. Vor Phase B prüfen, ob die Policies dem Schema deiner Lakekeeper-
Version entsprechen (`GET /cedar-schema` nach Phase B verfügbar).

---

## Phase B — Deployment in dev ⚠️

### B0. Postgres-Backup

```bash
kubectl exec -n lakekeeper lakekeeper-postgres-0 -- \
  pg_dump -Fc lakekeeper > backup-pre-cedar-dev-$(date +%F).dump
```

### B1. Aktuelle Cluster-Values ziehen und mergen

```bash
helm get values lakekeeper -n lakekeeper -o yaml > /tmp/current-dev.yaml

# manuell mergen mit lakekeeper/values-dev.yaml
# (oder mit --reuse-values + --set arbeiten, siehe B2)
```

### B2. ConfigMap deployen

```bash
kubectl apply -f lakekeeper/cedar/configmap.yaml
```

### B3. Helm-Upgrade — **erste Cluster-Änderung**

```bash
helm upgrade lakekeeper lakekeeper/lakekeeper \
  --namespace lakekeeper \
  --reuse-values \
  --values lakekeeper/values-dev.yaml \
  --atomic --wait --timeout 5m
```

- `--reuse-values` behält alle bestehenden Werte und überschreibt nur die in
  `values-dev.yaml` definierten Felder.
- `--atomic` rollt bei Fehler automatisch zurück.

### B4. Re-Bootstrap nach Authorizer-Wechsel

```bash
ADMIN_TOKEN=$(curl -s -X POST "$KEYCLOAK/realms/lakehouse/protocol/openid-connect/token" \
  -d "grant_type=password" \
  -d "client_id=lakekeeper" \
  -d "username=admin" \
  -d "password=..." | jq -r .access_token)

curl -X POST $LAKEKEEPER_URL/v1/bootstrap \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"accept-terms-of-use": true}'
```

### B5. Smoke-Test

```bash
kubectl get pods -n lakekeeper                                          # alle Ready?
kubectl logs -n lakekeeper -l app=lakekeeper --tail=50                 # keine Cedar-Errors?
curl $LAKEKEEPER_URL/cedar-schema -H "Authorization: Bearer $ADMIN_TOKEN"  # Schema da?
curl $LAKEKEEPER_URL/management/v1/user -H "Authorization: Bearer $ADMIN_TOKEN" | jq
```

---

## Phase C — Rollen + Keycloak konfigurieren

### C1. Lakekeeper-Rolle `IT-Admin` anlegen

```bash
curl -X POST $LAKEKEEPER_URL/management/v1/role \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "IT-Admin",
    "description": "Auto-assigned from Keycloak group IT-Admin"
  }'
```

### C2. Lakekeeper-Rolle `Viewer` anlegen (Default)

```bash
curl -X POST $LAKEKEEPER_URL/management/v1/role \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Viewer",
    "description": "Default read-only role"
  }'
```

### C3. Keycloak: Group-Membership-Mapper am Lakekeeper-Client

In der Keycloak-Admin-UI:

1. **Clients** → `lakekeeper` → **Client Scopes** → Dedicated Scope
2. **Add Mapper** → **By Configuration** → **Group Membership**
3. Felder setzen:
   - Name: `groups`
   - Token Claim Name: `groups`
   - Full group path: **OFF**
   - Add to ID token: **ON**
   - Add to access token: **ON**

### C4. Keycloak: Realm-Default-Gruppe `Viewer`

1. **Groups** → **Create group** → Name: `Viewer`
2. **Realm Settings** → **User registration** → **Default groups** → `Viewer`
3. Einmalig: bestehende User händisch in `Viewer` aufnehmen.

### C5. Token-Claim verifizieren

```bash
TOKEN=$(curl -s -X POST "$KEYCLOAK/realms/lakehouse/protocol/openid-connect/token" \
  -d "grant_type=password" -d "client_id=lakekeeper" \
  -d "username=testuser" -d "password=..." | jq -r .access_token)

echo $TOKEN | cut -d. -f2 | base64 -d 2>/dev/null | jq .groups
# Erwartung: ["IT-Admin", "Viewer"]
```

---

## Phase D — End-to-End-Test in dev

### D1. Happy Path

1. Testuser `alice` in Keycloak anlegen, Gruppe `IT-Admin` zuweisen.
2. Alice öffnet Lakekeeper-UI → login.
3. Prüfen:
   ```bash
   curl $LAKEKEEPER_URL/management/v1/user/{alice-id}/role \
     -H "Authorization: Bearer $ADMIN_TOKEN" | jq
   ```
   Erwartung: Rollen `IT-Admin` + `Viewer`.

### D2. Default-Pfad

1. Testuser `bob`, **nicht** in `IT-Admin`.
2. Bob loggt sich ein → hat nur Rolle `Viewer`.

### D3. Unhandled-User

User aus `Viewer`-Default-Gruppe rausnehmen → Login → Log-Event:
```bash
kubectl logs -n lakekeeper -l app=lakekeeper --tail=200 | grep unhandled_user
```

### D4. Override-Verhalten

```bash
# Alice die Rolle entziehen
curl -X DELETE $LAKEKEEPER_URL/management/v1/user/{alice-id}/role/IT-Admin \
  -H "Authorization: Bearer $ADMIN_TOKEN"

# Alice neu einloggen lassen
# Prüfen, ob Rolle wieder da ist
```

- **Rolle wieder da** → native Funktion überschreibt; Keycloak = Single Source of Truth.
- **Rolle weg** → manuelle Overrides bleiben; Verwaltung in Lakekeeper möglich.

Beides ist akzeptabel — wichtig ist, das Verhalten zu **kennen** und im Runbook festzuhalten.

### D5. Audit-Logs

```bash
kubectl logs -n lakekeeper -l app=lakekeeper --tail=500 | \
  grep -E "role_assignment|unhandled_user|cedar"
```

---

## Phase E — Prod-Rollout ⚠️

**Erst nach grünem dev-E2E (Phase D).** Identische Reihenfolge:

1. Postgres-Backup der prod-DB
2. ConfigMap in prod-Namespace deployen
3. `helm upgrade --atomic` mit `values-prod.yaml`
4. Re-Bootstrap
5. Smoke-Test mit echtem User
6. Monitoring 24 h: `unhandled_users` / Auth-Fehler

---

## Phase F — Aufräumen (nach 1–2 Wochen)

```bash
# 1. authz.openfga.* aus values-*.yaml entfernen, committen
# 2. helm upgrade ohne reuse-values, mit clean values-*.yaml
# 3. OpenFGA-Workloads abbauen
kubectl delete -n lakekeeper deploy openfga
kubectl delete -n lakekeeper statefulset openfga-postgres   # falls dediziert
```

---

## Rollback

Wenn Phase B/D oder E scheitert:

```bash
helm rollback lakekeeper -n lakekeeper

# Bei DB-Korruption: Backup einspielen
kubectl cp backup-pre-cedar-dev-YYYY-MM-DD.dump \
  lakekeeper/lakekeeper-postgres-0:/tmp/restore.dump
kubectl exec -n lakekeeper lakekeeper-postgres-0 -- \
  pg_restore -d lakekeeper -c /tmp/restore.dump

kubectl rollout restart -n lakekeeper deploy/lakekeeper
```

OpenFGA-Setup ist nach Rollback wieder aktiv. Falls Cedar-Migration langfristig
nicht klappt: Fallback auf Custom-Sync-Job-Variante (eigener K8s-CronJob über
die Mgmt-API).

---

## Quellen

- [PR #1574 — `OPENID_ROLES_CLAIM` (v0.11.2)](https://github.com/lakekeeper/lakekeeper/pull/1574)
- [PR #1625 — Provider-scoped role identifiers (v0.12.1)](https://github.com/lakekeeper/lakekeeper/pull/1625)
- [PR #1733 — Switching Authorizer via reconcile (v0.12.1)](https://github.com/lakekeeper/lakekeeper/pull/1733)
- [Lakekeeper Configuration Docs (main)](https://github.com/lakekeeper/lakekeeper/blob/main/docs/docs/configuration.md)
- [Lakekeeper Authorization-Cedar Docs](https://github.com/lakekeeper/lakekeeper/blob/main/docs/docs/authorization-cedar.md)
