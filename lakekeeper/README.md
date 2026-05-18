# Lakekeeper — OpenFGA-Setup

Lakekeeper nutzt **OpenFGA** als Authorizer-Backend (Community-Edition,
Apache-2.0). OpenFGA läuft als eigenständiger Dienst im selben Namespace.
Ein Python-Sidecar im Lakekeeper-Pod synchronisiert Keycloak-Gruppen
automatisch auf Lakekeeper-Rollen (siehe Abschnitt [Role-Sync Sidecar](#role-sync-sidecar--einmalige-vorbereitung)).

> **Hinweis Lakekeeper+**: Cedar (natives JWT-Gruppen-Mapping ohne Sidecar)
> ist ausschließlich in der kommerziellen Lakekeeper+-Variante verfügbar.
> Konfigurationsreferenz liegt unter `cedar/` (inaktiv).

---

## Inhalt dieses Ordners

| Pfad | Zweck |
|---|---|
| `openfga/values.yaml` | Helm-Values für den OpenFGA-Dienst |
| `openfga/secret.example.yaml` | Secret-Template für PG-URI (nie mit echten Werten committen) |
| `role-sync/sync.py` | Keycloak→Lakekeeper Role-Sync Script (Source of Truth) |
| `role-sync/configmap.yaml` | K8s-ConfigMap mit sync.py inline |
| `role-sync/secret.example.yaml` | Credentials-Template für den Sync-Service-Account |
| `values-dev.yaml` | Lakekeeper-Helm-Override für dev (inkl. Sidecar) |
| `values-prod.yaml` | Lakekeeper-Helm-Override für prod (inkl. Sidecar) |
| `cedar/` | Inaktiv — Referenz für spätere Lakekeeper+-Evaluierung |

---

## Deployment-Reihenfolge (Erstinstallation)

### 1. Helm-Repos

```bash
helm repo add lakekeeper https://charts.lakekeeper.io
helm repo add openfga    https://openfga.github.io/helm-charts
helm repo update
```

### 2. OpenFGA-Secret anlegen

```bash
kubectl create secret generic openfga-postgres-uri \
  --namespace lakekeeper \
  --from-literal=OPENFGA_DATASTORE_URI='postgres://openfga:PASSWORD@postgres-host:5432/openfga?sslmode=disable'
```

### 3. OpenFGA deployen

```bash
helm upgrade --install openfga openfga/openfga \
  --namespace lakekeeper \
  --values lakekeeper/openfga/values.yaml \
  --atomic --wait --timeout 5m
```

Smoke-Test:
```bash
kubectl get pods -n lakekeeper -l app.kubernetes.io/name=openfga
curl http://<openfga-svc>:8080/healthz   # Erwartung: {"status":"SERVING"}
```

### 4. Lakekeeper deployen (noch ohne Sidecar)

Für den Erststart den Sidecar-Block in `values-dev.yaml` temporär auskommentieren,
damit Lakekeeper ohne den Sync-Container hochfährt. Der Sidecar wird erst nach
dem Bootstrapping aktiviert (siehe Abschnitt unten).

```bash
helm upgrade --install lakekeeper lakekeeper/lakekeeper \
  --namespace lakekeeper \
  --reuse-values \
  --values lakekeeper/values-dev.yaml \
  --atomic --wait --timeout 5m
```

### 5. OpenFGA-Autorisierungsmodell initialisieren

```bash
LAKEKEEPER_POD=$(kubectl get pod -n lakekeeper -l app=lakekeeper -o jsonpath='{.items[0].metadata.name}')

kubectl exec -n lakekeeper $LAKEKEEPER_POD -- lakekeeper migrate
kubectl exec -n lakekeeper $LAKEKEEPER_POD -- lakekeeper openfga reconcile --mode add-missing
```

### 6. Bootstrap abschließen

```bash
kubectl exec -n lakekeeper $LAKEKEEPER_POD -- lakekeeper reopen-bootstrap --yes
```

Danach im Browser: Lakekeeper-UI aufrufen → als initialer Admin einloggen →
Bootstrap-Flow abschließen.

---

## Rollen-Verwaltung

### Rollen anlegen (einmalig manuell)

Lakekeeper-Rollen in der UI oder per API anlegen. Die Rollennamen müssen
exakt mit den `ROLE_MAPPING_<ROLLE>`-Keys in den Helm-Values übereinstimmen
(case-insensitiv, z.B. Key `ADMIN` → Rollenname `admin` oder `ADMIN`).

```bash
ADMIN_TOKEN=$(curl -s -X POST "$KEYCLOAK/realms/lakehouse/protocol/openid-connect/token" \
  -d "grant_type=password&client_id=lakekeeper&username=admin&password=..." \
  | jq -r .access_token)

curl -X POST $LAKEKEEPER_URL/management/v1/role \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "ADMIN", "description": "Vollzugriff"}'

curl -X POST $LAKEKEEPER_URL/management/v1/role \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "WORKER", "description": "Lesezugriff gold/serving"}'
```

Danach Berechtigungen der Rollen auf Warehouses/Namespaces in der UI setzen.
Das ist die einmalige manuelle Konfiguration — die Rollenmitgliedschaft
(wer welche Rolle hat) übernimmt danach der Sync-Sidecar.

### Manuelle Rollenzuweisung (Ausnahmen / nicht gemappte User)

User ohne passende Keycloak-Gruppe erhalten keine automatische Rolle.
Zuweisung über die Lakekeeper-UI oder per API:

```bash
# Role-ID ermitteln
ROLE_ID=$(curl -s $LAKEKEEPER_URL/management/v1/role \
  -H "Authorization: Bearer $ADMIN_TOKEN" | jq -r '.roles[] | select(.name=="WORKER") | .id')

# User-ID ermitteln (nach erstem Login des Users)
USER_ID=$(curl -s "$LAKEKEEPER_URL/management/v1/user?name=alice" \
  -H "Authorization: Bearer $ADMIN_TOKEN" | jq -r '.users[0].id')

# Zuweisung schreiben
curl -X POST $LAKEKEEPER_URL/management/v1/permissions/role/$ROLE_ID/assignments \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"writes\": [{\"user\": \"$USER_ID\", \"type\": \"assignee\"}]}"
```

---

## Role-Sync Sidecar — Einmalige Vorbereitung

Der Sidecar synchronisiert Keycloak-Gruppen automatisch auf Lakekeeper-Rollen.
Folgende Schritte sind **einmalig** vor dem ersten Sidecar-Deployment nötig.

### Schritt 1 — Keycloak Service Account anlegen

In der Keycloak-Admin-UI (Realm `lakehouse`):

1. **Clients** → **Create client**
   - Client ID: `svc-lakekeeper-sync`
   - Client authentication: **ON**
   - Service account roles: **ON**
2. **Credentials** → Client Secret notieren
3. **Service Account Roles** → **Assign role** → Filter: `realm-management` →
   Rolle `view-users` zuweisen

### Schritt 2 — K8s Secret anlegen

```bash
kubectl create secret generic lakekeeper-role-sync-credentials \
  --namespace lakekeeper \
  --from-literal=KEYCLOAK_CLIENT_SECRET='<client-secret-aus-schritt-1>'
```

### Schritt 3 — ConfigMap deployen

```bash
kubectl apply -f lakekeeper/role-sync/configmap.yaml
```

### Schritt 4 — Lakekeeper-Rollen anlegen

Sicherstellen, dass die Rollen (z.B. `ADMIN`, `WORKER`) in Lakekeeper existieren,
bevor der Sidecar startet — sonst loggt er ein Warning und tut nichts.
Siehe Abschnitt [Rollen anlegen](#rollen-anlegen-einmalig-manuell) oben.

### Schritt 5 — Service Account in Lakekeeper berechtigen

Der Sidecar braucht die Serverrolle `security_admin` in Lakekeeper, um
Rollenzuweisungen schreiben zu dürfen. Das erzeugt ein Henne-Ei-Problem:
Lakekeeper legt den User erst an, wenn er sich das erste Mal einloggt.

Vorgehen:

```bash
# a) Token des Service Accounts holen
SYNC_TOKEN=$(curl -s -X POST "$KEYCLOAK/realms/lakehouse/protocol/openid-connect/token" \
  -d "grant_type=client_credentials" \
  -d "client_id=svc-lakekeeper-sync" \
  -d "client_secret=<secret>" \
  | jq -r .access_token)

# b) Beliebigen Lakekeeper-API-Call machen → erstellt den User-Eintrag
curl -s $LAKEKEEPER_URL/management/v1/user \
  -H "Authorization: Bearer $SYNC_TOKEN" > /dev/null

# c) User-ID des Service Accounts ermitteln
SYNC_USER_ID=$(curl -s "$LAKEKEEPER_URL/management/v1/user?name=svc-lakekeeper-sync" \
  -H "Authorization: Bearer $ADMIN_TOKEN" | jq -r '.users[0].id')

# d) security_admin auf Server-Ebene zuweisen
curl -X POST $LAKEKEEPER_URL/management/v1/permissions/server/assignments \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"writes\": [{\"user\": \"$SYNC_USER_ID\", \"type\": \"security_admin\"}]}"
```

### Schritt 6 — Mapping konfigurieren und Sidecar aktivieren

In `values-dev.yaml` die `ROLE_MAPPING_*`-Env-Vars anpassen:

```yaml
- name: ROLE_MAPPING_ADMIN
  value: "AbteilungIT,AbteilungCompliance"
- name: ROLE_MAPPING_WORKER
  value: "AbteilungData"
```

Den temporär auskommentierten Sidecar-Block wieder einkommentieren, dann:

```bash
kubectl apply -f lakekeeper/role-sync/configmap.yaml
helm upgrade lakekeeper lakekeeper/lakekeeper \
  --namespace lakekeeper \
  --reuse-values \
  --values lakekeeper/values-dev.yaml \
  --atomic --wait --timeout 5m
```

Sidecar-Log prüfen:
```bash
kubectl logs -n lakekeeper -l app=lakekeeper -c role-sync --follow
```

---

## Rollback

```bash
helm rollback lakekeeper -n lakekeeper
# OpenFGA bleibt laufen; Lakekeeper fällt auf vorherige Values zurück.
```

---

## Quellen

- [Lakekeeper Docs — Authorization (OpenFGA)](https://docs.lakekeeper.io/docs/latest/authorization-openfga/)
- [Lakekeeper Management OpenAPI Spec](https://github.com/lakekeeper/lakekeeper/blob/main/docs/docs/api/management-open-api.yaml)
- [OpenFGA Helm Chart](https://openfga.github.io/helm-charts)
- [OpenFGA Docs](https://openfga.dev/docs)
