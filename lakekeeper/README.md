# Lakekeeper — OpenFGA-Setup + Token-Interceptor

Lakekeeper nutzt **OpenFGA** als Authorizer-Backend (Community-Edition,
Apache-2.0), beide laufen im selben Namespace. Die Zuweisung **Keycloak-
Gruppe → Lakekeeper-Rolle** übernimmt ein separates Helm-Chart
`lakekeeper-interceptor` (siehe [`./interceptor/`](./interceptor/)).

> **Hinweis Lakekeeper+**: Cedar (natives JWT-Gruppen-Mapping ohne Sidecar)
> ist ausschließlich in der kommerziellen Lakekeeper+-Variante verfügbar.
> Konfigurationsreferenz liegt unter `cedar/` (inaktiv).

## Inhalt dieses Ordners

| Pfad | Zweck |
|---|---|
| `openfga/` | Helm-Values + Secret-Vorlage für den OpenFGA-Authorizer |
| `interceptor/` | Eigenständiges Helm-Chart `lakekeeper-interceptor` (nginx + Receiver, Source + Templates + Doku) |
| `values-dev.yaml` | Lakekeeper-Helm-Overrides (reines Catalog-Setup). Prod nutzt aktuell die gleichen Werte — sobald eine echte Divergenz auftaucht, `values-prod.yaml` daneben anlegen. |
| `upgrade.md` | Versions-Upgrade-Vorgehen + Stolpersteine |
| `logging.md` | Audit-Logging der Authorization-Events (`LAKEKEEPER__AUDIT__TRACING__ENABLED`) |
| `ui-preview-extensions.md` | DuckDB-WASM-Extensions, die die UI-Preview braucht (für Offline-/Air-Gap-Mirror) |
| `cedar/` | Inaktiv — Referenz für spätere Lakekeeper+-Evaluierung |

## Architektur (Kurzfassung)

Zwei Pods, zwei Services:

```
   UI/Ingress ──▶ svc/lakekeeper-interceptor:8181
                    │ nginx proxy + Mirror an Receiver
                    ▼
   svc/lakekeeper:8181 ◀── Service-Clients (Spark, StarRocks, …) direkt
```

| Client | Route | Warum |
|---|---|---|
| User-Logins über UI/Ingress | `lakekeeper-interceptor:8181` | Receiver sieht User-Token → Erst-Login bekommt Rolle |
| Service-Clients | `lakekeeper:8181` (direkt) | client_credentials-Token, einmalig per `security_admin` bootstrapped — Mirror hätte nichts zu tun |

Details + Begründung (kein Sidecar im Lakekeeper-Pod, weil hartkodierter
Port-Name kollidiert): [`./interceptor/README.md`](./interceptor/README.md).

## Deployment-Reihenfolge (Erstinstallation)

### 1. Helm-Repos

```bash
helm repo add lakekeeper https://charts.lakekeeper.io
helm repo add openfga    https://openfga.github.io/helm-charts
helm repo update
```

### 2. OpenFGA-Secret + Deploy

```bash
kubectl create secret generic openfga-postgres-uri \
  --namespace lakekeeper \
  --from-literal=OPENFGA_DATASTORE_URI='postgres://openfga:PASSWORD@postgres-host:5432/openfga?sslmode=disable'

helm upgrade --install openfga openfga/openfga \
  --namespace lakekeeper \
  --values lakekeeper/openfga/values.yaml \
  --atomic --wait --timeout 5m
```

### 3. Lakekeeper deployen

```bash
helm upgrade --install lakekeeper lakekeeper/lakekeeper \
  --namespace lakekeeper \
  --reuse-values \
  --values lakekeeper/values-dev.yaml \
  --atomic --wait --timeout 5m
```

### 4. OpenFGA-Modell + Bootstrap

```bash
LAKEKEEPER_POD=$(kubectl get pod -n lakekeeper -l app=lakekeeper -o jsonpath='{.items[0].metadata.name}')
# Distroless-Image: Binary direkt mit vollem Pfad aufrufen (keine Shell).
kubectl exec -n lakekeeper $LAKEKEEPER_POD -- /home/nonroot/lakekeeper migrate
kubectl exec -n lakekeeper $LAKEKEEPER_POD -- /home/nonroot/lakekeeper openfga reconcile --mode add-missing
kubectl exec -n lakekeeper $LAKEKEEPER_POD -- /home/nonroot/lakekeeper reopen-bootstrap --yes
```

Danach im Browser: Lakekeeper-UI aufrufen → als initialer Admin einloggen
→ Bootstrap-Flow abschließen. Dieser User wird der Server-Admin.

### 5. Rollen anlegen

Rollennamen müssen exakt mit den `ROLE_MAPPING_<ROLLE>`-Keys aus
`interceptor/values-*.yaml` übereinstimmen (case-insensitiv).

```bash
ADMIN_TOKEN=$(curl -s -X POST "$KEYCLOAK/realms/lakehouse/protocol/openid-connect/token" \
  -d "grant_type=password&client_id=lakekeeper&username=<server-admin>&password=..." \
  | jq -r .access_token)

for role in ADMIN WORKER; do
  curl -X POST $LAKEKEEPER_URL/management/v1/role \
    -H "Authorization: Bearer $ADMIN_TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"$role\",\"description\":\"...\"}"
done
```

Danach Berechtigungen der Rollen auf Warehouses/Namespaces in der UI
setzen. Die Rollenmitgliedschaft (wer welche Rolle hat) übernimmt der
Interceptor automatisch.

### 6. Interceptor deployen

Siehe **[`./interceptor/README.md`](./interceptor/README.md)** —
Keycloak-Setup, Image-Build, Secret, `security_admin`-Bootstrap und
`helm install` sind dort zusammenhängend dokumentiert.

## Manuelle Rollenzuweisung (Ausnahmen)

User ohne passende Keycloak-Gruppe erhalten keine automatische Rolle.
Zuweisung über die Lakekeeper-UI oder per API:

```bash
ROLE_ID=$(curl -s $LAKEKEEPER_URL/management/v1/role \
  -H "Authorization: Bearer $ADMIN_TOKEN" | jq -r '.roles[] | select(.name=="WORKER") | .id')
USER_ID=$(curl -s "$LAKEKEEPER_URL/management/v1/user?name=alice" \
  -H "Authorization: Bearer $ADMIN_TOKEN" | jq -r '.users[0].id')

curl -X POST $LAKEKEEPER_URL/management/v1/permissions/role/$ROLE_ID/assignments \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"writes\": [{\"user\": \"$USER_ID\", \"type\": \"assignee\"}]}"
```

Manuell zugewiesene Rollen werden vom Interceptor nie überschrieben.

## Rollback

```bash
helm rollback lakekeeper -n lakekeeper                          # Catalog
kubectl rollout undo deployment/lakekeeper-interceptor -n lakekeeper   # Interceptor
```

Beide Komponenten haben getrennten Lifecycle, OpenFGA bleibt laufen.

## Quellen

- [Lakekeeper Docs — Authorization (OpenFGA)](https://docs.lakekeeper.io/docs/latest/authorization-openfga/)
- [Lakekeeper Management OpenAPI Spec](https://github.com/lakekeeper/lakekeeper/blob/main/docs/docs/api/management-open-api.yaml)
- [OpenFGA Helm Chart](https://openfga.github.io/helm-charts) / [Docs](https://openfga.dev/docs)
- [nginx `ngx_http_mirror_module`](https://nginx.org/en/docs/http/ngx_http_mirror_module.html)
