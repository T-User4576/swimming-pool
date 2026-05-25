# Lakekeeper Token-Interceptor

Eigenständiges Helm-Chart, das den Token-Interceptor (nginx + Python-
Receiver) vor dem Lakekeeper-Service deployt. User bekommen beim ersten
Login automatisch ihre Rolle gesetzt — basierend auf der `groups`-Claim
ihres OIDC-Access-Tokens, ohne dass der Receiver am IdP Admin-Rechte
braucht.

```
   UI/Ingress
       │
       ▼
   svc/lakekeeper-interceptor:8181
       │
       ▼
   [nginx] ──proxy──▶ svc/lakekeeper:8181
      │
      └─mirror (Header)──▶ [role-sync] verifiziert JWT,
                                       POST role-assignment
```

Cluster-Service-Clients (Spark, StarRocks, …) sprechen `svc/lakekeeper:8181`
direkt an — `client_credentials`-Tokens brauchen kein Auto-Assign.

**Warum kein Sidecar im Lakekeeper-Pod?** Das offizielle Lakekeeper-Chart
hat hartkodierte Port-Namen (`http`/8181) am Catalog-Container; ein zweiter
Container mit gleichem Port-Namen kollidiert und der Chart-Service zieht
den ersten Treffer = Lakekeeper. Eigenes Deployment → keine Kollision.

## Inhalt

| Pfad | Zweck |
|---|---|
| `Chart.yaml`, `values.yaml`, `values-{dev,prod}.yaml` | Helm-Chart-Definition + Defaults + Overrides |
| `templates/{configmap,deployment,service,ingress}.yaml` + `_helpers.tpl` | Render-Templates |
| `interceptor.py`, `nginx.conf` | Source — Receiver-Code + nginx-Config (Letztere wird vom Chart per `.Files.Get` in die ConfigMap eingebettet) |
| `Dockerfile` | Multi-Stage-Build für das role-sync-Image (non-root UID 10001, deps gebakt) |
| `secret.example.yaml` | Vorlage für das Credentials-Secret (NIE mit echten Werten committen) |
| `.helmignore` | Source-Files raus aus `helm package` |

## Voraussetzungen

### 1. Lakekeeper läuft im selben Namespace

Service heißt `lakekeeper` und hört auf 8181 (Default des offiziellen
Charts). Cross-Namespace siehe [unten](#nginxconf-bearbeiten).

### 2. Keycloak — Service-Account `svc-lakekeeper-sync`

In der Keycloak-Admin-UI (Realm `lakehouse`):

1. **Clients → Create client**
   - Client ID: `svc-lakekeeper-sync`
   - Client authentication: **ON**, Service account roles: **ON**
2. **Credentials** → Client Secret notieren
3. **Client scopes → `<client>-dedicated` → Add mapper → Audience**:
   - Included Client Audience: `lakekeeper`
   - Add to access token: **ON**
   (Sonst lehnt Lakekeeper den Writer-Token ab — fehlende `aud`-Claim.)

**Keine** realm-management-Rollen — der Receiver liest Gruppen direkt aus
dem User-Token.

### 3. Keycloak — Group-Membership-Mapper am UI-Client

Der Interceptor liest die `groups`-Claim aus dem **Access-Token** des
Users (nicht aus dem ID-Token).

**Clients → `lakekeeper` → Client scopes → `lakekeeper-dedicated` → Add
mapper → Group Membership:**
- Name + Token Claim Name: `groups`
- Full group path: **OFF** (`AbteilungIT` statt `/AbteilungIT`)
- **Add to access token: ON** ← der kritische Schalter

Verifizieren: User einloggen, Access-Token decodieren, `groups`-Claim muss
da sein.

### 4. role-sync-Image bauen

Der Receiver läuft unter restricted PodSecurityStandard — `pip install`
zur Boot-Zeit geht nicht. Image vorbacken:

```bash
docker build -t REGISTRY/lakekeeper-role-sync:0.1.0 \
             -f Dockerfile .
docker push REGISTRY/lakekeeper-role-sync:0.1.0
```

Tag in `values-{dev,prod}.yaml` unter `roleSync.image.tag` eintragen.
In prod **nie** `:latest`.

### 5. Credentials-Secret

```bash
kubectl create secret generic lakekeeper-role-sync-credentials \
  --namespace lakekeeper \
  --from-literal=KEYCLOAK_CLIENT_ID='svc-lakekeeper-sync' \
  --from-literal=KEYCLOAK_CLIENT_SECRET='<client-secret-aus-schritt-2>'
```

Beide Keys werden vom Chart per `envFrom` in den role-sync-Container
geladen. Secret-Name muss zu `roleSync.credentialsSecret` in values
passen (Default: `lakekeeper-role-sync-credentials`).

### 6. svc-lakekeeper-sync in Lakekeeper berechtigen

Der Receiver braucht die Project-Permission `security_admin` (Default-
Project), um Rollenzuweisungen schreiben zu dürfen. In v0.12 ist
`security_admin` Project-Level, nicht Server-Level. Henne-Ei: der svc-
User existiert in Lakekeeper erst nach einem expliziten Self-Provisioning-
Call.

> **UI vs. API:** Die Lakekeeper-UI bietet unter **Projects → Default
> Project → Members/Access** wahrscheinlich auch eine Permission-Verwaltung
> — ungetestet. Die curl-Variante unten ist getestet und scriptbar
> ([`kiddie-pool/tier1-auth/bootstrap-roles.sh`](../../../kiddie-pool/tier1-auth/bootstrap-roles.sh)).

```bash
# Server-Admin-Token (User aus dem Bootstrap-Flow)
ADMIN_TOKEN=$(curl -s -X POST "$KEYCLOAK/realms/lakehouse/protocol/openid-connect/token" \
  -d "grant_type=password&client_id=lakekeeper&username=<server-admin>&password=..." \
  | jq -r .access_token)

# svc-Token
SYNC_TOKEN=$(curl -s -X POST "$KEYCLOAK/realms/lakehouse/protocol/openid-connect/token" \
  -d "grant_type=client_credentials&client_id=svc-lakekeeper-sync&client_secret=<secret>" \
  | jq -r .access_token)

# Self-Provisioning (legt svc-User in Lakekeeper an, idempotent)
curl -X POST $LAKEKEEPER_URL/management/v1/user \
  -H "Authorization: Bearer $SYNC_TOKEN" -H "Content-Type: application/json" \
  -d '{"name":"svc-lakekeeper-sync","user-type":"application","update-if-exists":true}'

# svc-User-ID = "oidc~<sub>". sub aus JWT extrahieren (URL-safe base64).
SVC_SUB=$(echo "$SYNC_TOKEN" | cut -d. -f2 | tr '_-' '/+' \
  | { read s; printf '%s%s' "$s" "$(printf '%*s' $((4 - ${#s} % 4)) '' | tr ' ' '=')"; } \
  | base64 -d 2>/dev/null | jq -r .sub)

# security_admin auf Default-Project zuweisen (Admin-Token, nicht svc-Token!)
curl -X POST $LAKEKEEPER_URL/management/v1/permissions/project/assignments \
  -H "Authorization: Bearer $ADMIN_TOKEN" -H "Content-Type: application/json" \
  -d "{\"writes\":[{\"user\":\"oidc~$SVC_SUB\",\"type\":\"security_admin\"}]}"
```

Body-Schema verifiziert gegen Lakekeeper-OpenAPI v0.12.2.

## Deployment

`roleSync.roleMapping` / `roleSync.keycloak.*` / `roleSync.image.tag` /
`ingress.*` in `values-{dev,prod}.yaml` anpassen, dann:

```bash
helm upgrade --install lakekeeper-interceptor ./lakekeeper/interceptor \
  --namespace lakekeeper \
  --values ./lakekeeper/interceptor/values-dev.yaml \
  --atomic --wait --timeout 3m
```

## Verifikation

```bash
kubectl -n lakekeeper get pods -l app.kubernetes.io/name=lakekeeper-interceptor

# Receiver-Log live
kubectl -n lakekeeper logs -l app.kubernetes.io/name=lakekeeper-interceptor \
  -c role-sync --follow
# Erwartete Zeile bei neuem User: "sub=<sub> → Rolle '<role>' zugewiesen"
```

## Ingress

`values.ingress.enabled: true` aktiviert ein Ingress-Objekt mit dem
Interceptor-Service als Backend. Beispiel mit cert-manager:

```yaml
ingress:
  enabled: true
  className: nginx
  annotations:
    cert-manager.io/cluster-issuer: letsencrypt-prod   # oder eigener Issuer-Name
  hosts:
    - host: lakekeeper.example.com
      paths: [{ path: /, pathType: Prefix }]
  tls:
    - secretName: lakekeeper-tls
      hosts: [lakekeeper.example.com]
```

Für namespace-lokale Issuer (interne CA, selbstsigniert):
`cert-manager.io/issuer: <issuer-name>` statt `cluster-issuer`.

**Ingress NIE direkt auf `lakekeeper` setzen** — sonst sieht der Receiver
die User-Token nie.

## nginx.conf bearbeiten

`nginx.conf` wird vom Template per `.Files.Get` in die ConfigMap
eingebettet. Änderungen: Datei editieren → `helm upgrade …` — die
`checksum/nginx-config`-Annotation am Deployment ändert sich, kubelet
rollt den Pod automatisch neu.

**Cross-Namespace-Setup** (Interceptor in anderem Namespace als
Lakekeeper-Service): nginx.conf-Upstream auf den FQDN umschreiben
(`lakekeeper.<lakekeeper-ns>.svc.cluster.local:8181`).

## Image-Update-Flow

1. `interceptor.py` editieren
2. Neues Image mit immutable Tag bauen + pushen
3. `roleSync.image.tag` in `values-{dev,prod}.yaml` bumpen
4. `helm upgrade …`

## kiddie-pool nutzt diese Source-Files mit

`interceptor.py` und `nginx.conf` sind hier die **Source of Truth** —
`kiddie-pool/tier1-auth/start.sh` bindet sie als raw ConfigMap ein (kein
Helm, weil tier1 schnelle Iterations-Loops will und `pip install` zur
Boot-Zeit nutzt).

## Bekannte Constraints

- **Replicas OK, aber kein Sticky-Cache** — bei 2 Replicas kann derselbe
  User initial 2× verarbeitet werden; der zweite Call ist idempotent
  (`user_has_any_role()` → True → noop), kostet aber einen API-Roundtrip.
- **Receiver-Ausfall = kein Auto-Assign**, aber kein Catalog-Outage —
  `mirror` ist fail-open.
- **In-Memory-Cache geht beim Rollout verloren** — User werden danach
  nochmal verifiziert, keine doppelte Rolle.
- **Manuell zugewiesene Rollen werden nie überschrieben** — Receiver
  prüft `user_has_any_role()` vor jeder Zuweisung.
