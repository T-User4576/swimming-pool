# authz — OpenFGA aufsetzen

Konzept: [`konzept.md`](./konzept.md) · Helm-Values: [`config.md`](./config.md)

| | Version |
|---|---|
| Helm-Chart | `openfga/openfga` `0.3.16` (https://openfga.github.io/helm-charts) |
| Image | `openfga/openfga:v1.22.0` |
| CLI | `fga` `v0.8.1` (https://github.com/openfga/cli/releases) |

## 1. OpenFGA installieren

Voraussetzungen (Details im Kopf von [`values-openfga.yaml`](./values-openfga.yaml)):
Postgres-DB `openfga` + Secret `openfga-postgres-uri`, Secret
`openfga-preshared-keys`, kube-prometheus-stack, metrics-server,
gespiegeltes `k8s-wait-for`-Image.

```bash
helm repo add openfga https://openfga.github.io/helm-charts
helm upgrade --install openfga openfga/openfga --version 0.3.16 \
  --namespace authz --create-namespace \
  --values authz/values-openfga.yaml
```

Kein `--wait`/`--atomic`: Die DB-Migration läuft als Post-Install-Hook,
mit `--wait` blockiert die Installation (siehe `../lakekeeper/openfga/values.yaml`).

## 2. Store und Modell einspielen

```bash
export FGA_API_URL=http://<openfga>:8080
export FGA_API_TOKEN=<preshared key>

fga model test --tests store.fga.yaml            # lokal prüfen
fga store create --name data-service             # einmalig, ID merken
export FGA_STORE_ID=<id>
fga model write --file model.fga                 # neue Modellversion
fga tuple write --file tuples.yaml               # Zuordnungen
```

Später nur noch `model write` bzw. `tuple write`/`tuple delete` — der
Store bleibt, seine ID ändert sich nicht.

Ausprobieren lokal: `kiddie-pool/authz-openfga/`.
