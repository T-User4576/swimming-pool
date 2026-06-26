# Spark auf Kubernetes mit dem Spark-Operator

Wie Spark-Anwendungen im Cluster deployed, gestartet und gemonitored werden.
Setzt die Architektur-Grundlagen aus [`./architecture.md`](./architecture.md)
voraus.

## 1. Zwei Deployment-Modelle

| Modell | Wer startet die App | Wie |
|---|---|---|
| **`spark-submit` mit `--master k8s://...`** (native) | Mensch oder CI direkt | Tooling-Light, aber kein deklaratives Lifecycle-Management |
| **Spark Operator** (sparkoperator.k8s.io) | K8s-Operator | Deklarativ via `SparkApplication`-CRD — empfohlen für Production |

Wir nutzen den **Spark Operator**. Vorteil: SparkApplications sind erste-
Klasse-Objekte im Cluster, mit Lebenszyklus, Status, Retry-Logik, GC. Argo-
Workflows können sie via `resource: action: create` triggern und auf Status
warten — wie in [`../iceberg/argo/maintenance-workflow.yaml`](../iceberg/argo/maintenance-workflow.yaml) bereits umgesetzt.

## 2. Was der Operator anlegt

Wenn du eine `SparkApplication` erstellst, baut der Operator daraus:

```
SparkApplication (CRD, dein Manifest)
        │
        ▼ (Operator-Controller reagiert)
        │
   ┌────┴───────────────────────────────────────┐
   │                                            │
Driver Pod                          Headless Service (für Exec ←→ Driver)
   │
   │ startet beim Booten:
   │ - SparkContext
   │ - Driver Web-UI (Port 4040)
   │ - Kubernetes-Scheduler-Backend
   │
   └─► fordert über kube-apiserver Executor-Pods an
              │
              ▼
        Executor Pods (1..n, dynamisch)
              │
              ▼
        verbinden zurück zum Driver via Service-DNS
```

Wichtige Punkte:
- **Driver-Pod hat einen ServiceAccount**, der RBAC-Rechte braucht, um
  Executor-Pods zu erzeugen (verb: create, get, watch, delete auf pods).
- **Executor-Pods reden direkt mit dem Driver** über die Headless Service-
  Adresse — nicht über kube-apiserver.
- **Web-UI ist ephemeral**: nur solange der Driver-Pod lebt. Für historische
  Inspektion brauchst du den **Spark History Server** (siehe Abschnitt 7).

## 3. SparkApplication-Manifest — die wichtigsten Felder

Reduzierte Variante (Komplettes Beispiel: [`../iceberg/argo/maintenance-workflow.yaml`](../iceberg/argo/maintenance-workflow.yaml)):

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  generateName: my-job-
  namespace: spark
spec:
  type: Python                          # oder Scala, Java, R
  pythonVersion: "3"
  mode: cluster                         # immer cluster für Production
  image: apache/spark:3.5.1-python3
  imagePullPolicy: IfNotPresent
  mainApplicationFile: local:///opt/work/job.py
  arguments:
    - "--option"
    - "value"

  sparkConf:                            # spark-submit --conf
    spark.sql.adaptive.enabled: "true"
    spark.sql.shuffle.partitions: "200"
    # ... alle spark.* configs

  hadoopConf:                           # core-site.xml-Werte
    fs.s3a.endpoint: http://minio:9000

  deps:                                 # Maven-Coordinates oder Files
    packages:
      - org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0

  driver:
    cores: 2
    memory: 8g
    serviceAccount: spark-driver        # mit RBAC für Pod-Create
    labels: { version: "3.5.1" }

  executor:
    cores: 4
    instances: 5                        # statisch; oder Dynamic Allocation
    memory: 16g
    labels: { version: "3.5.1" }

  restartPolicy:
    type: OnFailure
    onFailureRetries: 2
    onFailureRetryInterval: 30          # Sekunden

  volumes:
    - name: code
      configMap:
        name: my-job-script
  driver:
    volumeMounts:
      - name: code
        mountPath: /opt/work
```

## 4. ServiceAccount & RBAC

Der Driver-Pod muss Executor-Pods erzeugen können. Minimal-RBAC:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark-driver
  namespace: spark
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-driver
  namespace: spark
rules:
  - apiGroups: [""]
    resources: [pods]
    verbs: [create, get, list, watch, delete, patch]
  - apiGroups: [""]
    resources: [services]
    verbs: [create, get, list, watch, delete]
  - apiGroups: [""]
    resources: [configmaps]
    verbs: [get, list, watch, create]
  - apiGroups: [""]
    resources: [persistentvolumeclaims]
    verbs: [create, get, list, watch, delete]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: spark-driver
  namespace: spark
subjects:
  - kind: ServiceAccount
    name: spark-driver
    namespace: spark
roleRef:
  kind: Role
  name: spark-driver
  apiGroup: rbac.authorization.k8s.io
```

Der **Operator selbst** braucht weitergehende Rechte (cluster-scoped meist),
die kommen aus dem Operator-Helm-Chart.

## 5. Wie kommt der Anwendungscode in den Pod?

Vier gängige Muster:

| Muster | Wann | Wie |
|---|---|---|
| **ConfigMap → Volume-Mount** | Kleine Skripte (≤ 1 MB) | `kubectl create configmap script --from-file=job.py`, dann via `volumes` mounten. So machen wir es bei `iceberg/spark/maintenance.py`. |
| **Code im Image** | Wenn Spark-Image und Job 1:1 zusammengehören | Eigenes Image mit Code unter `/opt/spark/work-dir/`, Image-Tag = Job-Version. Wenig Flexibilität bei vielen unterschiedlichen Jobs. |
| **`mainApplicationFile: <s3://...>`** | Code im Object Storage | Spark zieht das File beim Start. Setzt voraus, dass der Driver-Pod Zugriff auf den Bucket hat. |
| **Stable Runner-Image + venv-pack-Archiv** | Viele PySpark-Jobs gegen einen gepinnten Spark-/Iceberg-Stack | Ein "pysparkRunner"-Image bringt Spark + Iceberg-/AWS-JARs + Python-Runtime mit. Job-spezifischer Python-Code + Dependencies werden mit [`venv-pack`](https://jcristharif.com/venv-pack/) als portables Archiv gepackt und beim Spark-Submit als `--archives env.tar.gz#env` mitgegeben. Driver/Executor extrahieren das Archiv lokal und nutzen die enthaltene venv. So bleibt das Runner-Image stabil, jeder Job shippt seinen eigenen Dependency-Zustand. |

### Kurz zu venv-pack

```bash
# Build-Schritt (CI):
python -m venv /tmp/env
/tmp/env/bin/pip install -r requirements.txt
venv-pack -o env.tar.gz -p /tmp/env

# Im SparkApplication-Manifest:
spec:
  deps:
    archives:
      - s3a://artifacts/<job>/env.tar.gz#env     # # gibt den Mount-Namen
  sparkConf:
    spark.pyspark.python: ./env/bin/python
    spark.pyspark.driver.python: ./env/bin/python
```

Vorteile dieses Patterns:
- Runner-Image bleibt stabil über alle Jobs hinweg, kein N×Image-Build.
- Job-Code + Job-spezifische Dependencies sind versioniert (Archiv-URL = Version).
- CI-Lifecycle ist von der Spark-Runtime entkoppelt.

## 6. Dependencies / JARs

Iceberg, S3-Connectoren, etc. müssen in den Spark-Classpath. Drei Wege:

| Weg | Vorteile | Nachteile |
|---|---|---|
| **`spec.deps.packages`** (Maven Coordinates) | Einfach, keine Image-Builds | Maven-Auflösung beim Start = Latenz, Netzwerk-Abhängigkeit |
| **Custom Image** mit JARs in `/opt/spark/jars/` | Schnell, deterministisch | Image-Build nötig |
| **`spec.deps.jars`** mit URL/Pfad | Flexibel | Brüchig, schwer zu auditieren |

Für Production: **Image**. Für Dev/PoC: `packages`.

Iceberg-spezifisch (Stand: Iceberg 1.6.0 + Spark 3.5):
```yaml
deps:
  packages:
    - org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0
    - org.apache.iceberg:iceberg-aws-bundle:1.6.0
```

## 7. Logs & History Server

**Driver-/Executor-Logs**: Stdout der Pods → ephemeral. Sobald die Pods
weg sind, sind die Logs weg. Lösung: **Log-Collector (z. B. Fluent Bit /
Vector) → zentrales Log-Backend**.

**Spark UI während des Runs**:
```bash
kubectl port-forward -n spark <driver-pod> 4040:4040
# Browser → http://localhost:4040
```

**History Server für abgeschlossene Apps**:
1. Jeder Spark-Job schreibt Event-Log in den Object Store:
   ```yaml
   sparkConf:
     spark.eventLog.enabled: "true"
     spark.eventLog.dir: s3a://spark-event-logs/
   ```
2. Spark History Server als separates Deployment, das diese Logs liest und
   die UI für vergangene Apps bereitstellt.
3. Helm-Chart: `spark-history-server` (community).

Ohne History Server bist du bei Post-Mortem-Analysen blind. Sehr empfohlen
einzurichten, sobald mehr als ein paar Jobs laufen.

## 8. Networking & Service-Discovery

| Verbindung | Wie |
|---|---|
| Executor → Driver | Headless Service `<app-name>-driver-svc.spark.svc.cluster.local`, Port `7078` (default) |
| Driver → Block Manager | Pod-IP der Executors, Port `7079` |
| Driver → Web-UI | Port `4040` (im Driver-Pod) |
| Operator → SparkApplication-CRD | Standard kube-apiserver |

**Wichtig**: K8s NetworkPolicies, die Pod-zu-Pod-Verkehr blockieren, brechen
Spark sofort. Whitelist mind. die spark-Namespace nach innen.

## 9. Dynamic Allocation auf K8s

Spark Dynamic Allocation (Executors hochfahren bei Last, runter bei Idle)
funktioniert auf K8s, hat aber Caveats:

```yaml
sparkConf:
  spark.dynamicAllocation.enabled: "true"
  spark.dynamicAllocation.shuffleTracking.enabled: "true"   # Pflicht in K8s
  spark.dynamicAllocation.minExecutors: "1"
  spark.dynamicAllocation.maxExecutors: "20"
  spark.dynamicAllocation.executorIdleTimeout: "60s"
```

Caveats:
- Ohne `shuffleTracking.enabled` würde Spark Executors mit Shuffle-Daten
  killen → fehlgeschlagene Re-Reads. Pflicht-Setting.
- Pod-Spin-up dauert ~10–30s; bei sehr kurzen Jobs nicht sinnvoll.
- Argo-Workflows sehen Dynamic Allocation als "instances changed" — kein
  Problem, aber die `executor.instances`-Vorgabe wird ignoriert wenn DA an.

Empfehlung: **Statische Allocation** für Maintenance-Jobs (gleichbleibende Last),
**Dynamic** für ad-hoc / breit variierende Workloads.

## 10. Lebenszyklus & Cleanup

| Ereignis | Was passiert |
|---|---|
| App startet | Driver-Pod entsteht; Driver fordert Executors an |
| App läuft | Pods leben; Logs streamen |
| App finished (Success) | Executor-Pods werden vom Operator gelöscht; Driver-Pod bleibt im `Completed`-Status, je nach `timeToLiveSeconds` |
| App failed | Operator versucht laut `restartPolicy.onFailureRetries` neu |
| App-CR gelöscht | Operator räumt alle zugehörigen Pods auf |

`spec.timeToLiveSeconds` setzt das automatische GC-Fenster für completed
Driver-Pods — sonst sammeln sich die im `Completed`-Status für immer an.

```yaml
spec:
  timeToLiveSeconds: 86400              # 24h, dann CR + Driver-Pod weg
```

Für Argo-getriggerte Jobs reicht oft `timeToLiveSeconds: 3600` — Argo hat
seine eigene Workflow-History, die Driver-Pod-Trace ist redundant.

## 11. Bezug zu unserem Repo

Konkret implementiert:

- **Spark-Operator-Pattern**: [`../iceberg/argo/maintenance-workflow.yaml`](../iceberg/argo/maintenance-workflow.yaml)
  zeigt, wie ein Argo-WorkflowTemplate eine `SparkApplication` deklarativ erzeugt
  und auf Status `COMPLETED` wartet.
- **PySpark-Anwendungs-Code**: [`../iceberg/spark/maintenance.py`](../iceberg/spark/maintenance.py)
  — agnostischer Maintenance-Job, der via ConfigMap an den Driver-Pod gemountet wird.
- **ConfigMap-Pattern für Code**: in der Iceberg-Quick-Start (siehe
  [`../iceberg/README.md`](../iceberg/README.md)) als `kubectl create configmap iceberg-maintenance-script`.

Für die Best-Practices und Sizing-Empfehlungen: siehe
[`./best-practices.md`](./best-practices.md).
