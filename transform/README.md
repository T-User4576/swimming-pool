# transform/ — Deklaratives Transformations-Framework

Ein config-getriebener Spec-Runner für "einfache", häufig wiederkehrende
Datenverarbeitungen (Bronze→Silver→Gold: Deduplizierung, Spalten-Mapping,
Aggregation, Filter, Cast). Statt je Fall einen eigenen PySpark-Job zu
schreiben, wird die Verarbeitung als deklarative YAML-Spec beschrieben; ein
generischer Runner führt sie auf Spark-on-K8s aus und schreibt nach Iceberg.

Spiegelt das Agnostik-/Registry-Muster von `iceberg/spark/maintenance.py` +
`iceberg/argo/maintenance-workflow.yaml`: **eine** Code-Basis, **ein**
WorkflowTemplate, fachliche Definition ausschließlich in den Specs.

## Verzeichnis

| Pfad | Inhalt |
|---|---|
| [`transform-spec.md`](./transform-spec.md) | Spec-Schema-Referenz: alle `source`/`schema`/`steps`/`sink`-Felder, Erweiterbarkeit, Verifikations-Checkliste |
| [`spark/runner.py`](./spark/runner.py) | Generischer Spec-getriebener PySpark-Job (Registries `SOURCES`/`STEPS`/`SINKS`) |
| [`argo/transform-workflow.yaml`](./argo/transform-workflow.yaml) | WorkflowTemplate `spark-transform` + Beispiel-CronWorkflow + Beispiel-DAG |
| [`pipelines/`](./pipelines/) | Versionierte Pipeline-Specs (je ein fachlich geowntes Artefakt) |

## Quick-Start

Neue Verarbeitung anlegen:

1. YAML-Spec in `pipelines/` erstellen — Schema-Referenz: `transform-spec.md`.
2. Lokal dry-runnen (siehe unten), bis die Spec sauber lädt.
3. ConfigMap aktualisieren + (falls geplant) eine `CronWorkflow` für die
   Pipeline ergänzen.

Weder `runner.py` noch `transform-workflow.yaml` müssen dafür angefasst werden.

## Deployment

```bash
# 1) Runner-Code als ConfigMap
kubectl create configmap transform-runner-script \
  --from-file=runner.py=transform/spark/runner.py \
  -n spark --dry-run=client -o yaml | kubectl apply -f -

# 2) Alle Pipeline-Specs als ConfigMap
kubectl create configmap transform-pipeline-specs \
  --from-file=transform/pipelines/ \
  -n spark --dry-run=client -o yaml | kubectl apply -f -

# 3) WorkflowTemplate + CronWorkflows
kubectl apply -f transform/argo/transform-workflow.yaml
```

Voraussetzungen: Spark Operator im Cluster, Secret `minio-credentials` im
Namespace `spark`. Für den Kafka-Source-Typ ist das Package
`spark-sql-kafka-0-10` bereits in `deps.packages` des Workflows gesetzt.

## Ausführen

```bash
# Dry-Run im Cluster (kein Write, kein Snapshot — explain + Count im Log)
argo submit --from cronwf/transform-gold-revenue-daily -n argo -p dry-run=true

# Echter Lauf
argo submit --from cronwf/transform-gold-revenue-daily -n argo

# Lokal (vor jedem Cluster-Lauf empfohlen) — gegen lokales MinIO/Lakekeeper
spark-submit --master local[2] transform/spark/runner.py \
  --catalog lake --spec transform/pipelines/gold-revenue-daily.yaml --dry-run
```

## Code-Distribution

PoC: ConfigMap-Mount, exakt wie bei `iceberg/spark/maintenance.py` — `runner.py`
unter `/opt/spark/work-dir/`, die Specs unter `/opt/spark/specs/`.

**Upgrade-Pfad** (sobald die Spec-Menge das 1-MiB-ConfigMap-Limit erreicht oder
job-spezifische Python-Dependencies nötig werden): `runner.py` + ein
`requirements.txt` (mind. `PyYAML`) als venv-pack-Archiv bündeln und die Specs
aus einem S3-Prefix lesen (`--spec s3a://artifacts/transform/specs/<name>.yaml`).
Distributions-Patterns: `../spark/kubernetes.md`. Kein Custom-Image als
alleinige Strategie (AGENTS.md, Anti-Patterns).

> Hinweis: `runner.py` braucht `PyYAML` im Spark-Image. `apache/spark:3.5.1-python3`
> bringt es nicht garantiert mit — im PoC ggf. ergänzen, im venv-pack-Pfad über
> `requirements.txt` lösen.

## Offene Punkte

- Inkrementelle Verarbeitung (`source.incremental`-Block) — siehe
  `transform-spec.md` Abschnitt 7.
- `value_format: avro` für Kafka-Sources.
- Sink-seitige Schema-Validierung gegen die Ziel-Tabelle.
- Weitere Source-/Sink-Typen (JDBC, Datei-Export) — additiv über die Registries.
