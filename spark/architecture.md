# Spark — Architektur & Funktionsweise

Wie Spark intern arbeitet, unabhängig vom Deployment. Für die Kubernetes-
spezifischen Aspekte (Pod-Modell, Operator) siehe [`./kubernetes.md`](./kubernetes.md).

## 1. Die drei Komponenten einer Spark-Anwendung

```
                     +----------------+
                     |  Cluster Manager  | (in unserem Fall: Kubernetes-API)
                     +----------------+
                              │ allokiert Pods
              ┌───────────────┴───────────────┐
              ▼                               ▼
        ┌──────────┐                    ┌──────────┐
        │  Driver  │ ──── steuert ────► │ Executor │  (1..n)
        │   Pod    │                    │   Pods   │
        └──────────┘                    └──────────┘
         │ enthält:                      │ enthalten:
         - SparkContext/SparkSession     - JVM mit Cores/Memory
         - DAG-Scheduler                 - Task-Slots
         - Task-Scheduler                - Cache (Memory + Disk)
         - Web-UI Endpoint               - Shuffle-Storage
```

| Rolle | Lebensdauer | Verantwortung |
|---|---|---|
| **Driver** | Eine pro Application | Plant Stages/Tasks, sammelt Ergebnisse, hält Application-State |
| **Executor** | Eine bis viele pro App | Führt Tasks aus, hält Cache und Shuffle-Daten lokal |
| **Cluster Manager** | Cluster-weit | Allokiert/dealloziert Pods (in K8s: kube-apiserver) |

**Wichtig**: Driver und Executor reden direkt miteinander, nicht über den
Cluster-Manager. Die initiale Anforderung "ich brauche N Executors" geht über
den Manager, der Rest ist Peer-to-Peer.

## 2. Job → Stage → Task

Jede Action (`count()`, `write()`, `collect()`, ...) löst einen **Job** aus.
Der Driver baut daraus einen DAG und zerlegt ihn in **Stages** und **Tasks**.

```
Job:    DataFrame.write.parquet(...)
        │
Stage:  ────── Stage 0 ──────  ──── Stage 1 ────
        Read → Filter → Map     Shuffle → Aggregate → Write
        │                       │
Tasks:  T0,T1,T2,T3,T4,T5      T0,T1,T2,T3
        (eine pro Partition)    (eine pro Partition)
```

| Begriff | Was |
|---|---|
| **Job** | Eine Action triggert einen Job |
| **Stage** | Ein zusammenhängender Block ohne Shuffle (=Wide-Dependency-Grenze) |
| **Task** | Konkrete Ausführungseinheit auf einer Daten-Partition, läuft auf einem Executor-Core |

**Stage-Grenze** ist immer dort, wo Daten zwischen Executors umgeschaufelt
werden müssen — `groupBy`, `join`, `repartition`, `distinct`. Das ist der
Performance-kritische Punkt, weil Shuffle-Daten serialisiert, übers Netz
geschickt und auf Disk geschrieben werden.

## 3. Catalyst & Tungsten — der Optimizer-Stack

Spark SQL / DataFrame-API gehen durch eine vierstufige Pipeline:

```
1. Unresolved Logical Plan   ← parser
2. Resolved Logical Plan     ← analyzer (Catalog-Lookup, Type-Resolution)
3. Optimized Logical Plan    ← Catalyst (Predicate-Pushdown, Constant-Folding,
                                Projection-Pruning, Join-Reordering, ...)
4. Physical Plan             ← strategies (broadcast vs sort-merge join etc.)
5. RDD-Execution             ← Tungsten (whole-stage codegen, off-heap memory)
```

**Catalyst** transformiert deinen Code mit ~100 Optimierungs-Regeln. Du kriegst
das z.B. dadurch geschenkt, dass du einen `WHERE`-Filter nach einem `JOIN`
schreibst — Catalyst zieht ihn vor den Join, wenn das semantisch geht.

**Tungsten** ist die Execution-Engine: kompiliert ganze Stages zu einem
einzigen Bytecode-Block (Whole-Stage Code Generation), nutzt off-heap Memory
für Daten-Buffer, vermeidet JVM-Object-Overhead.

Praktisch heißt das: **du musst meistens nichts manuell optimieren**, solange
du DataFrame/SQL nutzt. RDD-Code (low-level, ohne Catalyst) ist heute selten
nötig.

## 4. Adaptive Query Execution (AQE)

Seit Spark 3.0 GA, in 3.5+ default an. Verändert den Physical Plan
**zur Laufzeit** basierend auf realen Daten-Statistiken nach jeder Stage.

| Was AQE kann | Wirkung |
|---|---|
| **Coalesce Shuffle Partitions** | Reduziert nach Shuffle die Anzahl Partitionen, wenn sie zu klein sind. Spart Tasks-Overhead. |
| **Switch Join Strategy** | Erkennt zur Laufzeit, ob ein Sort-Merge-Join in einen Broadcast-Join umgewandelt werden kann (eine Seite kleiner als erwartet). |
| **Skew Join Handling** | Erkennt Daten-Skew (eine Partition viel größer als andere) und splittet sie automatisch. |

Steuerung:
```ini
spark.sql.adaptive.enabled = true                              # default seit 3.2
spark.sql.adaptive.coalescePartitions.enabled = true
spark.sql.adaptive.skewJoin.enabled = true
spark.sql.adaptive.autoBroadcastJoinThreshold = 100MB           # zur Laufzeit prüfen
```

AQE ist der wichtigste Hebel, den man **nicht** abschaltet.

## 5. Joins — die teuerste Operation

Drei Strategien, die der Optimizer auswählt:

| Strategie | Wann wird gewählt | Kosten |
|---|---|---|
| **Broadcast Hash Join** | Eine Seite < `spark.sql.autoBroadcastJoinThreshold` (default 10MB) | Klein × Groß: Klein-Seite an alle Executors verschickt, kein Shuffle der Groß-Seite |
| **Sort-Merge Join** | Default für große Tabellen | Beide Seiten via Shuffle nach Join-Key partitioniert + sortiert, dann gemerged |
| **Shuffle Hash Join** | Selten; eine Seite klein genug für Hash-Map pro Partition | Mittlerer Shuffle, dann lokal gehasht |

Faustregel: **wenn eine der beiden Seiten klein ist, willst du Broadcast**.
Spark wählt das automatisch, wenn die Statistiken stimmen — sonst manuell:
```sql
SELECT /*+ BROADCAST(d) */ ... FROM facts f JOIN d ON ...
```

## 6. Partitionierung — zwei Bedeutungen

Wichtig zu unterscheiden:

| | Daten-Partition | Task-Partition |
|---|---|---|
| Wo | Auf Storage (Iceberg-Partition, Parquet-Files) | Im Spark zur Ausführung |
| Wer steuert es | Schreib-Pipeline (z.B. `PARTITIONED BY days(ts)`) | Spark-Engine + Konfiguration |
| Default-Wert | je Tabellen-Definition | `spark.sql.shuffle.partitions = 200` |

**Daten-Partitionen** beeinflussen, was beim Read überhaupt gelesen wird
(Partition Pruning). **Task-Partitionen** sind die Granularität der
Parallelität — eine Task pro Partition.

Bei AQE wird `shuffle.partitions = 200` weniger kritisch, weil AQE nach dem
Shuffle coalesced. Bei sehr großen Joins (TB-Bereich) trotzdem hochsetzen
auf 1000+, sonst sind die Partitionen zu groß für den Executor-Memory.

## 7. Shuffles — der Performance-Bottleneck

Shuffle = Daten zwischen Executors umverteilen. Passiert bei:
- `groupBy`, `join`, `distinct`, `orderBy`
- `repartition`, `coalesce` (mit Shuffle)
- Window-Functions

Was technisch passiert:
1. Map-Side: jeder Executor schreibt seine Output-Partitionen lokal auf Disk
2. Reduce-Side: jeder Executor zieht die ihm zugehörigen Partitionen über Netz
3. Daten werden serialisiert (Java/Kryo) → Disk-IO → Netz-IO → Disk-IO → Deserialisiert

Das ist **immer** der teuerste Schritt. Optimierungs-Hebel:
- Shuffle reduzieren (Pre-Aggregation, Partition-Pruning)
- Broadcast Joins wo möglich
- AQE für Coalesce + Skew-Handling
- Kompression: `spark.shuffle.compress = true` (default) + Codec
- Spill auf schnelle Disks (lokale NVMe statt Network-Storage)

## 8. Caching / Persistence

```python
df.cache()       # Memory only (default)
df.persist(StorageLevel.MEMORY_AND_DISK)
df.unpersist()
```

**Wann verwenden**:
- DataFrame wird **mehrfach** gebraucht (z.B. zwei separate Aggregationen darauf)
- Iterative Workloads (ML, Graph)

**Wann NICHT**:
- Wenn das DataFrame nur einmal gelesen wird → Cache ist Verschwendung
- Bei sehr großen Datasets, die nicht in den Memory passen → kostet mehr durch Spill als es spart

Cache greift erst nach der ersten Action — `cache()` ist lazy.

## 9. Lineage & Fault Tolerance

DataFrames sind **immutable** und tragen ihre Entstehungsgeschichte (Lineage)
intern. Wenn ein Executor stirbt, kann Spark anhand der Lineage die Tasks auf
den verlorenen Partitionen neu rechnen — ohne State-Verlust für die
Application.

Praktische Konsequenz: **Spark ist gegen Pod-Restarts bemerkenswert robust**.
Ein einzelner Executor-Crash verursacht maximal die Neuberechnung der Tasks
auf dem verlorenen Pod, kein App-Restart nötig.

**Aber**: stirbt der **Driver**, ist die ganze Application weg. Drum kommt
Driver-HA in K8s über Argo-Retry, nicht über Spark selbst.

## 10. Was du dir merken solltest

1. **Driver = Hirn, Executor = Muskeln, Cluster Manager = Personalvermittlung.**
2. **Stage-Grenze ≈ Shuffle-Grenze.** Shuffles minimieren = halbe Miete.
3. **AQE ist meistens dein Freund** — nicht abdrehen.
4. **Broadcast wenn möglich, Sort-Merge wenn nötig.**
5. **DataFrame > RDD** für 99% der Cases — Catalyst optimiert mit.
6. **Cache bedacht einsetzen**, nicht reflexartig.
7. **Driver-Tot = App-Tot.** Resilience kommt von außen (Argo-Retries).
