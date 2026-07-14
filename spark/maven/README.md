# Maven-Parent-POM für JVM-Spark-Jobs

Erklärt [`pom.xml`](./pom.xml) — was darin passiert, und warum es **so und
nicht anders** gebaut ist. Die POM ist eine Vorlage: swimming-pool hat bewusst
keinen Build; sobald ein echtes Job-Repo entsteht, wandert sie dorthin
(dann `groupId` von `com.example.lakehouse` auf die eigene Organisation ändern).

## Problem, das die POM löst

Spark-Jobs mit Iceberg + S3 leiden typischerweise an zwei Dingen:

1. **Versionskonflikte**, fast immer über *transitive* Dependencies
   eingeschleppt — klassisch: AWS SDK v1 (`com.amazonaws`, via `hadoop-aws`)
   kollidiert mit dem SDK v2 (`software.amazon.awssdk`), das Iceberg S3FileIO
   nutzt. Symptom sind kryptische `NoSuchMethodError` zur Laufzeit.
2. **Doppelte Versionspflege** — jede Job-POM pinnt Spark/Iceberg/AWS selbst,
   und beim Upgrade driftet es auseinander.

Die Antwort: **eine** Parent-POM, in der alle Versionen stehen. Job-Module
erben und deklarieren nur job-spezifische Extras — **nie Versionen**.

## Gepinnte Versionen und ihre Herkunft

| Property | Wert | Warum genau dieser Wert |
|---|---|---|
| `spark.version` | `4.0.3` | Cluster-Runtime, gepinnt in [`../../iceberg/argo/maintenance-workflow.yaml`](../../iceberg/argo/maintenance-workflow.yaml) (SoT laut AGENTS.md) |
| `scala.binary.version` | `2.13` | Spark 4.x gibt es nur noch für Scala 2.13 |
| `maven.compiler.release` | `17` | Spark 4.0 verlangt Java 17+; `apache/spark:4.0.3-python3` läuft auf Java 17 — Bytecode darf nicht neuer sein als die Cluster-JVM |
| `iceberg.version` | `1.10.2` | Muss zur Runtime im Cluster passen (gleiches Jar wie in den Argo-Workflows) |
| `awssdk.version` | `2.33.0` | **Exakt** die AWS-SDK-BOM-Version, gegen die Iceberg 1.10.2 gebaut ist — nachlesbar in `gradle/libs.versions.toml` im Iceberg-Repo (Release-Tag). Nie unabhängig von Iceberg bumpen. |
| `jackson.version` | `2.18.6` | Was Spark 4.0.3 selbst ausliefert (`spark-parent`-POM). Nur ein Guard, falls eine Library Jackson transitiv reinzieht. |

**Update-Prozedur bei Versions-Bumps:**

1. Neues Iceberg → `awssdk.version` aus dessen `gradle/libs.versions.toml` übernehmen.
2. Neues Spark → `jackson.version` (und ggf. Java-Level) aus der
   `spark-parent`-POM auf Maven Central übernehmen; prüfen, ob es die
   Iceberg-Runtime für die Spark-Minor-Version schon gibt
   (Artefakt-Name enthält sie: `iceberg-spark-runtime-<spark>_<scala>`).
3. Argo-Workflows und AGENTS.md-Versionstabelle synchron halten.

## Die vier Grundregeln — und warum nicht anders

### 1. Cluster-Artefakte sind `provided`

`spark-sql` und `iceberg-spark-runtime` stehen auf Scope `provided`: Sie werden
zum Kompilieren gebraucht, landen aber **nicht** im Job-Jar — der Cluster hat
sie schon (Image bzw. `deps.packages` im Workflow). Alternative wäre, sie
mitzubundeln: Dann lägen dieselben Klassen in zwei Versionen im Klassenpfad,
und welche gewinnt, entscheidet der Classloader — genau die Konfliktklasse,
die hier ausgeschlossen werden soll.

### 2. Ein BOM regiert alle AWS-Versionen

`software.amazon.awssdk:bom` wird in `dependencyManagement` importiert. Danach
deklariert kein Modul mehr eine Version für ein `software.amazon.awssdk:*`-Artefakt —
alle SDK-Module sind automatisch versionskonsistent. Gepinnt wird das BOM auf
Icebergs eigene Build-Version, weil S3FileIO (aus der Iceberg-Runtime) und der
eigene S3-Code zur Laufzeit **dieselben** SDK-Klassen teilen: Eine abweichende
SDK-Version funktioniert oft zufällig — bis sie es nicht mehr tut.

### 3. Einzelmodule statt Fat-Bundles

Statt `software.amazon.awssdk:bundle` (~500 MB, alle ~300 SDK-Module, Version
fest verdrahtet) nur das, was gebraucht wird:

- `s3` — S3-Client für "normalen" Anwendungscode (Dateien lesen/schreiben ohne Spark)
- `apache-client` — der Sync-HTTP-Client, den der S3-Client zur Laufzeit braucht
- `sts` — *nicht* im Baseline-Set; nur nötig, wenn Jobs Rollen annehmen
  (z. B. Lakekeeper vended credentials statt statischer MinIO-Keys) → dann im Job-Modul ergänzen

Bundles unterlaufen die BOM-Steuerung und blähen das Jar auf. **Ausnahme:**
`iceberg-aws-bundle` in den Argo-Workflows via `deps.packages` ist richtig —
dort gibt es keinen Maven-Build, der Einzelmodule auflösen könnte.

### 4. Kein S3A im Build — S3A ist Image-Sache

`hadoop-aws` (der S3A-Connector für `s3a://`-Pfade über die
Hadoop-FileSystem-API) zieht transitiv das AWS SDK **v1** (bis Hadoop 3.3)
bzw. das v2-**Fat-Bundle** (ab Hadoop 3.4) — beides hier gebannt.

Der Ban gilt dem **Build**, nicht dem Schema `s3a://` an sich:

- **Iceberg-Tabellen-I/O braucht kein S3A.** S3FileIO akzeptiert `s3://`,
  `s3a://` und `s3n://` gleichwertig — auch wenn der Catalog (Lakekeeper)
  Locations mit `s3a://` liefert, läuft das über SDK v2, ohne Hadoop.
- **Eigener Code** liest S3 über den SDK-v2-`S3Client` (deshalb `s3` +
  `apache-client` im Baseline-Set).
- **Echte Hadoop-FS-Pfade** gibt es im Plattform-Design trotzdem:
  Streaming-`checkpointLocation`, Landing-Zone-Reads
  (`spark.read.csv("s3a://landing/...")`), `spark.eventLog.dir`,
  `spark.archives`. Dafür gehört `hadoop-aws` (Version = Hadoop-Version des
  Images, bei Spark 4.0.3 also 3.4.1) **ins Runner-Image** — nie ins Job-Jar,
  damit keine konkurrierende Kopie neben der Image-Version liegt. Das plain
  `apache/spark`-Image bringt es nicht mit. Fehlerbild, wenn es fehlt:
  `No FileSystem for scheme "s3a"`.

## Die Plugin-Sektion

### `pluginManagement` vs. `plugins`

Gleiche Logik wie `dependencyManagement`: **`pluginManagement`** definiert nur
Version + Konfiguration, aktiviert aber nichts — Module opten per Deklaration
ein. **`plugins`** läuft automatisch in jedem Modul.

- **Shade** steht in `pluginManagement`: Nicht jedes Modul soll ein Uber-Jar
  bauen (der Parent selbst nicht, ein Shared-Library-Modul auch nicht).
  Job-Module aktivieren ihn per Dreizeiler ohne eigene Konfiguration.
- **Enforcer** steht in `plugins`: Die Konfliktregeln gelten immer — kein
  Modul kann sich durch Weglassen ausklinken.

### maven-shade-plugin — das deploybare Uber-Jar

`mvn package` allein baut ein Jar ohne Dependencies → `ClassNotFoundException`
auf dem Cluster. Der Shade legt alle `compile`-Dependencies mit ins Jar;
`provided` bleibt draußen (siehe Regel 1). Drei Konfigurationsdetails, die
jeweils einen konkreten Laufzeitfehler verhindern:

| Konfiguration | Verhindert |
|---|---|
| `ServicesResourceTransformer` | AWS SDK & Iceberg finden Implementierungen via `ServiceLoader` (`META-INF/services/`-Dateien). Beim Mergen vieler Jars kollidieren gleichnamige Dateien — ohne Transformer gewinnt die erste, der Rest geht verloren. Symptom: `Unable to load an HTTP implementation from any provider in the chain`. Der Transformer konkateniert die Einträge. |
| Filter auf `META-INF/*.SF`, `*.DSA`, `*.RSA` | Signierte Dependencies: Nach dem Umverpacken stimmt die Jar-Signatur nicht mehr → `SecurityException: Invalid signature file digest`. Signaturdateien werden entfernt. |
| `createDependencyReducedPom=false` | Unterdrückt die `dependency-reduced-pom.xml`, die der Shade sonst pro Build ins Projektverzeichnis schreibt — relevant nur, wenn andere das Uber-Jar als Maven-Dependency konsumieren; Job-Jars gehen an `spark-submit`. |

### maven-enforcer-plugin — Konflikte als Build-Fehler

Prüft den **komplett aufgelösten** Dependency-Baum (inkl. aller transitiven
Ebenen) und bricht den Build mit Verursacher-Angabe ab, statt das Problem als
Laufzeit-Mysterium auf den Cluster durchzureichen:

| Ban | Grund |
|---|---|
| `com.amazonaws:*` | AWS SDK v1 — kollidiert mit dem v2-Stack von S3FileIO |
| `software.amazon.awssdk:bundle` | Fat-Bundle, unterläuft das BOM (Regel 3) |
| `org.apache.iceberg:iceberg-aws-bundle` | dito; gehört in `deps.packages` der Workflows, nicht in Maven-Builds |
| `org.apache.hadoop:hadoop-aws` | S3A gehört ins Runner-Image, nie ins Job-Jar; zieht SDK v1 (Hadoop ≤3.3) bzw. das v2-Fat-Bundle (3.4+) nach (Regel 4) |

## Job-Modul anlegen

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <parent>
    <groupId>com.example.lakehouse</groupId>
    <artifactId>spark-jobs-parent</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <relativePath>../../pom.xml</relativePath>
  </parent>

  <artifactId>example-etl</artifactId>

  <dependencies>
    <!-- job-specific extras only, versions come from the parent, e.g.: -->
    <!-- <dependency>
      <groupId>software.amazon.awssdk</groupId>
      <artifactId>sts</artifactId>
    </dependency> -->
  </dependencies>

  <build>
    <plugins>
      <!-- opt-in: full shade config is inherited from the parent -->
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
      </plugin>
    </plugins>
  </build>
</project>
```

Dazu das Modul in der Parent-POM unter `<modules>` eintragen.

## Sonderfall: Dependencies-Jar für PySpark

Da die Jobs in diesem Repo PySpark sind, gibt es keine eigene JVM-Anwendung —
aber die JVM-Seite (Iceberg-Runtime, AWS SDK, Kafka-Connector) muss trotzdem
in den Cluster. Statt `deps.packages` (Maven-Auflösung bei jedem Job-Start:
Latenz, Netzwerk-Abhängigkeit) kann ein **Deps-Jar-Modul** ein einziges,
deterministisches Fat-Jar bauen: Modul ohne Code, das die Iceberg-Runtime mit
explizitem `<scope>compile</scope>` deklariert (überschreibt das gemanagte
`provided`, denn dieses Jar *ist* die Runtime-Auslieferung) plus die
AWS-Module aus dem Parent.

Auslieferung, in aufsteigender Production-Tauglichkeit: `--jars` beim
`spark-submit` (Dev) → `deps.jars` in der `SparkApplication`-CRD → ins
Runner-Image nach `/opt/spark/jars/` baken (Empfehlung aus
[`../kubernetes.md`](../kubernetes.md), passt zum Pattern "stabiles
Runner-Image + Python-Code separat").

Zwei harte Regeln dabei:

- **`spark-sql`/`spark-core` dürfen nie ins Deps-Jar** — Spark ist der
  Host-Prozess; nach dem ersten Build per `mvn dependency:tree` prüfen,
  dass nichts `org.apache.spark:*` als `compile` durchsickert.
- **Nie doppelt ausliefern**: entweder Deps-Jar *oder* `deps.packages` in den
  Workflows — beides zusammen legt dieselben Klassen zweimal in den Klassenpfad.
