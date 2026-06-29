# ClickHouse — Tiered Storage auf S3/MinIO (Hot/Cold)

Wie man ClickHouse so betreibt, dass aktuelle Daten auf lokalem NVMe liegen und
älteres automatisch nach S3/MinIO wandert (z. B. **„alles älter als 4 Wochen →
cold"**). Plus die Stolpersteine im großen Kontext (Zero-Copy, Kosten, Merges).

## Mentales Modell: zwei Konfigurationsorte

Das verteilt sich auf **zwei Ebenen** — nicht verwechseln:

| Was | Wo | Artefakt |
|---|---|---|
| **Disks + Storage-Policy** (Infrastruktur: „es gibt ein hot- und ein cold-Volume") | Server-Config | CHI → `spec.configuration.files` (`config.d/storage.xml`) |
| **Regel „4 Wochen → cold"** + Policy-Zuordnung | Tabellen-DDL (SQL) | `CREATE/ALTER TABLE … TTL … / storage_policy=…` |

Merke: Die **Zeit-Regel ist kein CHI-Setting**, sondern eine Tabelleneigenschaft.
Die CHI liefert nur die Volumes, an die die Tabelle ihre Parts schieben *kann*.

## 1. Infrastruktur in der CHI (Server-Config)

In `chi-hardened.yaml` unter `spec.configuration.files` → `config.d/storage.xml`:

```xml
<storage_configuration>
  <disks>
    <s3_disk>
      <type>s3</type>
      <endpoint>http://minio.minio.svc:9000/clickhouse-data/</endpoint>
      <access_key_id from_env="S3_ACCESS_KEY"/>
      <secret_access_key from_env="S3_SECRET_KEY"/>
    </s3_disk>
    <s3_cache>                         <!-- local NVMe cache in front of S3 -->
      <type>cache</type>
      <disk>s3_disk</disk>
      <path>/var/lib/clickhouse/s3_cache/</path>
      <max_size>200Gi</max_size>
    </s3_cache>
  </disks>
  <policies>
    <s3_tiered>
      <volumes>
        <hot>
          <disk>default</disk>         <!-- the local data PVC -->
          <move_factor>0.2</move_factor>
        </hot>
        <cold><disk>s3_cache</disk></cold>
      </volumes>
    </s3_tiered>
  </policies>
</storage_configuration>
```

- **`default`** ist das lokale Daten-PVC (`volumeClaimTemplate: data`).
- **`s3_cache`** (Typ `cache`) ist *der* Performance-Hebel: lokaler NVMe-Cache
  vor S3. Reads heißer Cold-Parts landen lokal, Wiederholzugriffe sind lokal.
- S3-Keys über **`from_env`** + Secret-Env am Container (`ch-s3`-Secret), nicht
  als Klartext im XML.

## 2. Die „4-Wochen-Regel" — Tabellen-DDL (SQL)

Das passiert **nicht** in der CHI, sondern per SQL an der Tabelle:

```sql
CREATE TABLE events
(
    ts   DateTime,
    ...,
    INDEX idx_user user_id TYPE bloom_filter GRANULARITY 4   -- skip index
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/events', '{replica}')
PARTITION BY toMonday(ts)         -- partition granularity <= tiering window!
ORDER BY (user_id, ts)
TTL ts + INTERVAL 4 WEEK TO VOLUME 'cold'
SETTINGS storage_policy = 's3_tiered';
```

Nachträglich ändern geht auch:

```sql
ALTER TABLE events MODIFY SETTING storage_policy = 's3_tiered';  -- nur "vorwärts" möglich
ALTER TABLE events MODIFY TTL ts + INTERVAL 4 WEEK TO VOLUME 'cold';
```

Beobachten / steuern:

```sql
SELECT * FROM system.moves;                         -- laufende Moves
SELECT table, disk_name, sum(bytes_on_disk) FROM system.parts
WHERE active GROUP BY table, disk_name;             -- was liegt wo
SYSTEM STOP MOVES events; / SYSTEM START MOVES events;
```

## 3. Worauf im großen Kontext achten

### Move-Semantik & Partitionierung (häufigster Fehler)

`TTL … TO VOLUME` verschiebt **ganze Parts** — ein Part kann nicht über zwei
Volumes liegen. Ein Part wird erst verschoben, wenn seine Daten die TTL-Grenze
überschreiten. Da ein Part einen Zeitbereich umfasst, „klebt" er am Hot-Volume,
bis auch seine jüngste Zeile alt genug ist.

→ **Partition-Granularität ≤ Tiering-Fenster** wählen. Bei 4-Wochen-Tiering
also nach Woche/Tag partitionieren, **nicht** nach Monat — sonst wandern Parts
zu spät und Merges mischen ständig Hot- und Cold-Daten.

Zweiter Move-Trigger: **`move_factor`** (oben 0.2). Unterschreitet das
Hot-Volume 20 % freien Platz, werden Parts auch *ohne* TTL nach cold geschoben
(kapazitätsbasiert). Schützt vor volllaufendem NVMe.

### Zero-Copy-Replication (das große Replikations-Thema)

Ohne Zero-Copy schreibt **jede Replica** ihre Parts nach S3 → bei Replikations-
faktor N liegt alles **N× im Bucket** (N× Storage-Kosten). Zero-Copy lässt die
Replicas sich **eine** S3-Kopie teilen; **Keeper hält die Refcounts**, jede
Replica hat lokal nur die Metadaten-Pointer.

Aktivieren (in `storage.xml`, siehe CHI):
```xml
<merge_tree>
  <allow_remote_fs_zero_copy_replication>1</allow_remote_fs_zero_copy_replication>
</merge_tree>
```

**Caveats — vor Prod ernst nehmen:**

- Historisch als „use with caution / not fully production-ready" markiert und
  mehrfach überarbeitet. **Version pinnen**, Release-Notes lesen, in kiddie-pool
  ausgiebig testen (inkl. Replica-Verlust + Wiederherstellung).
- **Objekt-Lifecycle hängt an Keeper-Refcounts.** Du darfst S3-Objekte/Prefixe
  **nicht manuell löschen** — sonst verwaiste oder fehlende Parts. Auch
  Backup/Restore muss zero-copy-aware sein (`clickhouse-backup` entsprechend
  konfigurieren).
- Bei Keeper-Datenverlust sind die Refcounts weg → potenziell verwaiste
  S3-Objekte. Keeper-HA (3 Replicas, `Retain`) ist hier doppelt wichtig.

Faustregel: **Zero-Copy nur einschalten, wenn die doppelten Storage-Kosten real
wehtun.** Sonst ist „jede Replica ihre Kopie" simpler und robuster.

### Kosten & Merges

- **S3 rechnet pro Request.** Viele kleine Parts → viele GET/PUT → teuer und
  langsam. Wenige große Inserts, grobe Partitionen, sinnvolle Part-Größen.
- **Merges auf Cold = Read+Write auf S3** (Traffic + Requests). Cold-Daten
  möglichst nicht mehr stark mergen lassen. (MinIO self-hosted: keine
  Request-Gebühr, aber IO-/CPU-Last bleibt.)
- **Filesystem-Cache** (`s3_cache.max_size`) auf den heißen Working-Set des
  Cold-Layers dimensionieren — der entscheidet über die Read-Latenz.

### MinIO-spezifisch

- Eigener Bucket nur für ClickHouse. **Keine MinIO-Lifecycle-/Expiry-Regeln**
  auf diesem Bucket — ClickHouse verwaltet den Objekt-Lifecycle selbst (bei
  Zero-Copy zwingend, sonst riskierst du Datenverlust).
- Endpoint/Path-Style/TLS und Connection-Limits beachten; ClickHouse parallelisiert
  S3-Requests stark (`s3_max_connections`).

### Engine-Hinweis

Storage-Policy + TTL gelten für die **gesamte MergeTree-Familie** inkl. aller
`Replicated*` (also auch `ReplicatedReplacingMergeTree`). Engine = Merge-Logik,
Policy = Speicherort, Partition/Indexes = Part-interne Struktur — orthogonal.
Skip-Indexes/Primary-Index funktionieren auf S3 identisch (kleine Index-Dateien
werden gecached, dann gezielte Range-GETs auf die `.bin`).
