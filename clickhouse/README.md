# ClickHouse — Operator-Wahl & Hardening

Kurz-Dokumentation für den ClickHouse-Betrieb. Basis: **Altinity Kubernetes
Operator for ClickHouse** (`ClickHouseInstallation`/CHI-CRD) + **ClickHouse
Keeper** als Coordination-Backend.

## Operator-Entscheidung: Altinity bleibt

Seit 2025 gibt es zusätzlich einen offiziellen Operator von ClickHouse Inc.
**Wir bleiben bewusst beim Altinity-Operator:**

- Altinity ist der ausgereifte De-facto-Standard (Jahre in Prod, aktiv gepflegt).
- Der offizielle Operator ist neu, nutzt **eigene, inkompatible CRDs** und hat
  **keinen dokumentierten Migrationspfad** von Altinity.
- Ein Operator besitzt die Daten nicht (die liegen auf PVCs + werden von
  ClickHouse/Keeper verwaltet) — aber ein Wechsel ist faktisch eine
  **Datenmigration**, kein In-Place-Umstieg. Lohnt das Risiko aktuell nicht.

→ Wechsel nur bei konkretem Schmerzpunkt, und dann nur als Parallel-Migration
(neuer Cluster, Daten kopieren) mit Backup + kiddie-pool-Test vorab.

## Verzeichnis-Inhalt

| Pfad | Zweck |
|---|---|
| `chi-hardened.yaml` | Beispiel-CHI: gehärteter SecurityContext, Linkerd-Annotations, saubere Config-Struktur |
| `chk-keeper.yaml` | Beispiel-CHK (ClickHouse Keeper): 3 Replicas, gehärtet, bewusst ungemesht |
| `s3-tiered-storage.md` | Hot/Cold-Tiering auf MinIO/S3: TTL-Regeln, wo konfigurieren, Zero-Copy, Kosten-Fallstricke |
| `operator-hardening.yaml` | Helm-Values-Snippet zum Härten des Altinity-Operator-Pods selbst |

## Hardening-Kurzreferenz (neues Cluster mit restricted PSA + Linkerd)

### SecurityContext

- ClickHouse läuft als **UID/GID 101** (`clickhouse`-User), seit v21.1 fix.
- `runAsNonRoot: true`, `runAsUser/Group: 101`, **`fsGroup: 101`** (sonst kein
  Schreibzugriff aufs PVC).
- **`fsGroupChangePolicy: OnRootMismatch`** ist Pflicht bei großen Volumes —
  sonst rekursiver `chown` über `/var/lib/clickhouse` bei jedem Start
  (Minuten bis Stunden).
- **Kein `chown`-initContainer** (braucht root, kollidiert mit runAsNonRoot) —
  `fsGroup` ersetzt das vollständig.
- `seccompProfile: RuntimeDefault` (von `restricted` verlangt).
- `readOnlyRootFilesystem` erstmal **`false`** lassen (von `restricted` nicht
  verlangt, sonst emptyDir-Mounts für `/tmp`, `/var/log/...` nötig).

### Capabilities — der zentrale Stolperstein

ClickHouse *will* `IPC_LOCK` (mlock) + `SYS_NICE` (Thread-Prio), **braucht sie
aber nicht** — ohne sie nur harmlose Log-Warnings. Das `restricted`-PSA-Profil
**verbietet jedes `capabilities.add`** außer `NET_BIND_SERVICE`.

→ Entscheidung: **`drop: ["ALL"]`, nichts adden.** Performance-Verlust
vernachlässigbar. Nur wenn die Caps zwingend gewünscht sind: auf `baseline`
+ eigene Policy statt `restricted` gehen.

### Linkerd

- **Opaque Ports** setzen (sonst L7-Detection-Timeouts / Verbindungsabbrüche):
  `config.linkerd.io/opaque-ports: "9000,9009,9181,9234"`
  (9000 native, 9009 interserver, 9181 Keeper client, 9234 Keeper raft;
  8123 HTTP darf L7 bleiben). mTLS bleibt aktiv.
- **`config.linkerd.io/proxy-await: "enabled"`** — sonst Startup-Race:
  ClickHouse verbindet zu Keeper, bevor der Proxy ready ist.
- **Keeper anfangs aus dem Mesh lassen** (Injection skippen) — Raft + Mesh
  ist heikel. Später einbeziehen, dann alle Keeper-Ports opaque + proxy-await.
- **Backup-Jobs** (`clickhouse-backup` als K8s-`Job`) terminieren nicht, weil
  der Proxy weiterläuft → `linkerd-await --shutdown -- <cmd>` als Wrapper.
- Auch der **Operator-Pod** muss restricted-PSA-konform sein → siehe
  `operator-hardening.yaml`.

### Config-Hygiene (Wildwuchs vermeiden)

- **RBAC nach SQL holen**: ein Bootstrap-User mit `access_management: 1` in XML,
  Rest über SQL-RBAC mit **`replicated` access storage** (in Keeper) → clusterweit
  konsistent, kein XML-Drift zwischen Replicas.
- CHI sauber strukturieren: `settings` / `profiles` / `quotas` / `users` statt
  roher XML-Blobs; `files` nur wo wirklich nötig.
- Passwörter als K8s-`Secret` (`valueFrom`), nicht Plaintext/SHA im CHI.
- Image-Version **explizit pinnen**, kein `latest`.
