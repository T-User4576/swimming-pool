# Lakekeeper UI-Preview — benötigte DuckDB-Extensions

Die „Preview"-Funktion der Lakekeeper-UI (Tabelleninhalt im Browser ansehen)
läuft auf **DuckDB-WASM**: Die Query-Engine wird als WebAssembly mit dem
UI-Bundle ausgeliefert und im Browser ausgeführt. Sie liest Iceberg-Metadaten
und Parquet-Dateien **direkt aus dem Object Store** (kein Server-seitiges Query).

Um Iceberg über S3/HTTP zu lesen, lädt DuckDB-WASM zur Laufzeit zwei
**Extensions** nach (`INSTALL`/`LOAD`). Diese liegen *nicht* im UI-Bundle,
sondern werden per HTTP von einem Extension-Repository geholt — standardmäßig
`https://extensions.duckdb.org`. In **Offline-/Air-Gap-Umgebungen** schlägt
dieser Download fehl, und die Preview bricht mit einem DuckDB-Extension-Fehler
ab. Dieses Dokument listet, was für einen internen Mirror gespiegelt werden muss.

## Benötigte Extensions

| Extension | Pflicht | Wozu |
|---|---|---|
| `httpfs` | ja | HTTP-/S3-Zugriff (die Parquet-/Metadaten-Dateien liegen im Object Store) |
| `iceberg` | ja | Iceberg-Tabellen lesen (`iceberg_metadata()`, `iceberg_scan()`) |
| `parquet` | nein | Parquet-Reader ist **im DuckDB-Core enthalten** — kein separates `INSTALL` nötig |

Hinweis: Der `iceberg`-Extension liest die Avro-Manifeste selbst — eine separate
`avro`-Extension ist für den Lesepfad normalerweise **nicht** erforderlich. Welche
Extensions die eingesetzte UI-Version konkret `INSTALL`t, lässt sich verbindlich
im Browser-DevTools nachsehen (s.u.) — `httpfs` und `iceberg` sind der Kern.

## Bezugsquelle und Pfad-Schema

DuckDB baut die Download-URL nach festem Muster:

```
{repository}/{duckdb_version}/{platform}/{extension}.duckdb_extension.wasm
   z.B.  https://extensions.duckdb.org/v1.x.x/wasm_eh/iceberg.duckdb_extension.wasm
```

- `{duckdb_version}` — exakte DuckDB-Version der UI-WASM-Engine (z.B. `v1.x.x`).
- `{platform}` — für WASM **`wasm_eh`** (Exception-Handling, moderner Standard);
  je nach Browser zusätzlich `wasm_mvp` bzw. `wasm_threads`.
- WASM-Builds liegen unter `…/{platform}/…` — **nicht** die nativen
  `.duckdb_extension`-Dateien verwenden, sondern die `…wasm…`-Varianten.

## Offline-Mirror einrichten

Die UI hat dafür ein eingebautes Feld: **Settings → „Custom extension
repository URL"**. Es setzt intern `SET custom_extension_repository = '<URL>'`.
Leer = Default (`extensions.duckdb.org`).

> **Wo wird das konfiguriert?** Das ist eine **Client-seitige UI-Einstellung**,
> **keine** Env-Variable und kein Helm-Wert am Lakekeeper-Deployment. Die
> DuckDB-WASM-Engine läuft im Browser, also wird auch dort gesetzt, woher sie
> ihre Extensions lädt — die UI führt `SET custom_extension_repository=…` auf der
> Browser-Session aus und merkt sich den Wert lokal (Browser-Settings/localStorage),
> **pro Browser/User**.
>
> Folge für Orgs: Es gibt **keinen zentralen Server-Default** dafür (im UI-Bundle
> nicht vorhanden). Jeder Nutzer trägt die Mirror-URL einmal selbst ein, oder ihr
> verteilt sie über Browser-Policy/localStorage-Vorbelegung. Das ist eine
> Eigenheit von DuckDB-WASM (Engine im Browser), nicht von Lakekeeper.

1. **Exakte Dateinamen ermitteln** (zuverlässig, versionsfest): Auf einer Maschine
   *mit* Netz die Preview öffnen, DevTools → Network, nach `extensions.duckdb.org`
   filtern. Die geladenen URLs zeigen Version, Plattform und Extension-Namen.
2. **Dateien herunterladen** — die offiziellen `.wasm` sind **signiert**, dann ist
   in DuckDB kein `allow_unsigned_extensions` nötig.
3. **Intern hosten**, mit **identischem Pfad-Layout** unter einer vom Browser
   erreichbaren Base-URL:
   ```
   https://<interner-host>/{duckdb_version}/{platform}/{extension}.duckdb_extension.wasm
   ```
4. In der UI **„Custom extension repository URL"** auf `https://<interner-host>`
   setzen — **Base-URL ohne** `/{version}/…`, den Rest hängt DuckDB selbst an.

## Stolpersteine

- **Beide** Extensions spiegeln: `httpfs` **und** `iceberg`.
- **Plattform-Varianten**: mindestens `wasm_eh`; zur Sicherheit `wasm_mvp` und
  `wasm_threads` mitnehmen (welche der Browser wählt, hängt von dessen Fähigkeiten ab).
- **Versions-Match**: Der Versionspfad muss exakt zur DuckDB-WASM-Version der UI
  passen. Nach einem Lakekeeper-/UI-Upgrade kann sich die DuckDB-Version ändern →
  Mirror entsprechend nachziehen (siehe [`upgrade.md`](./upgrade.md)).
- **CORS**: Der Browser holt die Extensions cross-origin → der Mirror-Host muss
  den UI-Origin per CORS erlauben (analog zum Object-Store-Zugriff der Preview).
- **Core-Engine**: Die DuckDB-WASM-Engine selbst (`duckdb-eh.wasm` etc.) kommt aus
  dem UI-Bundle und funktioniert offline — nur die o.g. Extensions fehlen.

## Privater Bucket: Zugriff per STS-Vended-Credentials

Die UI-Preview (DuckDB-WASM) kann **kein Remote Signing** — sie attached nur den
REST-Catalog und konsumiert **vended credentials** aus der `loadTable`-Antwort.
Auf einem **privaten** Bucket funktioniert die Preview daher ausschließlich mit
**STS** (`AssumeRole`). Remote Signing bleibt allein für Spark/pyiceberg/Trino;
für die Testbank ist die Alternative ein öffentlich lesbarer Bucket (s. kiddie-pool).

### 1. In MinIO — am besten über die MinIO-Console (`:9001`)

- **User** anlegen (Identity → Users): Username = Access Key, Passwort = Secret.
  **Kein Service Account** — der kann kein `AssumeRole`; es muss ein echter User
  sein. Lakekeeper ruft mit diesem User selbst `AssumeRole` auf und vendet dem
  Browser kurzlebige Creds.
- **Policy** anlegen (Identity → Policies) und dem User zuweisen — mit
  `s3:GetObject`/`PutObject`/`DeleteObject`/`ListBucket`/`GetBucketLocation` auf
  `arn:aws:s3:::<bucket>` **und** `arn:aws:s3:::<bucket>/*`. Die vended Creds sind
  nie mehr als diese Policy.

### 2. Im Lakekeeper-Warehouse (Storage-Settings)

| Feld | Wert |
|---|---|
| Credential-Type / Access Key / Secret | `access-key` + der **MinIO-User** aus Schritt 1 |
| **Enable STS** (`sts-enabled`) | **an** |
| Assume Role ARN | Dummy, z. B. `arn:aws:iam::123456789012:role/dummy` — MinIO ignoriert ihn |
| Remote Signing (`remote-signing-enabled`) | **an lassen** |
| Remote signing URL style | `path` (für MinIO) |
| Endpoint | **browser-erreichbarer Host** (gleicher Name innen wie außen) |

`sts-enabled` und `remote-signing-enabled` dürfen **gleichzeitig** an sein — es
sind zwei unabhängige Flags. Der Client wählt den Modus per Iceberg-REST-Header
`X-Iceberg-Access-Delegation`: der **Browser** fordert `vended-credentials`, **Spark**
`remote-signing`. So funktionieren beide parallel auf demselben privaten Bucket.

Ergebnis: Der Browser holt sich Temp-Creds (erkennbar am `x-amz-security-token` im
S3-GET) und liest den **privaten** Bucket; Spark signiert weiter per Remote Signing.
