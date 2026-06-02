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
