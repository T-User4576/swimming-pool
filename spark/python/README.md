# uv: Spickzettel für Python-Projekte

[uv](https://docs.astral.sh/uv/) ist der Paket- und Projektmanager für Python,
den wir für Spark-Jobs und Tools verwenden. Es ist ein einzelnes, sehr schnelles
Binary und ersetzt eine ganze Reihe einzelner Werkzeuge:

| Früher | Mit uv | Java-Analogie |
|---|---|---|
| `pyenv` | `uv python install 3.12` | SDKMAN |
| `python -m venv` + `activate` | passiert automatisch (`.venv/`) | – |
| `pip install` | `uv add` | Dependency in `pom.xml` eintragen |
| `pip-tools` / `poetry lock` | `uv lock` (`uv.lock`) | – (Maven hat kein echtes Lockfile) |
| `python -m build` + `twine upload` | `uv build` + `uv publish` | `mvn package` / `mvn deploy` |
| `pipx run` | `uvx` | – |

**Zwei Dinge sollte man sich merken:**

- Die `pyproject.toml` ist das, was die `pom.xml` in Maven ist. Das `uv.lock`
  hält die exakten Versionen fest und wird **immer mit committet**.
- Die venv muss man nie selbst aktivieren. `uv run <befehl>` sorgt vorher
  dafür, dass `.venv/` zum Lockfile passt.

---

## Installation

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
uv self update          # update later
```

Offline bzw. gegen Nexus statt PyPI: siehe die Index-Konfiguration in
[`../../starrocks/mcp/README.md`](../../starrocks/mcp/README.md#3-uv-konfigurieren-einmalig-pro-maschine).

---

## Weg A: Ein „richtiges“ Projekt

Das ist der richtige Weg für alles, was getestet, versioniert und ausgerollt
wird, also für jeden Spark-Job.

```bash
# 1. Create the project (src layout, buildable as a wheel)
uv init --package --python 3.10 my-job
cd my-job

# 2. Add dependencies
uv add pyiceberg                      # runtime dependency (also needed on the cluster)
uv add --dev pyspark==4.0.3 pytest    # local only (tests, IDE)

# 3. Work
uv run pytest                         # run tests
uv run python -m my_job               # run the job locally
uv run my-job                         # or via the entry point from [project.scripts]

# 4. Version + artefact
uv version --bump patch               # 0.1.0 -> 0.1.1
uv build                              # -> dist/my_job-0.1.1-py3-none-any.whl
```

> **Achtung, Python-Version:** Ohne `--python` übernimmt `uv init` die neueste
> lokal installierte Python-Version als `requires-python` (z. B. `>=3.14`).
> Gib hier deshalb die Version an, die im Runner-Image steckt. Nachschauen kannst
> du sie so:
> `docker run --rm apache/spark:4.0.3-python3 python3 --version`

Danach sieht das Projekt so aus:

```
my-job/
├── pyproject.toml        # dependencies + metadata, edited by hand or via `uv add`
├── uv.lock               # generated, gets committed, never edit by hand
├── .python-version       # Python version for local development
├── .venv/                # generated, NOT committed (already in .gitignore)
└── src/my_job/
    └── __init__.py
```

Die `pyproject.toml` (gekürzt):

```toml
[project]
name = "my-job"
version = "0.1.1"
requires-python = ">=3.10"
dependencies = [
    "pyiceberg>=0.8",
]

[project.scripts]
my-job = "my_job:main"

[dependency-groups]
dev = [
    "pyspark==4.0.3",   # "provided": already part of the Spark image
    "pytest>=9",
]

[build-system]
requires = ["uv_build>=0.13.0,<0.14.0"]
build-backend = "uv_build"
```

**Warum `pyspark` nur unter `dev` steht:** Das Spark-Image bringt PySpark
bereits mit. Würde es unter `dependencies` stehen, wäre es noch einmal im
Artefakt, und zwar mit über 300 MB, weil das PyPI-Paket alle Spark-JARs enthält.
Das entspricht `<scope>provided</scope>` in Maven. Die Version muss zur
Spark-Version im Cluster passen (aktuell **4.0.3**).

**In CI bzw. im Dockerfile** installiert man strikt nach Lockfile und ohne
dev-Abhängigkeiten:

```bash
uv sync --locked --no-dev       # fails if uv.lock does not match pyproject.toml
```

---

## Weg B: Nur ein kleines Script

Für Einmal-Skripte, Admin-Helfer oder ein schnelles Experiment braucht man kein
Projekt. Die Abhängigkeiten stehen direkt im Kopf der Datei (Standard PEP 723):

```bash
uv init --script check_tables.py --python 3.12   # creates the file with a header
uv add --script check_tables.py pyiceberg        # adds a dependency to the header
uv run check_tables.py                           # uv builds a temporary venv (cached) and runs it
```

Das Ergebnis:

```python
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pyiceberg>=0.8",
# ]
# ///

def main() -> None:
    ...

if __name__ == "__main__":
    main()
```

Die Datei kann man einfach weitergeben. Wer uv installiert hat, startet sie mit
`uv run check_tables.py`, ohne vorher irgendetwas zu installieren.

> **Grenze:** Für Spark-Jobs **auf dem Cluster** reicht das nicht. Dort
> brauchen auch die Executors die Abhängigkeiten, nicht nur der Prozess, der
> das Script startet. Die Varianten dafür beschreibt
> [`../kubernetes.md`](../kubernetes.md) (Code-Distribution).

**Noch kürzer:** Ein Tool einmalig ausführen, ohne es zu installieren:

```bash
uvx ruff check .                     # run a CLI tool ad hoc
uv run --with pandas python          # Python shell with pandas, nothing installed
```

---

## Befehle im Überblick

**Alltag im Projekt**

| Befehl | Was er tut |
|---|---|
| `uv add <paket>` | Abhängigkeit hinzufügen (pyproject + lock + venv) |
| `uv add --dev <paket>` | nur für Entwicklung und Tests |
| `uv remove <paket>` | Abhängigkeit entfernen |
| `uv run <befehl>` | Befehl in der Projekt-venv ausführen |
| `uv sync` | `.venv/` exakt auf den Stand des Lockfiles bringen |
| `uv lock --upgrade-package <paket>` | gezielt ein Paket aktualisieren |
| `uv lock --upgrade` | alle Pakete aktualisieren (mit Bedacht) |
| `uv tree` | Abhängigkeitsbaum anzeigen (wie `mvn dependency:tree`) |
| `uv export --format requirements-txt` | `requirements.txt` für Tools erzeugen, die kein uv kennen |

**Python-Versionen**

| Befehl | Was er tut |
|---|---|
| `uv python list` | installierte und verfügbare Versionen |
| `uv python install 3.12` | Version installieren |
| `uv python pin 3.12` | Version fürs Projekt festlegen (`.python-version`) |

**Bauen und Veröffentlichen**

| Befehl | Was er tut |
|---|---|
| `uv version` | aktuelle Version anzeigen |
| `uv version --bump patch\|minor\|major` | Version erhöhen |
| `uv build` | Wheel und sdist nach `dist/` bauen |
| `uv publish --index <name>` | in einen Index hochladen (z. B. Nexus) |

**Tools und Aufräumen**

| Befehl | Was er tut |
|---|---|
| `uvx <tool>` | CLI-Tool ad hoc ausführen |
| `uv tool install <tool>` | CLI-Tool dauerhaft installieren |
| `uv cache clean` | lokalen Paket-Cache leeren |

---

## Was uv nicht macht

uv baut **keine Docker-Images**. Es läuft **im** Dockerfile und installiert
dort die Abhängigkeiten. Image bauen und pushen übernehmen
`docker build` / `docker push` bzw. die CI.

Vorlage dafür: [`Dockerfile.template`](./Dockerfile.template) und
[`.dockerignore`](./.dockerignore). Beide neben die `pyproject.toml` kopieren
(Template als `Dockerfile`). Abhängigkeiten und eigener Code liegen in
getrennten Layern, sodass eine Code-Änderung nur den letzten, wenige KB großen
Layer neu baut. Die `.dockerignore` ist eine Allow-List: Nur
`pyproject.toml`, `uv.lock`, `README.md` und `src/` gelangen in den
Build-Kontext.

> Das Template basiert auf `python:3.12-slim` und eignet sich für reine
> Python-Jobs. Für PySpark fehlt dort die Java-Runtime. Spark-Jobs bauen
> deshalb auf dem Spark-Image auf.
