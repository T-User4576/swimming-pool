# OpenCode Custom Commands

Kurz-Notiz: wie man wiederkehrende Analyse-Abläufe als Slash-Command in OpenCode
hinterlegt — die praktikable Alternative zu "Skills" für dieses MCP-Setup.

## Prinzip

Ein Custom Command ist eine Markdown-Datei: optionales YAML-Frontmatter plus
Prompt-Text im Body. Beim Aufruf schickt OpenCode den Body — mit eingesetzten
Argumenten — ans LLM. Damit lässt sich ein fester Ablauf ("Schema holen,
Partitionierung prüfen, Queries vorschlagen") als **ein** Befehl konservieren,
statt ihn jedes Mal neu zu formulieren.

- **Ablageort**: `.opencode/commands/<name>.md` (projektweit) oder
  `~/.config/opencode/commands/<name>.md` (global)
- **Aufruf**: `/<name> <argumente>` in OpenCode
- **Argumente**: `$1`, `$2`, … oder `$ARGUMENTS` im Body
- **Extras**: `` !`shell-cmd` `` injiziert Shell-Output, `@datei` bindet eine Datei ein

Der Command orchestriert nur — die eigentliche Arbeit machen die MCP-Tools.

## Beispiel: `/analyze-table`

Datei `.opencode/commands/analyze-table.md`:

```markdown
---
description: Strukturierte Analyse einer Lakehouse-Tabelle
---
Analysiere die Tabelle $1 (Format: namespace.tabelle, letzte Ebene = Tabellenname).

1. Hol Schema, Kommentare und Partition-Spec über das Lakekeeper-MCP
   (Tool `describe_table`).
2. Prüfe anhand der Partition-Spec, welche WHERE-Filter Partition-Pruning
   auslösen — und welche nicht.
3. Schlage 3 typische Analyse-Queries vor. Namespace immer mit Backticks quoten:
   SELECT ... FROM lake.`<namespace>`.<tabelle> WHERE ...
4. Wenn sinnvoll: eine der Queries via StarRocks-MCP (Tool `read_query`)
   ausführen und das Ergebnis kurz einordnen.
```

Aufruf: `/analyze-table gold.finance.orders`

## Bezug zu den MCP-Servern

Der Command nutzt die Tools, die die beiden MCP-Server bereitstellen:
`describe_table` / `list_snapshots` aus dem Lakekeeper-MCP (siehe `server.py`)
und `read_query` / `analyze_query` aus dem StarRocks-MCP. Custom Commands sind
die nutzer-ausgelöste Schicht darüber — immer-aktive Konventionen gehören
dagegen in `AGENTS.md`, das OpenCode nativ als Regel-Datei einliest.

> Frontmatter-Felder und Syntax können sich ändern — im Zweifel gegen
> `opencode.ai/docs/commands` prüfen.
