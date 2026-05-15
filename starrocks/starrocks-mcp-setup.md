# StarRocks MCP Server – Installation (manuell)

## 1. Repo klonen & installieren

```bash
git clone https://github.com/StarRocks/mcp-server-starrocks.git
cd mcp-server-starrocks
pip install -e . --break-system-packages
```

## 2. OpenCode Config anpassen

Datei: `~/.config/opencode/config.json`

```json
{
  "mcp": {
    "starrocks": {
      "type": "local",
      "command": "mcp-server-starrocks",
      "env": {
        "STARROCKS_HOST": "dein-host",
        "STARROCKS_PORT": "9030",
        "STARROCKS_USER": "root",
        "STARROCKS_PASSWORD": "dein-passwort",
        "STARROCKS_DB": "deine-datenbank"
      },
      "enabled": true
    }
  }
}
```

## 3. Updates

```bash
cd mcp-server-starrocks
git pull
pip install -e . --break-system-packages
```
