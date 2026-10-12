# PoC — OIDC-Token → Rechte pro Backend

`fga_access.py` liest aus einem OIDC-Token User und Gruppen und fragt OpenFGA,
welche Rollen (= DB-User) der User auf welchem Backend hat. Zwei Modi:

- **Entscheidung** (`--backend <b>`, `allowed_roles_on(token, backend)`) —
  pro Request. Exakt per `BatchCheck`. Leere Liste = ablehnen; bei mehreren
  Rollen wählt der Aufrufer **einen** DB-User (nicht Teil des PoC).
- **Übersicht** (Default, `access_by_backend(token)`) — nur zur Anzeige.
  `ListObjects` kann bei Deadline/Max-Results ohne Fehler eine gekürzte Liste
  liefern.

```bash
export FGA_API_URL=http://localhost:8080 FGA_STORE_ID=<id> FGA_API_TOKEN=<preshared key>
export OIDC_ISSUER=https://<keycloak>/realms/<realm> OIDC_AUDIENCE=<client>
uv run fga_access.py <jwt>              # Übersicht, Signatur via JWKS geprüft
uv run fga_access.py --backend pg <jwt> # Entscheidung für ein Backend
uv run fga_access.py --no-verify <jwt>  # nur lokal
```

Ergebnis, z. B. für Gruppen `Data` + `Finance`:

```json
// --backend pg
{"user": "neu", "groups": ["Data", "Finance"], "backend": "pg",
 "roles": ["pg.read_restricted", "pg.write"]}
// Übersicht
{"user": "neu", "groups": ["Data", "Finance"],
 "backends": {"ch": ["ch.read"], "pg": ["pg.read_restricted", "pg.write"]}}
```

## Annahmen

- FGA-Gruppen-IDs = Gruppennamen im Token (Keycloak-Pfad-Präfix `/` wird entfernt).
- FGA-Person-ID = Claim `preferred_username` (`OIDC_USER_CLAIM`), sonst `sub`.
- Gruppen aus dem Token gehen als **Contextual Tuples** in die Abfrage —
  Mitgliedschaften müssen nicht in OpenFGA gespeichert sein. Direkte Grants
  an Personen und Bundles löst das Modell selbst auf.

## Abfragen

Entscheidung:

1. `Read(backend:<b>#has_role)` → Kandidaten-Rollen (ändert sich selten, cachebar)
2. `BatchCheck(person:<user> assignee role:<r>)` je Kandidat, in Blöcken à 50
   (`maxChecksPerBatchCheck`). Fehler eines Einzel-Checks → Exception, nicht
   „verweigert“.

Übersicht:

1. `ListObjects(role#assignee)` → alle effektiven Rollen
2. `ListObjects(backend#accessor)` → erreichbare Backends
3. `Read(backend:<b>#has_role)` → Rollen je Backend, Schnittmenge mit 1.

`Read` akzeptiert kein reines `backend:` ohne User, daher der Umweg über 2.
