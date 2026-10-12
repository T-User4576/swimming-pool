# /// script
# requires-python = ">=3.11"
# dependencies = ["httpx>=0.27", "pyjwt[crypto]>=2.8"]
# ///
"""
PoC: OIDC token -> roles (= DB users) per backend, resolved via OpenFGA.

Common to both modes:
  Verify the JWT (JWKS, iss, aud, exp) and read the user + the groups claim.
  The groups go into every query as CONTEXTUAL tuples
  (person:<user> member group:<g>), so memberships come from the token and
  need not be stored in OpenFGA. Direct grants and bundles are resolved by
  the model.

Decision (`allowed_roles_on`, CLI `--backend <b>`) - use this per request:
  1. Read(backend:<b>#has_role)  -> candidate roles (cacheable, rarely changes)
  2. BatchCheck(assignee) on each candidate -> exact allowed roles.
  Picking ONE role out of several is left to the caller.

Overview (`access_by_backend`, CLI default) - for display only, ListObjects
may return a truncated list on deadline/max-results without an error:
  1. ListObjects(role#assignee)     -> all effective roles.
  2. ListObjects(backend#accessor)  -> reachable backends.
  3. Read(backend:<b>#has_role)     -> roles per backend, intersected with 1.

Assumptions:
  - FGA group ids equal the group names in the token (Keycloak's leading "/"
    of full group paths is stripped).
  - FGA person ids equal the token's user claim (default `preferred_username`).

Usage:
  export FGA_API_URL=http://localhost:8080 FGA_STORE_ID=<id> FGA_API_TOKEN=<key>
  export OIDC_ISSUER=https://kc.example/realms/lake OIDC_AUDIENCE=data-service
  uv run fga_access.py <jwt>                 # overview, verified
  uv run fga_access.py --backend pg <jwt>    # decision for one backend
  uv run fga_access.py --no-verify <jwt>     # local testing only
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, field

import httpx
import jwt


@dataclass
class Settings:
    fga_url: str = field(default_factory=lambda: os.environ.get("FGA_API_URL", "http://localhost:8080").rstrip("/"))
    store_id: str = field(default_factory=lambda: os.environ.get("FGA_STORE_ID", ""))
    model_id: str | None = field(default_factory=lambda: os.environ.get("FGA_MODEL_ID") or None)
    api_token: str | None = field(default_factory=lambda: os.environ.get("FGA_API_TOKEN") or None)
    issuer: str | None = field(default_factory=lambda: os.environ.get("OIDC_ISSUER") or None)
    audience: str | None = field(default_factory=lambda: os.environ.get("OIDC_AUDIENCE") or None)
    groups_claim: str = field(default_factory=lambda: os.environ.get("OIDC_GROUPS_CLAIM", "groups"))
    user_claim: str = field(default_factory=lambda: os.environ.get("OIDC_USER_CLAIM", "preferred_username"))


# ---------------------------------------------------------------------------
# Token
# ---------------------------------------------------------------------------

def read_identity(token: str, s: Settings, verify: bool = True) -> tuple[str, list[str]]:
    """Return (user id, group names) from the JWT."""
    if verify:
        if not (s.issuer and s.audience):
            raise ValueError("OIDC_ISSUER and OIDC_AUDIENCE are required for verification")
        jwks = jwt.PyJWKClient(f"{s.issuer}/protocol/openid-connect/certs")  # Keycloak layout
        key = jwks.get_signing_key_from_jwt(token).key
        claims = jwt.decode(token, key, algorithms=["RS256", "ES256"],
                            audience=s.audience, issuer=s.issuer)
    else:
        claims = jwt.decode(token, options={"verify_signature": False})

    user = claims.get(s.user_claim) or claims["sub"]
    groups = [g.rsplit("/", 1)[-1] for g in claims.get(s.groups_claim, []) if g]
    return user, groups


# ---------------------------------------------------------------------------
# OpenFGA (plain HTTP API)
# ---------------------------------------------------------------------------

# Server default `maxChecksPerBatchCheck` (see ../config.md).
BATCH_CHECK_MAX = 50


class Fga:
    def __init__(self, s: Settings):
        if not s.store_id:
            raise ValueError("FGA_STORE_ID is required")
        headers = {"Authorization": f"Bearer {s.api_token}"} if s.api_token else {}
        self._http = httpx.Client(base_url=f"{s.fga_url}/stores/{s.store_id}",
                                  headers=headers, timeout=5)
        self._model_id = s.model_id

    def _post(self, path: str, body: dict) -> dict:
        if self._model_id and path != "/read":
            body["authorization_model_id"] = self._model_id
        resp = self._http.post(path, json=body)
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def _ctx(user: str, groups: list[str]) -> dict:
        """Token groups as contextual tuples."""
        return {"tuple_keys": [{"user": f"person:{user}", "relation": "member",
                                "object": f"group:{g}"} for g in groups]}

    def _list(self, type_: str, relation: str, user: str, groups: list[str]) -> list[str]:
        """ListObjects with the token groups as contextual tuples."""
        body = self._post("/list-objects", {
            "type": type_,
            "relation": relation,
            "user": f"person:{user}",
            "contextual_tuples": self._ctx(user, groups),
        })
        return [o.removeprefix(f"{type_}:") for o in body["objects"]]

    def roles_of(self, user: str, groups: list[str]) -> list[str]:
        """All roles the user reaches (direct, via token groups, via bundles)."""
        return self._list("role", "assignee", user, groups)

    def backends_of(self, user: str, groups: list[str]) -> list[str]:
        """Backends on which the user holds at least one role."""
        return self._list("backend", "accessor", user, groups)

    def roles_on(self, backend: str) -> set[str]:
        """All roles bound to a backend (`backend:<b> has_role role:*`, paginated).
        Read needs a full object id here; a bare `backend:` requires a user."""
        roles: set[str] = set()
        token = ""
        while True:
            body = self._post("/read", {
                "tuple_key": {"relation": "has_role", "object": f"backend:{backend}"},
                "page_size": 100,
                "continuation_token": token,
            })
            roles |= {t["key"]["user"].removeprefix("role:") for t in body["tuples"]}
            token = body.get("continuation_token") or ""
            if not token:
                return roles

    def check_roles(self, user: str, groups: list[str], candidates: list[str]) -> list[str]:
        """BatchCheck `person:<user> assignee role:<r>` for each candidate.
        Fails closed: any per-check error raises instead of being read as deny."""
        allowed: list[str] = []
        for i in range(0, len(candidates), BATCH_CHECK_MAX):
            chunk = candidates[i:i + BATCH_CHECK_MAX]
            body = self._post("/batch-check", {"checks": [{
                "tuple_key": {"user": f"person:{user}", "relation": "assignee",
                              "object": f"role:{r}"},
                "contextual_tuples": self._ctx(user, groups),
                "correlation_id": str(n),
            } for n, r in enumerate(chunk)]})
            for n, r in enumerate(chunk):
                res = body["result"][str(n)]
                if res.get("error"):
                    raise RuntimeError(f"check failed for role:{r}: {res['error']}")
                if res.get("allowed"):
                    allowed.append(r)
        return allowed


def access_by_backend(token: str, s: Settings | None = None, verify: bool = True) -> dict:
    """Public entry point: JWT -> {"user", "groups", "backends": {backend: [roles]}}."""
    s = s or Settings()
    user, groups = read_identity(token, s, verify)
    fga = Fga(s)
    roles = set(fga.roles_of(user, groups))
    backends = {b: sorted(roles & fga.roles_on(b)) for b in sorted(fga.backends_of(user, groups))}
    return {"user": user, "groups": groups, "backends": backends}


def allowed_roles_on(token: str, backend: str, s: Settings | None = None,
                     verify: bool = True) -> dict:
    """Public entry point for the access decision:
    JWT + backend -> {"user", "groups", "backend", "roles": [allowed roles]}.
    Empty `roles` = deny. Several roles = caller picks one DB user."""
    s = s or Settings()
    user, groups = read_identity(token, s, verify)
    fga = Fga(s)
    candidates = sorted(fga.roles_on(backend))
    return {"user": user, "groups": groups, "backend": backend,
            "roles": fga.check_roles(user, groups, candidates)}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("token", nargs="?", help="JWT (default: read from stdin)")
    p.add_argument("--backend", help="decide for this backend via BatchCheck")
    p.add_argument("--no-verify", action="store_true", help="skip signature check (testing only)")
    a = p.parse_args()
    token = (a.token or sys.stdin.read()).strip()
    verify = not a.no_verify
    result = (allowed_roles_on(token, a.backend, verify=verify) if a.backend
              else access_by_backend(token, verify=verify))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
