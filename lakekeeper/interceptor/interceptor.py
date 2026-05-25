"""
Lakekeeper role-sync token interceptor (receiver).

HTTP server on :8200. Receives every Lakekeeper request mirrored from an
upstream nginx (headers only, fire-and-forget). Reads the bearer token,
verifies it against the Keycloak JWKS, maps the `groups` claim to a
Lakekeeper role and assigns it — provided the user does not have a role
yet.

Guarantees:
- Always responds quickly with 200/204 (the mirror must not stall).
- Actual work happens on a background thread.
- Verifies EVERY token (signature via JWKS, iss, aud, exp) before trusting
  the groups claim.
- In-memory cache per `sub`: each user is processed at most once per
  receiver lifetime (hot path = 1× JWT decode + set lookup).
- Manually assigned roles are never overwritten.

Mapping: ROLE_MAPPING_<ROLE>=group1,group2,... read from env vars.
API reference: /docs/docs/api/management-open-api.yaml in the lakekeeper
repo.
"""
import base64
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock

import jwt
import requests
from jwt import PyJWKClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("interceptor")

KEYCLOAK_URL = os.environ["KEYCLOAK_URL"].rstrip("/")
KEYCLOAK_REALM = os.environ["KEYCLOAK_REALM"]
KEYCLOAK_CLIENT_ID = os.environ["KEYCLOAK_CLIENT_ID"]
KEYCLOAK_CLIENT_SECRET = os.environ["KEYCLOAK_CLIENT_SECRET"]
KEYCLOAK_AUDIENCE = os.environ.get("KEYCLOAK_AUDIENCE", "lakekeeper")
LAKEKEEPER_URL = os.environ.get("LAKEKEEPER_URL", "http://127.0.0.1:8182").rstrip("/")
OIDC_GROUPS_CLAIM = os.environ.get("OIDC_GROUPS_CLAIM", "groups")
LISTEN_PORT = int(os.environ.get("LISTEN_PORT", "8200"))

ISSUER = f"{KEYCLOAK_URL}/realms/{KEYCLOAK_REALM}"
JWKS_URL = f"{ISSUER}/protocol/openid-connect/certs"
TOKEN_URL = f"{ISSUER}/protocol/openid-connect/token"

# JWKS client caches the Keycloak public keys internally (key rotation is
# handled transparently — new kid -> refresh).
_jwks = PyJWKClient(JWKS_URL, cache_keys=True, lifespan=3600)

# Worker pool for the actual processing. Kept small because each user is
# touched only once; 4 threads are enough for peak login bursts.
_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="rolesync")

# In-memory cache of subs we've already seen.
_handled: set[str] = set()
_handled_lock = Lock()

# Writer token (svc-lakekeeper-sync) — cached until shortly before expiry.
_writer_token: dict = {"value": None, "expires_at": 0.0}
_writer_lock = Lock()


def load_mapping() -> dict[str, str]:
    """ROLE_MAPPING_<ROLE>=group1,group2 -> {group: ROLE}.
    First hit in alphabetically sorted role order wins."""
    group_to_role: dict[str, str] = {}
    prefix = "ROLE_MAPPING_"
    for key in sorted(os.environ):
        if key.startswith(prefix):
            role = key[len(prefix):]
            for group in (g.strip() for g in os.environ[key].split(",")):
                if group:
                    group_to_role.setdefault(group, role)
    return group_to_role


GROUP_TO_ROLE = load_mapping()


# ---------------------------------------------------------------------------
# Writer token (client_credentials)
# ---------------------------------------------------------------------------

def get_writer_token() -> str:
    """Fetch/cache the writer token. Refresh once only < 30s validity left."""
    with _writer_lock:
        if _writer_token["value"] and _writer_token["expires_at"] - time.time() > 30:
            return _writer_token["value"]
        resp = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": KEYCLOAK_CLIENT_ID,
                "client_secret": KEYCLOAK_CLIENT_SECRET,
            },
            timeout=10,
        )
        resp.raise_for_status()
        body = resp.json()
        _writer_token["value"] = body["access_token"]
        _writer_token["expires_at"] = time.time() + int(body.get("expires_in", 60))
        return _writer_token["value"]


# ---------------------------------------------------------------------------
# Lakekeeper management API
# ---------------------------------------------------------------------------

def _lk_request(method: str, path: str, body: dict | None = None) -> requests.Response:
    """One retry iteration: on 401, refresh the writer token and retry once."""
    for attempt in (1, 2):
        token = get_writer_token()
        resp = requests.request(
            method,
            f"{LAKEKEEPER_URL}{path}",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json=body,
            timeout=10,
        )
        if resp.status_code == 401 and attempt == 1:
            with _writer_lock:
                _writer_token["value"] = None
            continue
        return resp
    return resp


def get_role_id(role_name: str) -> str | None:
    """Look up a role id (case-insensitive compare, exact at create time)."""
    resp = _lk_request("GET", "/management/v1/role")
    resp.raise_for_status()
    target = role_name.upper()
    for role in resp.json().get("roles", []):
        if role["name"].upper() == target:
            return role["id"]
    return None


def user_has_role(lk_user_id: str, role_id: str) -> bool:
    """True if the user is listed as an assignee of this role."""
    resp = _lk_request("GET", f"/management/v1/permissions/role/{role_id}/assignments")
    resp.raise_for_status()
    for item in resp.json().get("assignments", []):
        if (
            item.get("type") == "assignee"
            and item.get("user") == lk_user_id
        ):
            return True
    return False


def user_has_any_role(lk_user_id: str) -> bool:
    """True if the user is already an assignee of any mapped role.
    Deliberately checking ALL roles, not just ROLE_MAPPING_* targets: the
    rule is 'anyone who got a role manually stays untouched', which has
    to hold for every existing role."""
    resp = _lk_request("GET", "/management/v1/role")
    resp.raise_for_status()
    for role in resp.json().get("roles", []):
        rid = role["id"]
        ar = _lk_request("GET", f"/management/v1/permissions/role/{rid}/assignments")
        ar.raise_for_status()
        for item in ar.json().get("assignments", []):
            if item.get("type") == "assignee" and item.get("user") == lk_user_id:
                return True
    return False


def assign_role(role_id: str, lk_user_id: str) -> None:
    resp = _lk_request(
        "POST",
        f"/management/v1/permissions/role/{role_id}/assignments",
        {"writes": [{"user": lk_user_id, "type": "assignee"}]},
    )
    resp.raise_for_status()


# ---------------------------------------------------------------------------
# JWT verification
# ---------------------------------------------------------------------------

def _unverified_sub(token: str) -> str | None:
    """Read sub from the JWT payload WITHOUT signature check (hot-path cache
    lookup). NOT used for the actual decision — only to know whether we
    have already processed this user."""
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        payload_b64 = parts[1]
        payload_b64 += "=" * (-len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
        return payload.get("sub")
    except Exception:
        return None


def verify(token: str) -> dict | None:
    """Verify signature, iss, aud, exp. Returns claims or None."""
    try:
        signing_key = _jwks.get_signing_key_from_jwt(token).key
        return jwt.decode(
            token,
            signing_key,
            algorithms=["RS256", "RS384", "RS512", "ES256", "ES384", "ES512"],
            issuer=ISSUER,
            audience=KEYCLOAK_AUDIENCE,
            options={"require": ["exp", "iss", "aud", "sub"]},
        )
    except jwt.InvalidTokenError as exc:
        log.warning("Token invalid: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------

def process_user(token: str) -> None:
    """Called on a background thread. Never raises."""
    try:
        claims = verify(token)
        if claims is None:
            return

        sub = claims["sub"]
        groups = claims.get(OIDC_GROUPS_CLAIM, []) or []
        if isinstance(groups, str):
            groups = [groups]

        matched_role: str | None = None
        for group in groups:
            if group in GROUP_TO_ROLE:
                matched_role = GROUP_TO_ROLE[group]
                break

        # Only add sub to the cache after a successful mapping: if the user
        # later joins a mapped group, a premature cache entry would block
        # them. This keeps the no-match path cheap to retry.
        if not matched_role:
            log.debug("sub=%s no mapped group in %s — skipped", sub, groups)
            return

        role_id = get_role_id(matched_role)
        if role_id is None:
            log.warning(
                "Role '%s' does not exist in Lakekeeper — sub %s skipped",
                matched_role, sub,
            )
            return

        lk_user_id = f"oidc~{sub}"
        if user_has_any_role(lk_user_id):
            log.debug("sub=%s already has a role — left untouched", sub)
            with _handled_lock:
                _handled.add(sub)
            return

        try:
            assign_role(role_id, lk_user_id)
            log.info("sub=%s -> role '%s' assigned", sub, matched_role)
            with _handled_lock:
                _handled.add(sub)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status == 409:
                # 409 Conflict = assignment tuple already exists in OpenFGA
                # (concurrent request, or user_has_any_role missed it due to
                # eventual consistency). Treat as success and cache to stop
                # the receiver from retrying on every subsequent request.
                log.debug("sub=%s already in role '%s' (409) — cached", sub, matched_role)
                with _handled_lock:
                    _handled.add(sub)
            else:
                # 404 = user not yet provisioned in Lakekeeper (happens when
                # the mirror catches a request before the very first user
                # registration). Do NOT cache — next request will succeed.
                log.warning("Assignment for sub=%s failed: %s", sub, exc)

    except Exception as exc:
        log.exception("process_user error: %s", exc)


# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_a, **_kw):
        pass  # Default log would emit a line per mirror hit — too noisy.

    def _ack(self, status: int = 204) -> None:
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _handle(self) -> None:
        # Health check for the receiver's own readiness probe.
        if self.path == "/healthz":
            self._ack(200)
            return

        auth = self.headers.get("Authorization", "")
        if not auth.lower().startswith("bearer "):
            self._ack()
            return
        token = auth[7:].strip()

        sub = _unverified_sub(token)
        if sub is None:
            self._ack()
            return

        with _handled_lock:
            if sub in _handled:
                self._ack()
                return

        # Ack with 204 immediately, processing is async — the mirror waits
        # for nothing.
        self._ack()
        _pool.submit(process_user, token)

    do_GET = _handle
    do_POST = _handle
    do_PUT = _handle
    do_PATCH = _handle
    do_DELETE = _handle
    do_HEAD = _handle


def main() -> None:
    log.info(
        "Interceptor starting on :%d, issuer=%s, audience=%s, groups-claim=%s, "
        "mapping=%s",
        LISTEN_PORT, ISSUER, KEYCLOAK_AUDIENCE, OIDC_GROUPS_CLAIM, GROUP_TO_ROLE,
    )
    if not GROUP_TO_ROLE:
        log.warning("No ROLE_MAPPING_* configured — interceptor is running but idle.")
    server = ThreadingHTTPServer(("0.0.0.0", LISTEN_PORT), Handler)
    server.serve_forever()


if __name__ == "__main__":
    main()
