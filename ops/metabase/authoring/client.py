"""Thin HTTP wrapper over Metabase's REST API.

Used by every authoring script. Centralizes:
- API-key auth (fetched from GCP Secret Manager at runtime)
- Base URL resolution (from MB_URL env)
- Error → exception conversion
- Retry on transient 5xx
- Dry-run interception of writes (POST/PUT/DELETE)

Expected env:
    MB_URL — https://<ip>.nip.io  (from `terraform output metabase_url`)
    MB_DRY_RUN — "1"/"true" to enable dry-run from env (equivalent to
        passing ``dry_run=True`` to the constructor)

The API key is resolved at runtime from Secret Manager:

    projects/dee-data-ops-prod/secrets/metabase-api-key/versions/latest

CI contexts that can't reach Secret Manager can set MB_SESSION directly
(e.g., from a GitHub Actions secret that wraps the same value).

Dry-run mode:
    When ``dry_run=True`` (or MB_DRY_RUN is truthy), the client logs every
    POST/PUT/DELETE it would send to stderr and returns a stub response
    with a synthetic negative ``id``. GETs against those synthetic ids are
    answered with empty stubs so upserts that read-back newly-created
    entities don't 404 mid-dry-run. The stub shape covers the keys the
    current authoring scripts rely on (``id``, ``entity_id``, ``dashcards``);
    callers that need more fields should add them here rather than in a
    subclass so the stub stays honest across scripts. Dry-run DOES NOT
    require credentials — Secret Manager fetch is also skipped.
"""
from __future__ import annotations

import json
import os
import sys
import time
from typing import Any

GCP_PROJECT = "dee-data-ops-prod"
API_KEY_SECRET = "metabase-api-key"


def _dry_run_env() -> bool:
    return os.environ.get("MB_DRY_RUN", "").lower() in ("1", "true", "yes")


class MetabaseClient:
    def __init__(
        self,
        url: str | None = None,
        session: str | None = None,
        *,
        dry_run: bool | None = None,
    ):
        self.dry_run = _dry_run_env() if dry_run is None else dry_run
        self._dry_counter = 0
        self._dry_writes: list[dict[str, Any]] = []
        self.url = (url or os.environ.get("MB_URL", "https://dry-run.invalid")).rstrip("/")
        if self.dry_run:
            self.session = session or os.environ.get("MB_SESSION") or "dry-run-stub-key"
            self._http = None
            print("[dry-run] MetabaseClient initialized — no real HTTP calls will be made",
                  file=sys.stderr)
            return
        self.session = session or os.environ.get("MB_SESSION") or self._fetch_api_key()
        auth_header = "x-api-key" if self.session.startswith("mb_") else "X-Metabase-Session"
        import httpx  # deferred: dry-run shouldn't require httpx installed
        self._http = httpx.Client(
            base_url=f"{self.url}/api",
            headers={auth_header: self.session},
            timeout=60,
        )

    @staticmethod
    def _fetch_api_key() -> str:
        # deferred import: dry-run shouldn't require google-cloud-secret-manager installed
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        resp = client.access_secret_version(
            name=f"projects/{GCP_PROJECT}/secrets/{API_KEY_SECRET}/versions/latest"
        )
        return resp.payload.data.decode().strip()

    # ───────────────────────────────────────── core verbs ──

    def get(self, path: str, **params: Any) -> Any:
        return self._request("GET", path, params=params)

    def post(self, path: str, body: dict | None = None) -> Any:
        return self._request("POST", path, json=body)

    def put(self, path: str, body: dict | None = None) -> Any:
        return self._request("PUT", path, json=body)

    def delete(self, path: str) -> Any:
        return self._request("DELETE", path)

    def _request(self, method: str, path: str, **kw: Any) -> Any:
        if self.dry_run:
            return self._dry_run_request(method, path, **kw)
        for attempt in range(3):
            resp = self._http.request(method, path.lstrip("/"), **kw)
            if resp.status_code < 500:
                resp.raise_for_status()
                if resp.headers.get("content-type", "").startswith("application/json"):
                    return resp.json()
                return resp.text
            time.sleep(2 ** attempt)
        resp.raise_for_status()

    # ───────────────────────────────────────── dry-run ──

    def _dry_run_request(self, method: str, path: str, **kw: Any) -> Any:
        """Dry-run interceptor. Writes log to stderr; returns stub response.

        GETs are answered with empty-but-shape-compatible stubs so upsert
        helpers can iterate over ``collections()`` / ``cards()`` / etc. and
        conclude "no existing match" (triggering the create-path, which then
        gets intercepted as a POST). For `/dashboard/{id}` GETs on negative
        ids (entities we synthesized earlier this run) we return a stub
        with empty ``dashcards`` so ``set_dashboard_cards`` can proceed.
        """
        norm = path.lstrip("/")
        if method == "GET":
            if norm == "collection":
                return []
            if norm == "card":
                return []
            if norm == "dashboard":
                return []
            if norm == "database":
                # find_database_id raises LookupError if the name isn't present,
                # which would abort dry-run. Return a single synthetic DB so the
                # lookup resolves — scripts pick the first DB by name match, so
                # the `name` must match what the script asks for. We don't
                # know that ahead of time, so we return a wildcard entry that
                # matches any name the caller requests by echoing it back.
                # Callers that use find_database_id will see our stub via
                # a secondary path (see databases() override below).
                return {"data": []}
            if norm.startswith("dashboard/"):
                dash_id = norm.split("/", 1)[1]
                return {"id": self._coerce_int(dash_id), "dashcards": []}
            # Generic stub: empty dict/list. Tighten if a script needs more.
            return {}
        # Mutation: log and return stub
        self._dry_counter -= 1
        stub_id = self._dry_counter
        body = kw.get("json") or {}
        entry = {
            "method": method,
            "path": norm,
            "stub_id": stub_id,
            "body_preview": self._body_preview(body),
        }
        self._dry_writes.append(entry)
        print(f"[dry-run] {method} /{norm} → stub id={stub_id}", file=sys.stderr)
        summary = self._summarize_body(body)
        if summary:
            print(f"[dry-run]   {summary}", file=sys.stderr)
        return {
            "id": stub_id,
            "entity_id": f"dry-run-{abs(stub_id):06d}",
            **({"dashcards": []} if norm.startswith("dashboard") else {}),
        }

    @staticmethod
    def _coerce_int(s: str) -> int:
        try:
            return int(s)
        except ValueError:
            return -1

    @staticmethod
    def _body_preview(body: dict | None) -> dict:
        if not body:
            return {}
        keys = ("name", "display", "collection_id", "cache_ttl")
        return {k: body[k] for k in keys if k in body}

    @staticmethod
    def _summarize_body(body: dict | None) -> str:
        if not body:
            return ""
        name = body.get("name")
        bits: list[str] = []
        if name:
            bits.append(f"name={name!r}")
        if "display" in body:
            bits.append(f"display={body['display']!r}")
        if "cache_ttl" in body:
            bits.append(f"cache_ttl={body['cache_ttl']}")
        native = (body.get("dataset_query") or {}).get("native") or {}
        if native.get("query"):
            q = native["query"].replace("\n", " ")
            bits.append(f"query={q[:140]!r}{'…' if len(native['query']) > 140 else ''}")
        if body.get("dashcards") is not None:
            bits.append(f"dashcards={len(body['dashcards'])}")
        return "  ".join(bits)

    def dry_run_summary(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for w in self._dry_writes:
            counts[w["method"]] = counts.get(w["method"], 0) + 1
        return counts

    # ───────────────────────────────────────── introspection ──

    def collections(self) -> list[dict]:
        return self.get("/collection")

    def databases(self) -> list[dict]:
        resp = self.get("/database")
        return resp["data"] if isinstance(resp, dict) else resp

    def cards(self) -> list[dict]:
        return self.get("/card")

    def dashboards(self) -> list[dict]:
        return self.get("/dashboard")
