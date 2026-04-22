"""Thin HTTP wrapper over Metabase's REST API.

Used by every authoring script. Centralizes:
- API-key auth (fetched from GCP Secret Manager at runtime)
- Base URL resolution (from MB_URL env)
- Error → exception conversion
- Retry on transient 5xx

Expected env:
    MB_URL — https://<ip>.nip.io  (from `terraform output metabase_url`)

The API key is resolved at runtime from Secret Manager:

    projects/dee-data-ops-prod/secrets/metabase-api-key/versions/latest

CI contexts that can't reach Secret Manager can set MB_SESSION directly
(e.g., from a GitHub Actions secret that wraps the same value).
"""
from __future__ import annotations

import os
import time
from typing import Any

import httpx
from google.cloud import secretmanager

GCP_PROJECT = "dee-data-ops-prod"
API_KEY_SECRET = "metabase-api-key"


class MetabaseClient:
    def __init__(self, url: str | None = None, session: str | None = None):
        self.url = (url or os.environ["MB_URL"]).rstrip("/")
        self.session = session or os.environ.get("MB_SESSION") or self._fetch_api_key()
        auth_header = "x-api-key" if self.session.startswith("mb_") else "X-Metabase-Session"
        self._http = httpx.Client(
            base_url=f"{self.url}/api",
            headers={auth_header: self.session},
            timeout=60,
        )

    @staticmethod
    def _fetch_api_key() -> str:
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
        for attempt in range(3):
            resp = self._http.request(method, path.lstrip("/"), **kw)
            if resp.status_code < 500:
                resp.raise_for_status()
                if resp.headers.get("content-type", "").startswith("application/json"):
                    return resp.json()
                return resp.text
            time.sleep(2 ** attempt)
        resp.raise_for_status()

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
