"""Thin HTTP wrapper over Metabase's REST API.

Used by every authoring script. Centralizes:
- Session-token auth (from MB_SESSION env or interactive login)
- Base URL resolution (from MB_URL env)
- Error → exception conversion
- Retry on transient 5xx

Expected env:
    MB_URL       — https://<ip>.nip.io  (from `terraform output metabase_url`)
    MB_USER      — admin email
    MB_PASSWORD  — admin password OR an existing session id in MB_SESSION

One of (MB_PASSWORD, MB_SESSION) must be set. MB_SESSION takes precedence.
"""
from __future__ import annotations

import os
import time
from typing import Any

import httpx


class MetabaseClient:
    def __init__(self, url: str | None = None, session: str | None = None):
        self.url = (url or os.environ["MB_URL"]).rstrip("/")
        self.session = session or os.environ.get("MB_SESSION") or self._login()
        self._http = httpx.Client(
            base_url=f"{self.url}/api",
            headers={"X-Metabase-Session": self.session},
            timeout=60,
        )

    def _login(self) -> str:
        user = os.environ["MB_USER"]
        password = os.environ["MB_PASSWORD"]
        resp = httpx.post(
            f"{self.url}/api/session",
            json={"username": user, "password": password},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["id"]

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
        return self.get("/database")

    def cards(self) -> list[dict]:
        return self.get("/card")

    def dashboards(self) -> list[dict]:
        return self.get("/dashboard")
