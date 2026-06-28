"""Thin Ollama client for the chat + tool-calling endpoint."""

from __future__ import annotations

from typing import Any

import httpx


class OllamaError(RuntimeError):
    pass


class OllamaClient:
    """Talks to a local Ollama server's /api/chat and /api/tags endpoints."""

    def __init__(
        self,
        host: str,
        model: str,
        *,
        keep_alive: str = "30m",
        temperature: float = 0.6,
        timeout: float = 180.0,
    ) -> None:
        self.host = host.rstrip("/")
        self.model = model
        self.keep_alive = keep_alive
        self.temperature = temperature
        self._client = httpx.Client(timeout=timeout)

    def chat(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """One non-streaming chat turn. Returns the assistant message dict.

        The returned message may contain `tool_calls`; the caller is responsible
        for executing them and continuing the loop.
        """
        payload: dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "stream": False,
            "keep_alive": self.keep_alive,
            "options": {"temperature": self.temperature},
        }
        if tools:
            payload["tools"] = tools

        try:
            resp = self._client.post(f"{self.host}/api/chat", json=payload)
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            raise OllamaError(f"Ollama request failed: {exc}") from exc

        data = resp.json()
        if "message" not in data:
            raise OllamaError(f"Unexpected Ollama response: {data}")
        return data["message"]

    def list_models(self) -> list[str]:
        """Names of locally installed models (for `jarvis doctor`)."""
        try:
            resp = self._client.get(f"{self.host}/api/tags")
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            raise OllamaError(f"Could not reach Ollama at {self.host}: {exc}") from exc
        return [m["name"] for m in resp.json().get("models", [])]
