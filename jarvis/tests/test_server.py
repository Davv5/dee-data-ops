"""Server + action-bridge tests with a fake LLM (no Ollama, no Electron)."""

from __future__ import annotations

import json
import threading
from http.server import ThreadingHTTPServer

import httpx

from jarvis.config import load_config
from jarvis.brain.llm import OllamaError
from jarvis.server import _BrainState, _make_handler


class _FakeLLM:
    def __init__(self, messages):
        self._messages = list(messages)

    def chat(self, history, tools=None):
        return self._messages.pop(0)

    def list_models(self):
        raise OllamaError("no ollama in test")  # simulate Ollama down


def _state_with(messages):
    state = _BrainState(load_config())
    state.agent.llm = _FakeLLM(messages)
    return state


def test_add_task_emits_action():
    scripted = [
        {
            "role": "assistant",
            "content": "",
            "tool_calls": [
                {"function": {"name": "add_task", "arguments": {"title": "call mum", "due": "2026-06-29T17:00:00"}}}
            ],
        },
        {"role": "assistant", "content": "I'll remind you to call mum at 5."},
    ]
    result = _state_with(scripted).ask("remind me to call mum at 5pm", tasks=[])
    assert result["reply"].startswith("I'll remind you")
    assert result["actions"] == [
        {"type": "add_task", "task": {"title": "call mum", "due": "2026-06-29T17:00:00", "category": "standard"}}
    ]


def test_plain_answer_has_no_actions():
    result = _state_with([{"role": "assistant", "content": "It's sunny."}]).ask("how are you", tasks=[])
    assert result["reply"] == "It's sunny."
    assert result["actions"] == []


def test_http_health_and_chat_routing():
    state = _state_with([{"role": "assistant", "content": "hello"}])
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(state))
    port = httpd.server_address[1]
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        base = f"http://127.0.0.1:{port}"
        health = httpx.get(f"{base}/health", timeout=5).json()
        assert health["ok"] is True and health["ollama"] is False  # no real Ollama here

        chat = httpx.post(f"{base}/chat", json={"text": "hi", "tasks": []}, timeout=5).json()
        assert chat["reply"] == "hello"

        assert httpx.post(f"{base}/chat", json={"text": ""}, timeout=5).status_code == 400
        assert httpx.get(f"{base}/nope", timeout=5).status_code == 404
    finally:
        httpd.shutdown()
