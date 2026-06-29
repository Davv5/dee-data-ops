"""Local HTTP brain server — the bridge the macOS HUD talks to.

`jarvis serve` starts this. The Electron app POSTs spoken/typed directives to
/chat and gets back a spoken reply plus any task actions to apply to its board.
Everything stays on localhost; nothing leaves the machine.

Endpoints:
  GET  /health  -> {ok, model, ollama: bool}
  POST /chat    -> body {text, tasks?} -> {reply, actions}  (503 if brain down)
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .config import Config, load_config
from .brain import actions
from .brain.agent import Agent
from .brain.llm import OllamaError

DEFAULT_PORT = 11500


class _BrainState:
    """One shared agent, guarded by a lock (single-user HUD)."""

    def __init__(self, config: Config) -> None:
        self.config = config
        # In app mode there's no interactive prompt: destructive tools are
        # allowed only when the user has turned confirmation off in config.
        self.agent = Agent(config, confirm_cb=lambda name, args: False)
        self.lock = threading.Lock()

    def ask(self, text: str, tasks: list[dict[str, Any]] | None) -> dict[str, Any]:
        with self.lock:
            token = actions.begin_request(tasks)
            try:
                reply = self.agent.say(text)
            finally:
                emitted = actions.end_request(token)
        return {"reply": reply, "actions": emitted}


def _make_handler(state: _BrainState):
    class Handler(BaseHTTPRequestHandler):
        # Quiet the default per-request stderr logging.
        def log_message(self, *_args):  # noqa: D401
            pass

        def _send(self, code: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):  # noqa: N802
            if self.path.rstrip("/") != "/health":
                return self._send(404, {"error": "not found"})
            ollama_up = True
            try:
                state.agent.llm.list_models()
            except OllamaError:
                ollama_up = False
            self._send(200, {"ok": True, "model": state.config.brain.model, "ollama": ollama_up})

        def do_POST(self):  # noqa: N802
            if self.path.rstrip("/") != "/chat":
                return self._send(404, {"error": "not found"})
            try:
                length = int(self.headers.get("Content-Length", 0))
                payload = json.loads(self.rfile.read(length) or b"{}")
            except (ValueError, json.JSONDecodeError):
                return self._send(400, {"error": "invalid JSON body"})

            text = (payload.get("text") or "").strip()
            if not text:
                return self._send(400, {"error": "empty text"})

            try:
                result = state.ask(text, payload.get("tasks"))
            except OllamaError as exc:
                return self._send(503, {"error": f"brain offline: {exc}"})
            except Exception as exc:  # never crash the server on one bad turn
                return self._send(500, {"error": f"{type(exc).__name__}: {exc}"})
            self._send(200, result)

    return Handler


def serve(config: Config | None = None, *, host: str = "127.0.0.1", port: int = DEFAULT_PORT) -> None:
    config = config or load_config()
    state = _BrainState(config)
    httpd = ThreadingHTTPServer((host, port), _make_handler(state))
    mode = "autonomous" if not config.safety.confirm_destructive else "confirm-guarded"
    print(f"JARVIS brain listening on http://{host}:{port}  (model={config.brain.model}, {mode})")
    print("POST /chat  ·  GET /health  ·  Ctrl-C to stop")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nbrain stopped.")
        httpd.shutdown()
