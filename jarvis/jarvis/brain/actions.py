"""Per-request action bridge.

When the brain runs behind the HTTP server (the macOS HUD), some "tools" don't
execute anything themselves — they emit a structured *action* that the Electron
app applies to its own task store (the source of truth for tasks). This module
carries that per-request state via context variables so the task tools stay
plain functions and the server can collect what they emitted after the agent
finishes a turn.

In the plain `jarvis chat` CLI there is no app, so no context is set and the
task tools degrade gracefully (see tools/tasks.py).
"""

from __future__ import annotations

import contextvars
from typing import Any

# Actions emitted during the current request (applied by the Electron app).
_actions: contextvars.ContextVar[list[dict[str, Any]] | None] = contextvars.ContextVar(
    "jarvis_actions", default=None
)
# Snapshot of the app's current tasks, supplied by the request (read-only).
_tasks: contextvars.ContextVar[list[dict[str, Any]] | None] = contextvars.ContextVar(
    "jarvis_tasks", default=None
)


def begin_request(tasks: list[dict[str, Any]] | None) -> contextvars.Token:
    """Open an action-collecting scope for one request. Returns a reset token."""
    _tasks.set(tasks or [])
    return _actions.set([])


def end_request(token: contextvars.Token) -> list[dict[str, Any]]:
    """Close the scope and return everything that was emitted."""
    actions = _actions.get() or []
    _actions.reset(token)
    return list(actions)


def emit_action(action: dict[str, Any]) -> bool:
    """Record an action for the app to apply. True if a scope was active."""
    bucket = _actions.get()
    if bucket is None:
        return False
    bucket.append(action)
    return True


def current_tasks() -> list[dict[str, Any]]:
    return _tasks.get() or []


def app_connected() -> bool:
    """True when running inside a server request (i.e. the app is the caller)."""
    return _actions.get() is not None
