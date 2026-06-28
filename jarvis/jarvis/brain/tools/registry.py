"""Tool registration, schema export, and guarded dispatch."""

from __future__ import annotations

import json
from typing import Any, Callable, Optional

# name -> {"fn", "destructive", "schema"}
_TOOLS: dict[str, dict[str, Any]] = {}

# Confirmation callback signature: (tool_name, args) -> bool (True = allowed).
ConfirmCallback = Callable[[str, dict[str, Any]], bool]


def tool(
    *,
    name: str,
    description: str,
    parameters: dict[str, Any],
    destructive: bool = False,
) -> Callable[[Callable], Callable]:
    """Register a function as an LLM-callable tool.

    `parameters` is a JSON Schema object describing the function arguments.
    `destructive=True` routes the call through the confirmation layer.
    """

    def decorator(fn: Callable) -> Callable:
        _TOOLS[name] = {
            "fn": fn,
            "destructive": destructive,
            "schema": {
                "type": "function",
                "function": {
                    "name": name,
                    "description": description,
                    "parameters": parameters,
                },
            },
        }
        return fn

    return decorator


def tool_schemas() -> list[dict[str, Any]]:
    return [t["schema"] for t in _TOOLS.values()]


def run_tool(
    name: str,
    args: dict[str, Any],
    *,
    confirm_destructive: bool = True,
    confirm_cb: Optional[ConfirmCallback] = None,
) -> str:
    """Execute a tool by name, applying the confirmation policy.

    Returns a string result (JSON-serialized on failure) suitable for feeding
    back to the model as a `tool` message.
    """
    entry = _TOOLS.get(name)
    if entry is None:
        return json.dumps({"error": f"unknown tool: {name}"})

    if entry["destructive"] and confirm_destructive:
        allowed = confirm_cb(name, args) if confirm_cb else False
        if not allowed:
            return json.dumps({"status": "cancelled", "reason": "user declined"})

    try:
        result = entry["fn"](**(args or {}))
    except TypeError as exc:
        return json.dumps({"error": f"bad arguments for {name}: {exc}"})
    except Exception as exc:  # tools must never crash the loop
        return json.dumps({"error": f"{type(exc).__name__}: {exc}"})

    return result if isinstance(result, str) else json.dumps(result)
